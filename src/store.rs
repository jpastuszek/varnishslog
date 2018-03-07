// VslIdent (vxid) is encoded as 30 bit long u where 31'st and 32'nd bits are used to tell
// client and backend requests apart; value of 0 is reserved for non-request related
// records.
// VslIdent is assigned in VXID_Get from per worker pool of 32768 IDs. When worker runs out
// additional set of IDs is allocated to it.
// So in practice we have 1Gi IDs available per client/backend request before they wrap
// around.
//
// We need to constrain memory usage + make sure that super old records are not keept
// forever if this constraing is not reached.
//
// To constrain memory we just set a limit of how many slots are available at any given
// time. To constraint time we can count VSL records in u64 or smaller wrapping integer
// so we can tell if two VslIdent's are from two different times.

use linked_hash_map::{self, LinkedHashMap};
use std::cmp::min;
use std::num::Wrapping;
use std::fmt::{self, Debug, Display};
use fnv::FnvHasher;
use std::hash::BuildHasherDefault;

use vsl::record::VslIdent;

// How many VslIdent recorts to keep in the store
const MAX_SLOTS: usize = 4000;
// How many inserts old a record can be before we drop it on remove
const MAX_EPOCH_DIFF: u64 = 14410;
// How many objects to nuke/expire when store is full or we are expiring as factor of MAX_SLOTS
const EVICT_FACTOR: f32 = 0.01;

#[derive(Debug)]
pub struct Config {
    max_slots: usize,
    max_epoch_diff: u64,
    evict_count: usize,
    stat_epoch_interval: Option<u64>,
    epoch_source: fn(Wrapping<u64>) -> Wrapping<u64>,
}

#[derive(Debug)]
struct Stats {
    inserted: Wrapping<u64>,
    removed: Wrapping<u64>,
    expired: Wrapping<u64>,
    nuked: Wrapping<u64>,
    slots_free: usize,
    max_slots: usize,
}

impl Stats {
    fn new(max_slots: usize) -> Stats {
        Stats {
            inserted: Wrapping(0),
            removed: Wrapping(0),
            expired: Wrapping(0),
            nuked: Wrapping(0),
            slots_free: 0,
            max_slots: max_slots,
        }
    }
}

impl Display for Stats {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "records inserted: {} removed: {} expired: {} nuked: {} slots used: {} free: {}",
               self.inserted, self.removed, self.expired, self.nuked, self.max_slots - self.slots_free, self.slots_free)
    }
}

impl Default for Config {
    fn default() -> Config {
        Config::new(MAX_SLOTS, MAX_EPOCH_DIFF, EVICT_FACTOR, None, None).unwrap()
    }
}

quick_error! {
    #[derive(Debug)]
    pub enum ConfigError {
        InvalidMaxSlots {
            description("Max slots must be greater than 0")
        }
        InvalidMaxEpochDiff {
            description("Max epoch diff must be greater then 0")
        }
        InvalidEvictFactor {
            description("Evict factor must yield evict count greater then 0")
        }
        InvalidStatEpochInterval {
            description("Stat eposh interval must be greater then 0")
        }
    }
}

fn sequential_epoch(epoch: Wrapping<u64>) -> Wrapping<u64> {
    epoch + Wrapping(1)
}

impl Config {
    pub fn new(max_slots: usize, max_epoch_diff: u64, evict_factor: f32, stat_epoch_interval: Option<u64>, epoch_source: Option<fn(Wrapping<u64>) -> Wrapping<u64>>) -> Result<Config, ConfigError> {
        let evict_count = (max_slots as f32 * evict_factor).ceil() as usize;

        if !(max_slots > 0) {
            return Err(ConfigError::InvalidMaxSlots)
        }
        if !(max_epoch_diff > 0) {
            return Err(ConfigError::InvalidMaxEpochDiff)
        }
        if !(evict_count > 0) {
            return Err(ConfigError::InvalidEvictFactor)
        }
        if let Some(false) = stat_epoch_interval.map(|s| s > 0) {
            return Err(ConfigError::InvalidStatEpochInterval)
        }

        Ok(Config {
            max_slots: max_slots,
            max_epoch_diff: max_epoch_diff,
            evict_count: evict_count,
            stat_epoch_interval: stat_epoch_interval,
            epoch_source: epoch_source.unwrap_or(sequential_epoch),
        })
    }
}

type Callback<T> = fn(&str, Wrapping<u64>, Wrapping<u64>, VslIdent, &T) -> (); 

// Wrapper that implements Debug
struct DebugCallback<T>(T);
impl<T> fmt::Debug for DebugCallback<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "<Callback>")
    }
}

#[derive(Debug)]
pub struct VslStore<T: Debug> {
    name: &'static str,
    // in order of insertion, oldest (lowest epoch) records are at the front
    store: LinkedHashMap<VslIdent, (Wrapping<u64>, T), BuildHasherDefault<FnvHasher>>,
    slots_free: usize,
    expire_count: usize,
    nuke_count: usize,
    epoch: Wrapping<u64>,
    epoch_source: fn(Wrapping<u64>) -> Wrapping<u64>,
    max_epoch_diff: u64,
    stat_epoch_interval: Option<u64>,
    stats: Stats,
    last_stats_epoch: Wrapping<u64>,
    on_expire: DebugCallback<Callback<T>>,
    on_nuke: DebugCallback<Callback<T>>,
}

impl<T: Debug> VslStore<T> {
    pub fn new(name: &'static str, on_expire: Option<Callback<T>>, on_nuke: Option<Callback<T>>) -> VslStore<T> {
        VslStore::with_config(name, on_expire, on_nuke, &Default::default())
    }

    pub fn with_config(name: &'static str, on_expire: Option<Callback<T>>, on_nuke: Option<Callback<T>>, config: &Config) -> VslStore<T> {
        VslStore {
            name: name,
            store: LinkedHashMap::default(),
            slots_free: config.max_slots,
            expire_count: config.evict_count,
            nuke_count: config.evict_count,
            epoch: Wrapping(0),
            epoch_source: config.epoch_source,
            max_epoch_diff: config.max_epoch_diff,
            stat_epoch_interval: config.stat_epoch_interval,
            stats: Stats::new(config.max_slots),
            last_stats_epoch: Wrapping(0),
            on_expire: DebugCallback(on_expire.unwrap_or(Self::log_expire)),
            on_nuke: DebugCallback(on_nuke.unwrap_or(Self::log_nuke)),
        }
    }

    pub fn log_expire(store_name: &str, current_epoch: Wrapping<u64>, record_epoch: Wrapping<u64>, record_ident: VslIdent, record: &T) -> () {
        warn!("VslStore[{}]: Removed expired record from store: current epoch {}, record epoch {}, ident: {}:\n{:#?}", store_name, current_epoch, record_epoch, record_ident, record);
    }
    pub fn log_nuke(store_name: &str, current_epoch: Wrapping<u64>, record_epoch: Wrapping<u64>, record_ident: VslIdent, record: &T) -> () {
        warn!("VslStore[{}]: Nuked record from store: current epoch {}, record epoch {}, ident: {}:\n{:#?}", store_name, current_epoch, record_epoch, record_ident, record);
    }

    pub fn insert(&mut self, ident: VslIdent, value: T) where T: Debug {
        // increase the epoch before we expire to make space for new insert
        self.epoch = (self.epoch_source)(self.epoch);

        self.expire();
        if self.slots_free < 1 {
            self.nuke();
        }

        assert!(self.slots_free >= 1);

        if self.store.insert(ident, (self.epoch, value)).is_none() {
            self.slots_free -= 1;
            self.stats.slots_free = self.slots_free;
            self.stats.inserted += Wrapping(1);
        }

        if let Some(true) = self.stat_epoch_interval.map(|i| self.epoch - self.last_stats_epoch >= Wrapping(i)) {
            info!("VslStore[{}] (epoch: {}): Statistics: {}", self.name, self.epoch, self.stats);
            self.last_stats_epoch = self.epoch;
        }
    }

    pub fn get_mut(&mut self, ident: &VslIdent) -> Option<&mut T> {
        self.store.get_mut(ident).map(|&mut (_epoch, ref mut t)| t)
    }

    pub fn get(&self, ident: &VslIdent) -> Option<&T> {
        self.store.get(ident).map(|&(_epoch, ref t)| t)
    }

    pub fn contains_key(&self, ident: &VslIdent) -> bool {
        self.store.contains_key(ident)
    }

    pub fn remove(&mut self, ident: &VslIdent) -> Option<T> {
        let val = self.store.remove(ident).map(|(_epoch, t)| t);
        if val.is_some() {
            self.slots_free += 1;
            self.stats.slots_free = self.slots_free;
            self.stats.removed += Wrapping(1);
        }
        val
    }

    pub fn values(&self) -> Values<T> {
        Values(self.store.values())
    }

    fn expire(&mut self) where T: Debug {
        let to_expire = self.store.values()
            .take(self.expire_count)
            .take_while(|&&(epoch, _)| self.epoch - epoch >= Wrapping(self.max_epoch_diff))
            .count();

        if to_expire == 0 {
            return
        }

        for _ in 0..to_expire {
            let (ident, (epoch, record)) = self.store.pop_front().unwrap();
            self.slots_free += 1;
            self.stats.slots_free = self.slots_free;
            self.stats.expired += Wrapping(1);
            self.on_expire.0(&self.name, self.epoch, epoch, ident, &record);
        }
    }

    fn nuke(&mut self) where T: Debug {
        let to_nuke: usize = min(self.nuke_count, self.store.len());

        for _ in 0..to_nuke {
            let (ident, (epoch, record)) = self.store.pop_front().unwrap();
            self.slots_free += 1;
            self.stats.slots_free = self.slots_free;
            self.stats.nuked += Wrapping(1);
            self.on_nuke.0(&self.name, self.epoch, epoch, ident, &record);
        }
    }
}

pub struct Values<'a, T: 'a>(linked_hash_map::Values<'a, VslIdent, (Wrapping<u64>, T)>);

impl<'a, T> Iterator for Values<'a, T> {
    type Item = &'a T;
    fn next(&mut self) -> Option<Self::Item> {
        self.0.next().map(|v| &v.1)
    }
}

#[cfg(test)]
mod tests {
    pub use super::*;

    use vsl::record::VslIdent;
    impl<T: Debug> VslStore<T> {
        pub fn oldest(&self) -> Option<(&VslIdent, &T)> {
            self.store.front().map(|(i, v)| (i, &v.1))
        }
    }

    #[test]
    fn nuking() {
        let mut s = VslStore::with_config("foo", None, None, &Config::new(10, 200, 0.1, None, None).unwrap());
        for i in 0..130 {
            s.insert(i, i);
        }

        assert_eq!(*s.oldest().unwrap().0, 130 - 10);
    }

    #[test]
    fn slot_count() {
        let mut s = VslStore::with_config("foo", None, None, &Config::new(10, 100, 0.1, None, None).unwrap());

        for i in 0..10 {
            s.insert(i, i);
        }

        for i in 0..2 {
            s.remove(&i);
        }

        s.remove(&100);

        for i in 10..13 {
            s.insert(i, i);
        }

        assert_eq!(*s.oldest().unwrap().0, 13 - 10);
    }

    #[test]
    fn expire() {
        let mut s = VslStore::with_config("foo", None, None, &Config::new(200, 10, 0.1, None, None).unwrap());

        for i in 0..140 {
            s.insert(i, i);
        }

        assert_eq!(*s.oldest().unwrap().0, 140 - 10);
    }
}
