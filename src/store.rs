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
use std::fmt::Debug;
use fnv::FnvHasher;
use std::hash::BuildHasherDefault;

use vsl::record::VslIdent;

pub type Epoch = usize;

// How many VslIdent recorts to keep in the store
const MAX_SLOTS: usize = 4_000;
// How many inserts old a record can be before we drop it on remove
const MAX_EPOCH_DIFF: usize = 100_000;
// How many objects to nuke/expire when store is full or we are expiring as factor of MAX_SLOTS
const EVICT_FACTOR: f32 = 0.01;

pub struct Config {
    max_slots: usize,
    max_epoch_diff: usize,
    evict_count: usize,
}

impl Default for Config {
    fn default() -> Config {
        Config::new(MAX_SLOTS, MAX_EPOCH_DIFF, EVICT_FACTOR).unwrap()
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
    }
}

impl Config {
    pub fn new(max_slots: usize, max_epoch_diff: usize, evict_factor: f32) -> Result<Config, ConfigError> {
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

        Ok(Config {
            max_slots: max_slots,
            max_epoch_diff: max_epoch_diff,
            evict_count: evict_count,
        })
    }
}

#[derive(Debug)]
pub struct VslStore<T> {
    name: &'static str,
    // in order of insertion, oldest (lowest epoch) records are at the front
    store: LinkedHashMap<VslIdent, (Wrapping<Epoch>, T), BuildHasherDefault<FnvHasher>>,
    slots_free: usize,
    expire_count: usize,
    nuke_count: usize,
    epoch: Wrapping<Epoch>,
    max_epoch_diff: Wrapping<Epoch>,
}

impl<T> VslStore<T> {
    pub fn new(name: &'static str) -> VslStore<T> {
        VslStore::with_config(name, &Default::default())
    }

    pub fn with_config(name: &'static str, config: &Config) -> VslStore<T> {
        VslStore {
            name: name,
            store: LinkedHashMap::default(),
            slots_free: config.max_slots,
            expire_count: config.evict_count,
            nuke_count: config.evict_count,
            epoch: Wrapping(0),
            max_epoch_diff: Wrapping(config.max_epoch_diff),
        }
    }

    pub fn insert(&mut self, ident: VslIdent, value: T) where T: Debug {
        // increase the epoch before we expire to make space for new insert
        self.epoch = self.epoch + Wrapping(1);

        if self.epoch % Wrapping(self.expire_count) == Wrapping(0) || self.slots_free < 1 {
            // expire every expire_count insert to bulk it up
            // try to expire if storage is full before nuking
            self.expire();
        }

        if self.slots_free < 1 {
            self.nuke();
        }

        assert!(self.slots_free >= 1);

        if self.store.insert(ident, (self.epoch, value)).is_none() {
            self.slots_free -= 1;
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
        }
        val
    }

    pub fn values(&self) -> Values<T> {
        Values(self.store.values())
    }

    fn expire(&mut self) where T: Debug {
        let to_expire = self.store.values()
            .take(self.expire_count)
            .take_while(|&&(epoch, _)| self.epoch - epoch >= self.max_epoch_diff)
            .count();

        if to_expire == 0 {
            return
        }

        for _ in 0..to_expire {
            let (ident, (epoch, record)) = self.store.pop_front().unwrap();
            self.slots_free += 1;
            warn!("VslStore[{}]: Removed expired record from store: current epoch {}, record epoch {}, ident: {}:\n{:#?}", &self.name, self.epoch, epoch, ident, record);
        }
    }

    fn nuke(&mut self) where T: Debug {
        let to_nuke: usize = min(self.nuke_count, self.store.len());

        for _ in 0..to_nuke {
            let (ident, (epoch, record)) = self.store.pop_front().unwrap();
            self.slots_free += 1;
            warn!("VslStore[{}]: Nuked record from store: current epoch {}, record epoch {}, ident: {}:\n{:#?}", &self.name, self.epoch, epoch, ident, record);
        }
    }
}

pub struct Values<'a, T: 'a>(linked_hash_map::Values<'a, VslIdent, (Wrapping<Epoch>, T)>);

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
    impl<T> VslStore<T> {
        pub fn oldest(&self) -> Option<(&VslIdent, &T)> {
            self.store.front().map(|(i, v)| (i, &v.1))
        }
    }

    #[test]
    fn nuking() {
        let mut s = VslStore::with_config("foo", &Config::new(10, 200, 0.1).unwrap());
        for i in 0..130 {
            s.insert(i, i);
        }

        assert_eq!(*s.oldest().unwrap().0, 130 - 10);
    }

    #[test]
    fn slot_count() {
        let mut s = VslStore::with_config("foo", &Config::new(10, 100, 0.1).unwrap());

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
        let mut s = VslStore::with_config("foo", &Config::new(200, 10, 0.1).unwrap());

        // will expire every 20 records (200 * 0.1)
        for i in 0..140 {
            s.insert(i, i);
        }

        assert_eq!(*s.oldest().unwrap().0, 140 - 10);
    }
}
