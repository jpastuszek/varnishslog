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
const MAX_SLOTS: usize = 16_000;
// How many inserts old a record can be before we drop it on remove
const MAX_EPOCH_DIFF: Epoch = 1_000_000;
// Max number of objects to expire on insert
const EXPIRE_COUNT: usize = 10;
// How many objects to nuke when store is full as factor of MAX_SLOTS
const NUKE_FACTOR: f32 = 0.01;

#[derive(Debug)]
pub struct VslStore<T> {
    // in order of insertion, oldest (lowest epoch) records are at the front
    store: LinkedHashMap<VslIdent, (Wrapping<Epoch>, T), BuildHasherDefault<FnvHasher>>,
    expired: Vec<VslIdent>,
    slots_free: usize,
    expire_count: usize,
    nuke_count: usize,
    epoch: Wrapping<Epoch>,
    max_epoch_diff: Wrapping<Epoch>,
}

impl<T> Default for VslStore<T> {
    fn default() -> Self {
        VslStore::with_max_slots_and_epoch_diff(MAX_SLOTS, MAX_EPOCH_DIFF)
    }
}

impl<T> VslStore<T> {
    pub fn new() -> VslStore<T> {
        Default::default()
    }

    pub fn with_max_slots_and_epoch_diff(max_slots: usize, max_epoch_diff: Epoch) -> VslStore<T> {
        VslStore {
            store: LinkedHashMap::default(),
            expired: Vec::new(),
            slots_free: max_slots,
            expire_count: EXPIRE_COUNT,
            nuke_count: (max_slots as f32 * NUKE_FACTOR).ceil() as usize,
            epoch: Wrapping(0),
            max_epoch_diff: Wrapping(max_epoch_diff),
        }
    }

    pub fn insert(&mut self, ident: VslIdent, value: T) where T: Debug {
        // increase the epoch before we expire to make space for new insert
        self.epoch = self.epoch + Wrapping(1);
        self.expire();

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
        self.store.remove(ident).map(|(_epoch, t)| t)
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

        warn!("Expiring {} records", to_expire);
        for _ in 0..to_expire {
            let (ident, (epoch, record)) = self.store.pop_front().unwrap();
            info!("Removed expired record from store: current epoch {}, record epoch {}, ident: {}:\n{:#?}", self.epoch, epoch, ident, record);
        }
    }

    fn nuke(&mut self) where T: Debug {
        let to_nuke: usize = min(self.nuke_count, self.store.len());

        warn!("Nuking {} oldest records", to_nuke);
        for _ in 0..to_nuke {
            let (ident, (epoch, record)) = self.store.pop_front().unwrap();
            info!("Nuked record from store: current epoch {}, record epoch {}, ident: {}:\n{:#?}", self.epoch, epoch, ident, record);
            self.slots_free += 1;
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
        let mut s = VslStore::with_max_slots_and_epoch_diff(10, 100);
for i in 0..13 {
            s.insert(i, i);
        }

        assert_eq!(*s.oldest().unwrap().0, 13 - 10);
    }

    #[test]
    fn expire() {
        let mut s = VslStore::with_max_slots_and_epoch_diff(100, 10);

        for i in 0..13 {
            s.insert(i, i);
        }

        assert_eq!(*s.oldest().unwrap().0, 13 - 10);
    }
}
