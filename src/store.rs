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
use std::num::Wrapping;
use std::fmt::Debug;
use fnv::FnvHasher;
use std::hash::BuildHasherDefault;

use vsl::record::VslIdent;

pub type Epoch = u32;

// How many VslIdent recorts to keep in the map
const MAX_SLOTS: u32 = 16_000;
// How many inserts old a record can be before we drop it on remove
const MAX_EPOCH_DIFF: Epoch = 1_000_000;
// How many objects to nuke when we need to nuke as factor of MAX_SLOTS
const NUKE_FACTOR: f32 = 0.01;

#[derive(Debug)]
pub struct VslStore<T> {
    map: LinkedHashMap<VslIdent, (Wrapping<Epoch>, T), BuildHasherDefault<FnvHasher>>,
    expired: Vec<VslIdent>,
    slots_free: u32,
    nuke_count: u32,
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

    pub fn with_max_slots_and_epoch_diff(max_slots: u32, max_epoch_diff: Epoch) -> VslStore<T> {
        VslStore {
            map: LinkedHashMap::default(),
            expired: Vec::new(),
            slots_free: max_slots,
            nuke_count: (max_slots as f32 * NUKE_FACTOR).ceil() as u32,
            epoch: Wrapping(0),
            max_epoch_diff: Wrapping(max_epoch_diff),
        }
    }

    pub fn insert(&mut self, ident: VslIdent, value: T) {
        self.expire();

        if self.slots_free < 1 {
            self.nuke();
        }
        assert!(self.slots_free >= 1);

        self.epoch = self.epoch + Wrapping(1);

        if self.map.insert(ident, (self.epoch, value)).is_none() {
            self.slots_free -= 1;
        }
    }

    pub fn get_mut(&mut self, ident: &VslIdent) -> Option<&mut T> where T: Debug {
        let opt = self.map.get_refresh(ident);
        if let Some(&mut (epoch, ref mut t)) = opt {
            if self.epoch - epoch > self.max_epoch_diff {
                warn!("Adding old record to expirity list; current epoch {}, record epoch {}, ident: {}: {:?}", self.epoch, epoch, ident, t);
                self.expired.push(*ident);
                return None
            }
            return Some(t)
        }
        None
    }

    pub fn remove(&mut self, ident: &VslIdent) -> Option<T> where T: Debug {
        let opt = self.map.remove(ident);
        if let Some((epoch, t)) = opt {
            self.slots_free += 1;
            if self.is_expired(epoch) {
                warn!("Dropping old record; current epoch {}, record epoch {}, ident: {}: {:?}", self.epoch, epoch, ident, t);
                return None
            }
            return Some(t)
        }
        None
    }

    pub fn values(&self) -> Values<T> {
        Values(self.map.values())
    }

    fn is_expired(&self, entry_epoch: Wrapping<Epoch>) -> bool {
        self.epoch - entry_epoch > self.max_epoch_diff
    }

    fn expire(&mut self) {
        for expired_ident in self.expired.drain(..) {
            self.map.remove(&expired_ident);
        }
    }

    fn nuke(&mut self) {
        warn!("Nuking up to {} oldest records", self.nuke_count);

        for _ in 0..self.nuke_count {
            if self.map.pop_front().is_none() {
                break;
            }
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
        pub fn get(&self, ident: &VslIdent) -> Option<&T> {
            self.map.get(ident).map(|v| &v.1)
        }

        pub fn oldest(&self) -> Option<(&VslIdent, &T)> {
            self.map.front().map(|(i, v)| (i, &v.1))
        }
    }

    #[test]
    fn old_elements_should_be_removed_on_overflow() {
        let mut s = VslStore::with_max_slots_and_epoch_diff(10, 100);

        for i in 0..1024 {
            s.insert(i, i);
        }

        assert_eq!(*s.oldest().unwrap().0, 1024 - 10);
    }

    #[test]
    fn old_elements_should_be_removed_on_overflow_with_remove() {
        let mut s = VslStore::with_max_slots_and_epoch_diff(10, 100);

        for i in 0..1024 {
            s.insert(i, i);
            if i >= 4 && i % 4 == 0 {
                s.remove(&(i - 2));
            }
        }

        assert_eq!(*s.oldest().unwrap().0, 1024 - 10 - 2);
    }

    #[test]
    fn old_elements_should_not_be_retruned_by_remove() {
        let mut s = VslStore::with_max_slots_and_epoch_diff(1024, 100);

        for i in 0..1024 {
            s.insert(i, i);
        }

        assert!(s.remove(&0).is_none());
        assert!(s.remove(&100).is_none());
        assert!(s.remove(&(1023 - 101)).is_none());
        assert!(s.remove(&(1023 - 100)).is_some());
    }

    #[test]
    fn old_elements_should_not_be_retruned_by_get_mut() {
        let mut s = VslStore::with_max_slots_and_epoch_diff(1024, 100);

        for i in 0..1024 {
            s.insert(i, i);
        }

        assert!(s.get_mut(&0).is_none());
        assert!(s.get_mut(&100).is_none());
        assert!(s.get_mut(&(1023 - 101)).is_none());
        assert!(s.get_mut(&(1023 - 100)).is_some());
    }
}
