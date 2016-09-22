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

use linked_hash_map::LinkedHashMap;
use linked_hash_map::Values;
use std::num::Wrapping;
use super::VslIdent;

const MAX_SLOTS: u32 = 1024 * 16;

#[derive(Debug)]
pub struct VslStore<T> {
    map: LinkedHashMap<VslIdent, T>,
    vsl_epoch: Wrapping<u8>, // use u32?
    slots_free: u32,
}

impl<T> VslStore<T> {
    pub fn new() -> VslStore<T> {
        VslStore::with_max_slots(MAX_SLOTS)
    }

    pub fn with_max_slots(max_slots: u32) -> VslStore<T> {
        VslStore {
            map: LinkedHashMap::new(),
            vsl_epoch: Wrapping(0),
            slots_free: max_slots,
        }
    }

    pub fn insert(&mut self, ident: VslIdent, value: T) {
        if self.slots_free < 1 {
            self.nuke();
        }
        assert!(self.slots_free >= 1);

        self.vsl_epoch = self.vsl_epoch + Wrapping(1);
        if self.map.insert(ident, value).is_none() {
            self.slots_free = self.slots_free - 1;
        }
    }

    pub fn remove(&mut self, ident: &VslIdent) -> Option<T> {
        let opt = self.map.remove(ident);
        if opt.is_some() {
            self.slots_free = self.slots_free + 1;
        }
        opt
    }

    pub fn get(&self, ident: &VslIdent) -> Option<&T> {
        self.map.get(ident)
    }

    pub fn values(&self) -> Values<VslIdent, T> {
        self.map.values()
    }

    fn nuke(&mut self) {
        if self.map.pop_front().is_some() {
            self.slots_free = self.slots_free + 1;
        }
    }

    pub fn oldest(&self) -> Option<(&VslIdent, &T)> {
        self.map.front()
    }
}

#[cfg(test)]
mod tests {
    pub use super::*;

    #[test]
    fn overflow_slots() {
        let mut s = VslStore::with_max_slots(10);

        for i in 0..1024 {
            s.insert(i, i);
        }

        assert_eq!(*s.oldest().unwrap().0, 1024 - 10);
    }

    #[test]
    fn overflow_slots_with_remove() {
        let mut s = VslStore::with_max_slots(10);

        for i in 0..1024 {
            s.insert(i, i);
            if i >= 4 && i % 4 == 0 {
                s.remove(&(i - 2));
            }
        }

        assert_eq!(*s.oldest().unwrap().0, 1024 - 10 - 2);
    }
}
