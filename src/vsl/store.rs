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
use super::VslIdent;

pub type Epoch = u32;

const MAX_SLOTS: u32 = 16_000;
const MAX_EPOCH_DIFF: Epoch = 1_000_000;

#[derive(Debug)]
pub struct VslStore<T> {
    map: LinkedHashMap<VslIdent, (Wrapping<Epoch>, T)>,
    slots_free: u32,
    epoch: Wrapping<Epoch>,
    max_epoch_diff: Wrapping<Epoch>,
}

impl<T> VslStore<T> {
    pub fn new() -> VslStore<T> {
        VslStore::with_max_slots_and_epoch_diff(MAX_SLOTS, MAX_EPOCH_DIFF)
    }

    pub fn with_max_slots_and_epoch_diff(max_slots: u32, max_epoch_diff: Epoch) -> VslStore<T> {
        VslStore {
            map: LinkedHashMap::new(),
            slots_free: max_slots,
            epoch: Wrapping(0),
            max_epoch_diff: Wrapping(max_epoch_diff),
        }
    }

    pub fn insert(&mut self, ident: VslIdent, value: T) {
        if self.slots_free < 1 {
            self.nuke();
        }
        assert!(self.slots_free >= 1);

        self.epoch = self.epoch + Wrapping(1);

        if self.map.insert(ident, (self.epoch, value)).is_none() {
            self.slots_free = self.slots_free - 1;
        }
    }

    pub fn remove(&mut self, ident: &VslIdent) -> Option<T> {
        let opt = self.map.remove(ident);
        if let Some((epoch, t)) = opt {
            self.slots_free = self.slots_free + 1;
            if self.epoch - epoch > self.max_epoch_diff {
                return None
            }
            return Some(t)
        }
        None
    }

    pub fn values(&self) -> Values<T> {
        Values(self.map.values())
    }

    fn nuke(&mut self) {
        if self.map.pop_front().is_some() {
            self.slots_free = self.slots_free + 1;
        }
    }

    #[cfg(test)]
    pub fn get(&self, ident: &VslIdent) -> Option<&T> {
        self.map.get(ident).map(t)
    }

    #[cfg(test)]
    pub fn oldest(&self) -> Option<(&VslIdent, &T)> {
        self.map.front().map(|(i, v)| (i, t(v)))
    }
}

fn t<T>(v: &(Wrapping<Epoch>, T)) -> &T {
    &v.1
}

pub struct Values<'a, T: 'a>(linked_hash_map::Values<'a, VslIdent, (Wrapping<Epoch>, T)>);

impl<'a, T> Iterator for Values<'a, T> {
    type Item = &'a T;
    fn next(&mut self) -> Option<Self::Item> {
        self.0.next().map(t)
    }
}

#[cfg(test)]
mod tests {
    pub use super::*;

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
}
