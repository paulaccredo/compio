use std::collections::{BTreeMap, btree_map::Entry};

#[derive(Clone, Debug, Eq, PartialEq)]
struct LockRecord {
    offset: u64,
    length: u64,
    count: usize,
    exclusive: bool,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct LockMap {
    records: BTreeMap<u64, LockRecord>,
}

impl LockMap {
    fn try_acquire(&mut self, offset: u64, length: u64, exclusive: bool) -> bool {
        let Some(end) = range_end(offset, length) else {
            return false;
        };
        let start = offset;
        let candidate = self
            .records
            .range((std::ops::Bound::Excluded(start), std::ops::Bound::Unbounded))
            .next()
            .map(|(&candidate_end, record)| (candidate_end, record.offset, record.length));

        if let Some((candidate_end, candidate_offset, candidate_length)) = candidate {
            if candidate_offset == offset && candidate_end == end && candidate_length == length {
                return match self.records.entry(candidate_end) {
                    Entry::Occupied(mut entry) => entry.get_mut().try_acquire(exclusive),
                    Entry::Vacant(_) => false,
                };
            }
            if candidate_offset < end {
                return false;
            }
        }

        match self.records.entry(end) {
            Entry::Vacant(entry) => {
                entry.insert(LockRecord::new(offset, length, exclusive));
                true
            }
            Entry::Occupied(_) => false,
        }
    }

    #[cfg(test)]
    fn release(&mut self, offset: u64, length: u64) -> bool {
        let Some(end) = range_end(offset, length) else {
            return false;
        };
        match self.records.entry(end) {
            Entry::Occupied(mut entry)
                if entry.get().offset == offset && entry.get().length == length =>
            {
                let remove = entry.get_mut().release();
                if remove {
                    entry.remove();
                }
                true
            }
            _ => false,
        }
    }
}

#[derive(Clone, Debug, Default)]
pub(super) struct SharedLockMap {
    // The map includes held locks and in-flight reservations.
    state: std::sync::Arc<std::sync::Mutex<LockMap>>,
}

#[derive(Clone, Copy, Debug)]
struct ReservationInfo {
    end: u64,
    offset: u64,
    length: u64,
    exclusive: bool,
}

#[derive(Debug)]
pub(super) struct LockReservation {
    owner: SharedLockMap,
    // Dropping this token does not establish that the native operation failed.
    info: Option<ReservationInfo>,
}

impl SharedLockMap {
    pub(super) fn try_reserve(
        &self,
        offset: u64,
        length: u64,
        exclusive: bool,
    ) -> Option<LockReservation> {
        let mut state = self.state.lock().expect("lock poisoned");
        let end = range_end(offset, length)?;

        if !state.try_acquire(offset, length, exclusive) {
            return None;
        }

        Some(LockReservation {
            owner: self.clone(),
            info: Some(ReservationInfo {
                end,
                offset,
                length,
                exclusive,
            }),
        })
    }

    #[cfg(test)]
    fn snapshot(&self) -> LockMap {
        self.state.lock().expect("lock poisoned").clone()
    }

    #[cfg(test)]
    pub(super) fn count_at(&self, end: u64) -> Option<usize> {
        self.state
            .lock()
            .expect("lock poisoned")
            .records
            .get(&end)
            .map(|record| record.count)
    }
}

impl LockReservation {
    pub(super) fn complete(&mut self, succeeded: bool) -> bool {
        let Some(reservation) = self.info.take() else {
            return false;
        };

        let mut state = self.owner.state.lock().expect("lock poisoned");
        let Some(record) = state.records.get_mut(&reservation.end) else {
            return false;
        };
        if record.offset != reservation.offset || record.length != reservation.length {
            return false;
        }

        // Only the native operation's reported outcome may undo its
        // reservation.
        if !succeeded {
            record.count -= 1;
            if reservation.exclusive {
                record.exclusive = false;
            }
        }
        let remove = record.count == 0;
        if remove {
            state.records.remove(&reservation.end);
        }

        true
    }
}

fn range_end(offset: u64, length: u64) -> Option<u64> {
    if length == 0 {
        None
    } else {
        offset.checked_add(length)
    }
}

impl LockRecord {
    fn new(offset: u64, length: u64, exclusive: bool) -> Self {
        Self {
            offset,
            length,
            count: 1,
            exclusive,
        }
    }

    fn try_acquire(&mut self, exclusive: bool) -> bool {
        if self.count == 0 || exclusive {
            return false;
        }

        let Some(count) = self.count.checked_add(1) else {
            return false;
        };
        self.count = count;
        true
    }

    #[cfg(test)]
    fn release(&mut self) -> bool {
        if self.count == 0 {
            return false;
        }

        self.count -= 1;
        self.exclusive = false;
        self.count == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_record_tracks_one_shared_acquisition() {
        let record = LockRecord::new(12, 8, false);

        assert_eq!(record.offset, 12);
        assert_eq!(record.length, 8);
        assert_eq!(record.count, 1);
        assert!(!record.exclusive);
    }

    #[test]
    fn new_record_tracks_one_exclusive_acquisition() {
        let record = LockRecord::new(12, 8, true);

        assert_eq!(record.count, 1);
        assert!(record.exclusive);
    }

    #[test]
    fn repeated_shared_acquisitions_increment_the_count() {
        let mut record = LockRecord::new(12, 8, false);

        assert!(record.try_acquire(false));
        assert!(record.try_acquire(false));
        assert_eq!(record.count, 3);
        assert!(!record.exclusive);
    }

    #[test]
    fn shared_acquisition_preserves_exclusive_mode() {
        let mut record = LockRecord::new(12, 8, true);

        assert!(record.try_acquire(false));
        assert_eq!(record.count, 2);
        assert!(record.exclusive);
    }

    #[test]
    fn shared_record_rejects_exclusive_acquisition_unchanged() {
        let mut record = LockRecord::new(12, 8, false);
        let prior = record.clone();

        assert!(!record.try_acquire(true));
        assert_eq!(record, prior);
    }

    #[test]
    fn shared_acquisition_rejects_count_overflow_unchanged() {
        let mut record = LockRecord::new(12, 8, false);
        record.count = usize::MAX;
        let prior = record.clone();

        assert!(!record.try_acquire(false));
        assert_eq!(record, prior);
    }

    #[test]
    fn exclusive_record_rejects_duplicate_exclusive_acquisition_unchanged() {
        let mut record = LockRecord::new(12, 8, true);
        let prior = record.clone();

        assert!(!record.try_acquire(true));
        assert_eq!(record, prior);
    }

    #[test]
    fn release_drops_exclusive_before_shared_acquisitions_and_signals_removal() {
        let mut record = LockRecord::new(12, 8, true);
        assert!(record.try_acquire(false));

        assert!(!record.release());
        assert_eq!(record.count, 1);
        assert!(!record.exclusive);

        assert!(record.release());
        assert_eq!(record.count, 0);
    }

    #[test]
    fn releasing_an_empty_record_leaves_it_unchanged() {
        let mut record = LockRecord::new(12, 8, false);
        assert!(record.release());
        let prior = record.clone();

        assert!(!record.release());
        assert_eq!(record, prior);

        assert!(!record.try_acquire(false));
        assert_eq!(record, prior);
    }

    #[test]
    fn ordered_map_accepts_adjacent_and_disjoint_ranges() {
        let mut map = LockMap::default();

        assert!(map.try_acquire(4, 2, true));
        assert!(map.try_acquire(0, 4, true));
        assert!(map.try_acquire(20, 3, false));
        assert_eq!(map.records.len(), 3);
    }

    #[test]
    fn ordered_map_rejects_partial_overlap_and_containment_both_ways() {
        for (offset, length) in [(4, 4), (2, 2), (0, 4), (6, 4), (1, 8), (0, 12)] {
            let mut map = LockMap::default();
            assert!(map.try_acquire(2, 6, true));
            let prior = map.clone();

            assert!(!map.try_acquire(offset, length, true));
            assert_eq!(
                map,
                prior,
                "range {offset}..{:?}",
                range_end(offset, length)
            );
        }
    }

    #[test]
    fn ordered_map_allows_only_exact_shared_nesting() {
        let mut map = LockMap::default();

        assert!(map.try_acquire(10, 5, true));
        assert!(map.try_acquire(10, 5, false));
        assert_eq!(map.records[&15].count, 2);
        assert!(map.records[&15].exclusive);

        let prior = map.clone();
        assert!(!map.try_acquire(10, 5, true));
        assert_eq!(map, prior);
    }

    #[test]
    fn equal_end_does_not_make_different_ranges_exact_matches() {
        let mut map = LockMap::default();
        assert!(map.try_acquire(10, 10, true));
        let prior = map.clone();

        assert!(!map.try_acquire(15, 5, false));
        assert_eq!(map, prior);
        assert!(!map.release(15, 5));
        assert_eq!(map, prior);
    }

    #[test]
    fn failed_insert_and_release_leave_records_unchanged() {
        let mut map = LockMap::default();
        assert!(map.try_acquire(2, 6, false));
        let prior = map.clone();

        assert!(!map.try_acquire(4, 3, true));
        assert!(!map.release(2, 5));
        assert_eq!(map, prior);
    }

    #[test]
    fn release_updates_shared_counts_then_removes_the_final_record() {
        let mut map = LockMap::default();
        assert!(map.try_acquire(30, 4, false));
        assert!(map.try_acquire(30, 4, false));

        assert!(map.release(30, 4));
        assert_eq!(map.records[&34].count, 1);
        assert!(map.release(30, 4));
        assert!(map.records.is_empty());
        assert!(!map.release(30, 4));
    }

    #[test]
    fn final_release_preserves_a_distinct_held_range() {
        let mut map = LockMap::default();
        assert!(map.try_acquire(2, 4, false));
        assert!(map.try_acquire(20, 5, true));
        assert!(map.try_acquire(20, 5, false));

        assert!(map.release(2, 4));

        assert_eq!(map.records.len(), 1);
        let record = &map.records[&25];
        assert_eq!(record.offset, 20);
        assert_eq!(record.length, 5);
        assert_eq!(record.count, 2);
        assert!(record.exclusive);
    }

    #[test]
    fn map_release_drops_exclusive_before_shared_acquisitions() {
        let mut map = LockMap::default();
        assert!(map.try_acquire(6, 4, true));
        assert!(map.try_acquire(6, 4, false));

        assert!(map.release(6, 4));
        let record = &map.records[&10];
        assert_eq!(record.count, 1);
        assert!(!record.exclusive);
    }

    #[test]
    fn failed_map_updates_preserve_shared_mode_and_overflowed_count() {
        let mut map = LockMap::default();
        assert!(map.try_acquire(40, 8, false));
        let prior = map.clone();

        assert!(!map.try_acquire(40, 8, true));
        assert_eq!(map, prior);

        map.records.get_mut(&48).unwrap().count = usize::MAX;
        let prior = map.clone();
        assert!(!map.try_acquire(40, 8, false));
        assert_eq!(map, prior);
    }

    #[test]
    fn checked_end_arithmetic_preserves_u64_boundary_ranges() {
        let offset = u64::MAX - 1;
        let mut map = LockMap::default();

        assert_eq!(range_end(offset, 1), Some(u64::MAX));
        assert!(map.try_acquire(offset, 1, true));
        assert!(!map.try_acquire(u64::MAX, 1, true));
        assert_eq!(map.records.len(), 1);
    }

    #[test]
    fn maximum_u64_inputs_do_not_wrap_into_low_end_keys() {
        let mut map = LockMap::default();
        assert!(map.try_acquire(0, 2, true));
        assert!(map.try_acquire(u64::MAX - 1, 1, true));

        assert_eq!(map.records.len(), 2);
        assert!(map.records.contains_key(&2));
        assert!(map.records.contains_key(&u64::MAX));
    }

    #[test]
    fn overflowing_range_end_is_rejected_without_changing_map() {
        let mut map = LockMap::default();
        assert!(map.try_acquire(0, 2, true));
        let prior = map.clone();

        assert_eq!(range_end(u64::MAX, 1), None);
        assert_eq!(range_end(u64::MAX - 1, 2), None);
        assert!(!map.try_acquire(u64::MAX, 1, true));
        assert!(!map.try_acquire(u64::MAX - 1, 2, true));
        assert_eq!(map, prior);
    }

    #[test]
    fn ordered_lookup_skips_many_ranges_before_and_after_candidate() {
        let mut map = LockMap::default();
        for index in 0..64 {
            assert!(map.try_acquire(index * 4, 2, true));
        }

        let prior = map.clone();
        assert!(!map.try_acquire(129, 1, true));
        assert_eq!(map, prior);
        assert!(map.try_acquire(130, 2, true));
        assert!(map.try_acquire(258, 2, true));
        assert!(map.try_acquire(260, 2, true));
        assert_eq!(map.records.len(), 67);
    }

    #[test]
    fn zero_length_ranges_are_rejected_without_changing_map() {
        let mut empty = LockMap::default();
        assert!(!empty.try_acquire(12, 0, false));
        assert!(empty.records.is_empty());
        assert!(!empty.release(12, 0));

        let mut map = LockMap::default();
        assert!(map.try_acquire(8, 4, true));
        let prior = map.clone();
        assert!(!map.try_acquire(12, 0, false));
        assert_eq!(map, prior);

        let mut end_collision = LockMap::default();
        assert!(end_collision.try_acquire(8, 4, true));
        let prior = end_collision.clone();
        assert!(!end_collision.try_acquire(12, 0, true));
        assert_eq!(end_collision, prior);
    }

    #[test]
    fn synchronized_owner_and_state_are_send_and_sync() {
        fn assert_send_sync<T: Send + Sync>() {}

        assert_send_sync::<SharedLockMap>();
        assert_send_sync::<LockMap>();
    }

    #[test]
    fn owner_clones_share_state_and_keep_it_alive() {
        let owner = SharedLockMap::default();
        let clone = owner.clone();
        let mut reservation = owner.try_reserve(4, 8, true).unwrap();
        assert!(reservation.complete(true));
        drop(reservation);
        drop(owner);

        assert_eq!(clone.snapshot().records[&12].count, 1);
        assert!(clone.try_reserve(4, 8, true).is_none());
        let mut other = clone.try_reserve(20, 2, true).unwrap();
        assert!(other.complete(false));
    }

    #[test]
    fn failed_shared_reservation_preserves_held_exclusive_state() {
        let owner = SharedLockMap::default();
        let mut exclusive = owner.try_reserve(4, 8, true).unwrap();
        assert!(exclusive.complete(true));
        let before = owner.snapshot();

        let mut shared = owner.try_reserve(4, 8, false).unwrap();
        assert_eq!(owner.snapshot().records[&12].count, 2);
        assert!(shared.complete(false));
        assert_eq!(owner.snapshot(), before);
        assert!(owner.try_reserve(4, 8, true).is_none());
    }

    #[test]
    fn failed_new_reservation_removes_only_its_range_and_can_be_retried() {
        let owner = SharedLockMap::default();
        let mut held = owner.try_reserve(4, 8, true).unwrap();
        assert!(held.complete(true));
        let mut failed = owner.try_reserve(20, 4, true).unwrap();

        assert!(failed.complete(false));
        let map = owner.snapshot();
        assert_eq!(map.records.len(), 1);
        assert!(map.records.contains_key(&12));
        assert!(!map.records.contains_key(&24));

        let mut retry = owner.try_reserve(20, 4, true).unwrap();
        assert!(retry.complete(true));
        assert_eq!(owner.snapshot().records.len(), 2);
    }

    #[test]
    fn out_of_order_reservation_completions_preserve_successful_modes() {
        for exclusive_succeeded in [false, true] {
            for shared_succeeded in [false, true] {
                for shared_completes_first in [false, true] {
                    let owner = SharedLockMap::default();
                    let mut exclusive = owner.try_reserve(4, 8, true).unwrap();
                    let mut shared = owner.try_reserve(4, 8, false).unwrap();

                    if shared_completes_first {
                        assert!(shared.complete(shared_succeeded));
                        assert!(exclusive.complete(exclusive_succeeded));
                    } else {
                        assert!(exclusive.complete(exclusive_succeeded));
                        assert!(shared.complete(shared_succeeded));
                    }

                    let map = owner.snapshot();
                    match (exclusive_succeeded, shared_succeeded) {
                        (false, false) => assert!(map.records.is_empty()),
                        (true, false) => {
                            let record = &map.records[&12];
                            assert_eq!(record.count, 1);
                            assert!(record.exclusive);
                        }
                        (false, true) => {
                            let record = &map.records[&12];
                            assert_eq!(record.count, 1);
                            assert!(!record.exclusive);
                        }
                        (true, true) => {
                            let record = &map.records[&12];
                            assert_eq!(record.count, 2);
                            assert!(record.exclusive);
                        }
                    }

                    let completed = owner.snapshot();
                    assert!(!exclusive.complete(!exclusive_succeeded));
                    assert!(!shared.complete(!shared_succeeded));
                    assert_eq!(owner.snapshot(), completed);
                }
            }
        }
    }

    #[test]
    fn dropping_incomplete_reservation_keeps_range_conservatively_reserved() {
        let owner = SharedLockMap::default();
        let reservation = owner.try_reserve(4, 8, true).unwrap();
        let before = owner.snapshot();

        drop(reservation);

        assert_eq!(owner.snapshot(), before);
        assert!(owner.try_reserve(5, 4, false).is_none());
        assert!(owner.try_reserve(4, 8, true).is_none());
    }

    #[test]
    fn reservation_completion_is_applied_once() {
        let owner = SharedLockMap::default();
        let mut reservation = owner.try_reserve(4, 8, false).unwrap();

        assert!(reservation.complete(false));
        assert!(!reservation.complete(true));
        assert!(owner.snapshot().records.is_empty());

        let mut reservation = owner.try_reserve(20, 4, true).unwrap();
        assert!(reservation.complete(true));
        let before = owner.snapshot();
        assert!(!reservation.complete(false));
        assert_eq!(owner.snapshot(), before);
    }

    #[test]
    fn competing_exclusive_reservations_are_atomic_across_clones() {
        let owner = SharedLockMap::default();
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(3));
        let (sender, receiver) = std::sync::mpsc::channel();

        let mut threads = Vec::new();
        for clone in [owner.clone(), owner.clone()] {
            let barrier = barrier.clone();
            let sender = sender.clone();
            threads.push(std::thread::spawn(move || {
                barrier.wait();
                let reservation = clone.try_reserve(4, 8, true);
                sender.send(reservation).unwrap();
            }));
        }
        drop(sender);
        barrier.wait();

        let reservations = [
            receiver
                .recv_timeout(std::time::Duration::from_secs(5))
                .unwrap(),
            receiver
                .recv_timeout(std::time::Duration::from_secs(5))
                .unwrap(),
        ];
        for thread in threads {
            thread.join().unwrap();
        }
        assert_eq!(reservations.iter().filter(|item| item.is_some()).count(), 1);
        let mut reservation = reservations.into_iter().flatten().next().unwrap();
        assert!(reservation.complete(false));
        assert!(owner.snapshot().records.is_empty());
    }
}
