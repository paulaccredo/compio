#[derive(Clone, Debug, Eq, PartialEq)]
struct LockRecord {
    offset: u64,
    length: u64,
    count: usize,
    exclusive: bool,
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
}
