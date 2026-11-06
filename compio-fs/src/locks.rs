use std::{
    collections::{BTreeMap, btree_map::Entry},
    io,
};

use windows_sys::Win32::{
    Foundation::{ERROR_IO_PENDING, GetLastError},
    Storage::FileSystem::UnlockFileEx,
    System::{
        IO::OVERLAPPED,
        Threading::{
            CloseThreadpoolWait, CreateEventW, CreateThreadpoolWait, PTP_CALLBACK_INSTANCE,
            PTP_WAIT, SetThreadpoolWait,
        },
    },
};

#[derive(Clone, Debug, Eq, PartialEq)]
struct LockRecord {
    offset: u64,
    length: u64,
    count: usize,
    exclusive: bool,
    pending_acquisitions: usize,
    release_pending: bool,
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
    state: std::sync::Arc<std::sync::Mutex<LockState>>,
}

#[derive(Debug, Default)]
struct LockState {
    map: LockMap,
    // WHY: Drop needs an owned handle independent of SharedFd and a runtime.
    cleanup_file: Option<std::fs::File>,
    #[cfg(test)]
    _drop_signal: Option<DropSignal>,
}

impl Drop for LockState {
    fn drop(&mut self) {
        let Some(file) = self.cleanup_file.take() else {
            return;
        };

        clear_locks(&self.map, |offset, length| {
            let cleanup_file = file.try_clone()?;
            unlock_best_effort(cleanup_file, offset, length)
        });
    }
}

fn clear_locks(map: &LockMap, mut unlock: impl FnMut(u64, u64) -> io::Result<()>) {
    for record in map.records.values() {
        for _ in 0..record.count {
            // WHY: Drop cannot return native errors; handle closure releases
            // any range that best-effort unlock could not release.
            let _ = unlock(record.offset, record.length);
        }
    }
}

struct PendingUnlock {
    file: std::fs::File,
    _event: std::os::windows::io::OwnedHandle,
    overlapped: OVERLAPPED,
    #[cfg(test)]
    _drop_signal: Option<DropSignal>,
}

#[cfg(test)]
#[derive(Debug)]
struct DropSignal(std::sync::mpsc::Sender<()>);

#[cfg(test)]
impl Drop for DropSignal {
    fn drop(&mut self) {
        let _ = self.0.send(());
    }
}

fn unlock_best_effort(file: std::fs::File, offset: u64, length: u64) -> io::Result<()> {
    let operation = prepare_unlock(file, offset)?;
    submit_unlock(operation, |handle, overlapped| {
        // SAFETY: the request owns the duplicated handle and heap OVERLAPPED
        // through immediate completion or the registered event callback.
        let result =
            unsafe { UnlockFileEx(handle, 0, length as u32, (length >> 32) as u32, overlapped) };
        if result != 0 {
            Ok(())
        } else {
            // SAFETY: GetLastError has no pointer or handle preconditions.
            let error = unsafe { GetLastError() };
            Err(io::Error::from_raw_os_error(error as i32))
        }
    })
}

fn prepare_unlock(file: std::fs::File, offset: u64) -> io::Result<Box<PendingUnlock>> {
    use std::os::windows::io::{AsRawHandle, FromRawHandle};

    // SAFETY: null attributes/name request a new unnamed manual-reset event.
    let event = unsafe { CreateEventW(std::ptr::null(), 1, 0, std::ptr::null()) };
    if event.is_null() {
        return Err(io::Error::last_os_error());
    }

    // SAFETY: CreateEventW returned a new owned event handle.
    let event = unsafe { std::os::windows::io::OwnedHandle::from_raw_handle(event) };
    let event_handle = event.as_raw_handle();
    let mut operation = Box::new(PendingUnlock {
        file,
        _event: event,
        overlapped: OVERLAPPED::default(),
        #[cfg(test)]
        _drop_signal: None,
    });
    operation.overlapped.Anonymous.Anonymous.Offset = offset as u32;
    operation.overlapped.Anonymous.Anonymous.OffsetHigh = (offset >> 32) as u32;
    // WHY: the low bit suppresses a completion packet for this non-driver
    // OVERLAPPED while the event still reports completion to the thread pool.
    operation.overlapped.hEvent = (event_handle as usize | 1) as _;

    Ok(operation)
}

fn submit_unlock(
    operation: Box<PendingUnlock>,
    issue: impl FnOnce(std::os::windows::io::RawHandle, *mut OVERLAPPED) -> io::Result<()>,
) -> io::Result<()> {
    use std::os::windows::io::AsRawHandle;

    let context = Box::into_raw(operation);
    // SAFETY: the raw pointer owns the request until it is reconstructed
    // below or by the callback. The wait is unarmed until native Pending.
    let wait =
        unsafe { CreateThreadpoolWait(Some(unlock_complete), context.cast(), std::ptr::null()) };
    if wait == 0 {
        let error = io::Error::last_os_error();
        // SAFETY: wait allocation failed, so no callback can use the context.
        drop(unsafe { Box::from_raw(context) });
        return Err(error);
    }

    // SAFETY: the wait is unarmed, and this function still owns the request.
    let event_handle = unsafe { (*context)._event.as_raw_handle() };
    // SAFETY: the wait is unarmed, and the request remains alive here.
    let handle = unsafe { (*context).file.as_raw_handle() };
    // SAFETY: the wait is unarmed, and the request remains alive here.
    let overlapped = unsafe { std::ptr::addr_of_mut!((*context).overlapped) };
    let result = issue(handle, overlapped);
    match result {
        Ok(()) => {
            // SAFETY: this wait was never armed, so no callback can run.
            unsafe { CloseThreadpoolWait(wait) };
            // SAFETY: the wait is closed without arming, so no callback can
            // use the request context.
            drop(unsafe { Box::from_raw(context) });
            Ok(())
        }
        Err(error) if error.raw_os_error() == Some(ERROR_IO_PENDING as i32) => {
            // SAFETY: the callback now owns context; the manual-reset event
            // retains a signal that arrives before the wait is armed.
            unsafe { SetThreadpoolWait(wait, event_handle, std::ptr::null()) };
            Ok(())
        }
        Err(error) => {
            // SAFETY: this wait was never armed, so no callback can run.
            unsafe { CloseThreadpoolWait(wait) };
            // SAFETY: the wait is closed without arming, so no callback can
            // use the request context.
            drop(unsafe { Box::from_raw(context) });
            Err(error)
        }
    }
}

unsafe extern "system" fn unlock_complete(
    _instance: PTP_CALLBACK_INSTANCE,
    context: *mut std::ffi::c_void,
    wait: PTP_WAIT,
    _wait_result: u32,
) {
    // SAFETY: the callback runs once after the event is signaled; closing the
    // wait is asynchronous if this callback is still active.
    unsafe { CloseThreadpoolWait(wait) };
    // SAFETY: the pending branch transferred this Box to the callback before
    // arming the one-shot wait, and no other path reclaims it.
    let operation = unsafe { Box::from_raw(context.cast::<PendingUnlock>()) };
    drop(operation);
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

        if state.map.records.get(&end).is_some_and(|record| {
            record.offset == offset && record.length == length && record.release_pending
        }) {
            return None;
        }

        if !state.map.try_acquire(offset, length, exclusive) {
            return None;
        }
        state
            .map
            .records
            .get_mut(&end)
            .expect("successful acquisition reservation has a record")
            .pending_acquisitions += 1;

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

    pub(super) fn ensure_cleanup_file(
        &self,
        make_file: impl FnOnce() -> io::Result<std::fs::File>,
    ) -> io::Result<()> {
        let mut state = self.state.lock().expect("lock poisoned");
        if state.cleanup_file.is_none() {
            state.cleanup_file = Some(make_file()?);
        }
        Ok(())
    }

    pub(super) fn try_reserve_release(
        &self,
        offset: u64,
        length: u64,
    ) -> Option<LockReleaseReservation> {
        let end = range_end(offset, length)?;
        let mut state = self.state.lock().expect("lock poisoned");
        let record = state.map.records.get_mut(&end)?;
        if record.offset != offset
            || record.length != length
            || record.pending_acquisitions != 0
            || !record.try_reserve_release()
        {
            return None;
        }

        // Keep the held count unchanged until the driver reports unlock
        // success.
        Some(LockReleaseReservation {
            owner: self.clone(),
            info: Some(ReservationInfo {
                end,
                offset,
                length,
                exclusive: false,
            }),
        })
    }

    #[cfg(test)]
    fn snapshot(&self) -> LockMap {
        self.state.lock().expect("lock poisoned").map.clone()
    }

    #[cfg(test)]
    pub(super) fn count_at(&self, end: u64) -> Option<usize> {
        self.state
            .lock()
            .expect("lock poisoned")
            .map
            .records
            .get(&end)
            .map(|record| record.count)
    }

    #[cfg(test)]
    pub(super) fn test_state_at(&self, end: u64) -> Option<(usize, bool, usize, bool)> {
        self.state
            .lock()
            .expect("lock poisoned")
            .map
            .records
            .get(&end)
            .map(|record| {
                (
                    record.count,
                    record.exclusive,
                    record.pending_acquisitions,
                    record.release_pending,
                )
            })
    }
}

impl LockReservation {
    pub(super) fn complete(&mut self, succeeded: bool) -> bool {
        let Some(reservation) = self.info.take() else {
            return false;
        };

        let mut state = self.owner.state.lock().expect("lock poisoned");
        let Some(record) = state.map.records.get_mut(&reservation.end) else {
            return false;
        };
        if record.offset != reservation.offset || record.length != reservation.length {
            return false;
        }

        record.pending_acquisitions -= 1;
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
            state.map.records.remove(&reservation.end);
        }

        true
    }
}

#[derive(Debug)]
pub(super) struct LockReleaseReservation {
    owner: SharedLockMap,
    info: Option<ReservationInfo>,
}

impl LockReleaseReservation {
    pub(super) fn complete(&mut self, succeeded: bool) -> bool {
        let Some(reservation) = self.info.take() else {
            return false;
        };

        let mut state = self.owner.state.lock().expect("lock poisoned");
        let Some(record) = state.map.records.get_mut(&reservation.end) else {
            return false;
        };
        if record.offset != reservation.offset || record.length != reservation.length {
            return false;
        }

        record.finish_release(succeeded);
        if record.count == 0 {
            state.map.records.remove(&reservation.end);
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
            pending_acquisitions: 0,
            release_pending: false,
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

    fn try_reserve_release(&mut self) -> bool {
        if self.release_pending {
            return false;
        }
        self.release_pending = true;
        true
    }

    fn finish_release(&mut self, succeeded: bool) {
        self.release_pending = false;
        if succeeded {
            self.count -= 1;
            self.exclusive = false;
        }
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
    fn cleanup_continues_after_an_unlock_error_for_every_held_count() {
        let mut map = LockMap::default();
        assert!(map.try_acquire(5, 4, true));
        assert!(map.try_acquire(5, 4, false));
        assert!(map.try_acquire(20, 2, false));

        let mut attempts = Vec::new();
        clear_locks(&map, |offset, length| {
            attempts.push((offset, length));
            if attempts.len() == 1 {
                Err(io::Error::other("controlled cleanup failure"))
            } else {
                Ok(())
            }
        });

        assert_eq!(attempts, [(5, 4), (5, 4), (20, 2)]);
    }

    #[test]
    fn reservation_keeps_cleanup_owner_alive_until_completion() {
        use std::sync::mpsc;

        let tempfile = tempfile::NamedTempFile::new().unwrap();
        let owner = SharedLockMap::default();
        owner
            .ensure_cleanup_file(|| std::fs::File::open(tempfile.path()))
            .unwrap();
        let (sender, receiver) = mpsc::channel();
        owner.state.lock().unwrap()._drop_signal = Some(DropSignal(sender));
        let mut reservation = owner.try_reserve(16, 4, true).unwrap();

        drop(owner);
        assert_eq!(receiver.try_recv(), Err(mpsc::TryRecvError::Empty));
        assert!(reservation.complete(true));
        drop(reservation);
        receiver
            .recv_timeout(std::time::Duration::from_secs(5))
            .expect("last reservation drop should release the shared cleanup owner");
    }

    #[cfg(windows)]
    #[test]
    fn controlled_pending_unlock_retains_storage_until_event_completion() {
        use std::{sync::mpsc, time::Duration};

        use windows_sys::Win32::{
            Foundation::{GetHandleInformation, HANDLE},
            System::Threading::SetEvent,
        };

        let tempfile = tempfile::NamedTempFile::new().unwrap();
        let file = std::fs::File::open(tempfile.path()).unwrap();
        let mut operation = prepare_unlock(file, 0).unwrap();
        let (sender, receiver) = mpsc::channel();
        operation._drop_signal = Some(DropSignal(sender));

        let mut event = None;
        let mut file_handle = None;
        submit_unlock(operation, |handle, overlapped| {
            file_handle = Some(handle);
            // SAFETY: the test control reads the request-owned OVERLAPPED
            // before reporting synthetic Pending; it does not retain it.
            let event_handle: HANDLE = unsafe { (*overlapped).hEvent };
            assert_eq!(event_handle as usize & 1, 1);
            event = Some((event_handle as usize & !1) as HANDLE);
            Err(io::Error::from_raw_os_error(ERROR_IO_PENDING as i32))
        })
        .unwrap();

        let event = event.expect("test control captured the completion event");
        let file_handle = file_handle.expect("test control captured the file handle");
        let mut flags = 0;
        // SAFETY: the callback has not run, so the request still owns the
        // event.
        assert_ne!(unsafe { GetHandleInformation(event, &mut flags) }, 0);
        assert_eq!(receiver.try_recv(), Err(mpsc::TryRecvError::Empty));
        // SAFETY: the queued request owns this duplicated file handle until
        // the test signals its event and observes the callback's drop signal.
        assert_ne!(unsafe { GetHandleInformation(file_handle, &mut flags) }, 0);
        // SAFETY: the pending request retains this live manual-reset event.
        assert_ne!(unsafe { SetEvent(event) }, 0);
        receiver
            .recv_timeout(Duration::from_secs(5))
            .expect("completion callback should release pending request storage");
    }

    #[cfg(windows)]
    #[test]
    fn controlled_pending_unlock_observes_event_signaled_before_wait_arm() {
        use std::{sync::mpsc, time::Duration};

        use windows_sys::Win32::{
            Foundation::{GetHandleInformation, HANDLE},
            System::Threading::SetEvent,
        };

        let tempfile = tempfile::NamedTempFile::new().unwrap();
        let file = std::fs::File::open(tempfile.path()).unwrap();
        let mut operation = prepare_unlock(file, 0).unwrap();
        let (sender, receiver) = mpsc::channel();
        operation._drop_signal = Some(DropSignal(sender));

        submit_unlock(operation, |_handle, overlapped| {
            // SAFETY: the test signals the request-owned event before it
            // reports synthetic Pending; wait registration happens later.
            let raw_event: HANDLE = unsafe { (*overlapped).hEvent };
            assert_eq!(raw_event as usize & 1, 1);
            let event = (raw_event as usize & !1) as HANDLE;
            let mut flags = 0;
            // SAFETY: the pending request owns this event.
            assert_ne!(unsafe { GetHandleInformation(event, &mut flags) }, 0);
            // SAFETY: the event is owned by the pending request.
            assert_ne!(unsafe { SetEvent(event) }, 0);
            Err(io::Error::from_raw_os_error(ERROR_IO_PENDING as i32))
        })
        .unwrap();

        receiver
            .recv_timeout(Duration::from_secs(5))
            .expect("the wait should observe an event signaled before it was armed");
    }

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
    fn release_reservation_does_not_mark_a_held_range_released_until_success() {
        let owner = SharedLockMap::default();
        let mut acquire = owner.try_reserve(4, 8, true).unwrap();
        assert!(acquire.complete(true));
        let before = owner.snapshot();
        let mut release = owner.try_reserve_release(4, 8).unwrap();

        let reserved = owner.snapshot();
        let held_before = &before.records[&12];
        let held_during = &reserved.records[&12];
        assert_eq!(held_during.count, held_before.count);
        assert_eq!(held_during.exclusive, held_before.exclusive);
        assert_eq!(
            held_during.pending_acquisitions,
            held_before.pending_acquisitions
        );
        assert!(!held_before.release_pending);
        assert!(held_during.release_pending);
        assert!(owner.try_reserve(4, 8, false).is_none());
        assert!(owner.try_reserve(5, 2, true).is_none());
        assert!(release.complete(false));
        assert_eq!(owner.snapshot(), before);
        assert!(!release.complete(true));
    }

    #[test]
    fn release_reservation_rejects_non_exact_and_in_flight_acquisitions() {
        let owner = SharedLockMap::default();
        let mut acquire = owner.try_reserve(4, 8, true).unwrap();

        assert!(owner.try_reserve_release(5, 7).is_none());
        assert!(owner.try_reserve_release(4, 8).is_none());
        assert!(acquire.complete(true));
        assert!(owner.try_reserve_release(4, 8).is_some());
    }

    #[test]
    fn completed_release_applies_once_and_preserves_exclusive_first_order() {
        let owner = SharedLockMap::default();
        let mut exclusive = owner.try_reserve(4, 8, true).unwrap();
        assert!(exclusive.complete(true));
        let mut shared = owner.try_reserve(4, 8, false).unwrap();
        assert!(shared.complete(true));

        let mut first = owner.try_reserve_release(4, 8).unwrap();
        assert!(first.complete(true));
        assert!(!first.complete(true));
        let record = &owner.snapshot().records[&12];
        assert_eq!(record.count, 1);
        assert!(!record.exclusive);

        let mut second = owner.try_reserve_release(4, 8).unwrap();
        assert!(second.complete(true));
        assert!(owner.snapshot().records.is_empty());
    }

    #[test]
    fn only_one_release_may_be_in_flight_for_a_range() {
        let owner = SharedLockMap::default();
        let mut first = owner.try_reserve(4, 8, false).unwrap();
        assert!(first.complete(true));
        let mut second = owner.try_reserve(4, 8, false).unwrap();
        assert!(second.complete(true));

        let mut release_one = owner.try_reserve_release(4, 8).unwrap();
        assert!(owner.try_reserve_release(4, 8).is_none());
        assert!(release_one.complete(false));
        assert_eq!(owner.snapshot().records[&12].count, 2);

        let mut retry = owner.try_reserve_release(4, 8).unwrap();
        assert!(retry.complete(true));
        assert_eq!(owner.snapshot().records[&12].count, 1);
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
