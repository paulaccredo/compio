use std::{
    io::{self, Write as _},
    net::{TcpListener, TcpStream},
    num::NonZeroU16,
    time::Duration,
};

use compio_buf::{BufResult, SetLenExt};
#[cfg(unix)]
use compio_driver::op::{AcceptMulti, Mode, OFlags, Pipe, ReadMulti, Write};
#[cfg(windows)]
use compio_driver::op::{TryLockFile, UnlockFile};
use compio_driver::{
    AsRawFd, Extra, OpCode, OwnedFd, Proactor, PushEntry, ResultTakeBuffer, SharedFd, TakeBuffer,
    op::{Asyncify, CloseFile, CloseSocket, ReadAt, ReadManagedAt, RecvMulti},
};
use rustix::net::RecvFlags;

#[cfg(unix)]
#[test]
fn truncate_file_poll() {
    use compio_driver::{ToSharedFd, op::TruncateFile};

    let mut driver = Proactor::builder().build().unwrap();

    let fd = std::fs::File::create_new("temp.txt").unwrap();
    let file = SharedFd::new(fd);
    driver.attach(file.as_raw_fd()).unwrap();

    let size = 5;
    let op = TruncateFile::new(file.to_shared_fd(), size);
    let _ = push_and_wait(&mut driver, op);

    let meta = file.metadata().unwrap();
    std::fs::remove_file("temp.txt").unwrap();
    assert_eq!(5, meta.len());
}

#[cfg(windows)]
fn open_file(driver: &mut Proactor) -> OwnedFd {
    use std::os::windows::{
        fs::OpenOptionsExt,
        io::{FromRawHandle, IntoRawHandle, OwnedHandle},
    };

    use windows_sys::Win32::Storage::FileSystem::FILE_FLAG_OVERLAPPED;

    let op = Asyncify::new(|| {
        BufResult(
            std::fs::OpenOptions::new()
                .read(true)
                .attributes(FILE_FLAG_OVERLAPPED)
                .open("Cargo.toml")
                .map(|f| f.into_raw_handle() as usize),
            (),
        )
    });
    let (fd, _) = push_and_wait(driver, op).unwrap();
    OwnedFd::File(unsafe { OwnedHandle::from_raw_handle(fd as _) })
}

#[cfg(unix)]
fn open_file(driver: &mut Proactor) -> OwnedFd {
    use std::ffi::CString;

    use compio_buf::IntoInner;
    use compio_driver::op::{CurrentDir, OpenFile};

    let op = OpenFile::new(
        CurrentDir,
        CString::new("Cargo.toml").unwrap(),
        OFlags::CLOEXEC | OFlags::RDONLY,
        Mode::from_bits_retain(0o666),
    );
    let (_, op) = push_and_wait(driver, op).unwrap();
    op.into_inner()
}

#[cfg(windows)]
fn open_lock_test_handle(path: &std::path::Path) -> SharedFd<std::fs::File> {
    use std::os::windows::fs::OpenOptionsExt;

    use windows_sys::Win32::Storage::FileSystem::{
        FILE_FLAG_OVERLAPPED, FILE_SHARE_DELETE, FILE_SHARE_READ, FILE_SHARE_WRITE,
    };

    SharedFd::new(
        std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE)
            .custom_flags(FILE_FLAG_OVERLAPPED)
            .open(path)
            .unwrap(),
    )
}

#[cfg(windows)]
fn push_and_wait_lock<O: OpCode + 'static>(driver: &mut Proactor, op: O) -> BufResult<usize, O> {
    match driver.push(op) {
        PushEntry::Ready(result) => result,
        PushEntry::Pending(mut key) => {
            let deadline = std::time::Instant::now() + Duration::from_secs(5);
            loop {
                let now = std::time::Instant::now();
                assert!(now < deadline, "lock operation did not complete in time");
                let timeout = (deadline - now).min(Duration::from_millis(100));
                match driver.poll(Some(timeout)) {
                    Ok(()) => {}
                    Err(error) if error.kind() == io::ErrorKind::TimedOut => continue,
                    Err(error) => panic!("polling lock operation failed: {error}"),
                }
                match driver.pop(key) {
                    PushEntry::Pending(next) => key = next,
                    PushEntry::Ready(result) => return result,
                }
            }
        }
    }
}

#[cfg(windows)]
fn attach_lock_handles(driver: &mut Proactor, handles: &[&SharedFd<std::fs::File>]) {
    for handle in handles {
        driver.attach(handle.as_raw_fd()).unwrap();
    }
}

#[cfg(windows)]
#[test]
fn lock_file_shared_conflict_unlock_and_reacquire() {
    use windows_sys::Win32::Foundation::ERROR_LOCK_VIOLATION;

    let mut driver = Proactor::new().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("lock.tmp");
    std::fs::File::create(&path).unwrap();
    let owner = open_lock_test_handle(&path);
    let contender = open_lock_test_handle(&path);
    attach_lock_handles(&mut driver, &[&owner, &contender]);

    push_and_wait_lock(&mut driver, TryLockFile::shared(owner.clone(), 64, 16))
        .0
        .unwrap();
    let conflict = push_and_wait_lock(
        &mut driver,
        TryLockFile::exclusive(contender.clone(), 64, 16),
    )
    .0
    .unwrap_err();
    assert_eq!(conflict.raw_os_error(), Some(ERROR_LOCK_VIOLATION as _));

    push_and_wait_lock(&mut driver, UnlockFile::new(owner.clone(), 64, 16))
        .0
        .unwrap();
    push_and_wait_lock(
        &mut driver,
        TryLockFile::exclusive(contender.clone(), 64, 16),
    )
    .0
    .unwrap();
    push_and_wait_lock(&mut driver, UnlockFile::new(contender.clone(), 64, 16))
        .0
        .unwrap();
}

#[cfg(windows)]
#[test]
fn lock_file_shared_shared_coexist() {
    use windows_sys::Win32::Foundation::ERROR_LOCK_VIOLATION;

    let mut driver = Proactor::new().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("lock.tmp");
    std::fs::File::create(&path).unwrap();
    let first = open_lock_test_handle(&path);
    let second = open_lock_test_handle(&path);
    let contender = open_lock_test_handle(&path);
    attach_lock_handles(&mut driver, &[&first, &second, &contender]);

    push_and_wait_lock(&mut driver, TryLockFile::shared(first.clone(), 32, 8))
        .0
        .unwrap();
    push_and_wait_lock(&mut driver, TryLockFile::shared(second.clone(), 32, 8))
        .0
        .unwrap();
    let conflict = push_and_wait_lock(
        &mut driver,
        TryLockFile::exclusive(contender.clone(), 32, 8),
    )
    .0
    .unwrap_err();
    assert_eq!(conflict.raw_os_error(), Some(ERROR_LOCK_VIOLATION as _));

    push_and_wait_lock(&mut driver, UnlockFile::new(first.clone(), 32, 8))
        .0
        .unwrap();
    let conflict = push_and_wait_lock(
        &mut driver,
        TryLockFile::exclusive(contender.clone(), 32, 8),
    )
    .0
    .unwrap_err();
    assert_eq!(conflict.raw_os_error(), Some(ERROR_LOCK_VIOLATION as _));

    push_and_wait_lock(&mut driver, UnlockFile::new(second.clone(), 32, 8))
        .0
        .unwrap();
    push_and_wait_lock(
        &mut driver,
        TryLockFile::exclusive(contender.clone(), 32, 8),
    )
    .0
    .unwrap();
    push_and_wait_lock(&mut driver, UnlockFile::new(contender.clone(), 32, 8))
        .0
        .unwrap();
}

#[cfg(windows)]
#[test]
fn lock_file_exclusive_conflicts_with_shared_and_exclusive() {
    use windows_sys::Win32::Foundation::ERROR_LOCK_VIOLATION;

    let mut driver = Proactor::new().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("lock.tmp");
    std::fs::File::create(&path).unwrap();
    let owner = open_lock_test_handle(&path);
    let contender = open_lock_test_handle(&path);
    attach_lock_handles(&mut driver, &[&owner, &contender]);

    push_and_wait_lock(&mut driver, TryLockFile::exclusive(owner.clone(), 48, 8))
        .0
        .unwrap();
    for result in [
        push_and_wait_lock(&mut driver, TryLockFile::shared(contender.clone(), 48, 8)),
        push_and_wait_lock(
            &mut driver,
            TryLockFile::exclusive(contender.clone(), 48, 8),
        ),
    ] {
        let error = result.0.unwrap_err();
        assert_eq!(error.raw_os_error(), Some(ERROR_LOCK_VIOLATION as _));
    }

    push_and_wait_lock(&mut driver, UnlockFile::new(owner.clone(), 48, 8))
        .0
        .unwrap();
    push_and_wait_lock(&mut driver, TryLockFile::shared(contender.clone(), 48, 8))
        .0
        .unwrap();
    push_and_wait_lock(&mut driver, UnlockFile::new(contender.clone(), 48, 8))
        .0
        .unwrap();
    push_and_wait_lock(
        &mut driver,
        TryLockFile::exclusive(contender.clone(), 48, 8),
    )
    .0
    .unwrap();
    push_and_wait_lock(&mut driver, UnlockFile::new(contender.clone(), 48, 8))
        .0
        .unwrap();
}

#[cfg(windows)]
#[test]
fn lock_file_same_handle_exclusive_then_shared_unlocks_exclusive_first() {
    use windows_sys::Win32::Foundation::ERROR_LOCK_VIOLATION;

    let mut driver = Proactor::new().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("lock.tmp");
    std::fs::File::create(&path).unwrap();
    let owner = open_lock_test_handle(&path);
    let contender = open_lock_test_handle(&path);
    attach_lock_handles(&mut driver, &[&owner, &contender]);

    push_and_wait_lock(&mut driver, TryLockFile::exclusive(owner.clone(), 80, 8))
        .0
        .unwrap();
    push_and_wait_lock(&mut driver, TryLockFile::shared(owner.clone(), 80, 8))
        .0
        .unwrap();
    let conflict = push_and_wait_lock(
        &mut driver,
        TryLockFile::exclusive(contender.clone(), 80, 8),
    )
    .0
    .unwrap_err();
    assert_eq!(conflict.raw_os_error(), Some(ERROR_LOCK_VIOLATION as _));

    push_and_wait_lock(&mut driver, UnlockFile::new(owner.clone(), 80, 8))
        .0
        .unwrap();
    push_and_wait_lock(&mut driver, TryLockFile::shared(contender.clone(), 80, 8))
        .0
        .unwrap();
    push_and_wait_lock(&mut driver, UnlockFile::new(contender.clone(), 80, 8))
        .0
        .unwrap();
    let conflict = push_and_wait_lock(
        &mut driver,
        TryLockFile::exclusive(contender.clone(), 80, 8),
    )
    .0
    .unwrap_err();
    assert_eq!(conflict.raw_os_error(), Some(ERROR_LOCK_VIOLATION as _));

    push_and_wait_lock(&mut driver, UnlockFile::new(owner.clone(), 80, 8))
        .0
        .unwrap();
    push_and_wait_lock(
        &mut driver,
        TryLockFile::exclusive(contender.clone(), 80, 8),
    )
    .0
    .unwrap();
    push_and_wait_lock(&mut driver, UnlockFile::new(contender.clone(), 80, 8))
        .0
        .unwrap();
}

#[cfg(windows)]
#[test]
fn lock_file_adjacent_and_disjoint_ranges_are_independent() {
    use windows_sys::Win32::Foundation::ERROR_LOCK_VIOLATION;

    let mut driver = Proactor::new().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("lock.tmp");
    std::fs::File::create(&path).unwrap();
    let owner = open_lock_test_handle(&path);
    let contender = open_lock_test_handle(&path);
    attach_lock_handles(&mut driver, &[&owner, &contender]);

    push_and_wait_lock(&mut driver, TryLockFile::exclusive(owner.clone(), 10, 10))
        .0
        .unwrap();
    push_and_wait_lock(&mut driver, TryLockFile::exclusive(owner.clone(), 20, 10))
        .0
        .unwrap();
    push_and_wait_lock(&mut driver, TryLockFile::exclusive(owner.clone(), 50, 10))
        .0
        .unwrap();
    let error = push_and_wait_lock(
        &mut driver,
        TryLockFile::exclusive(contender.clone(), 20, 10),
    )
    .0
    .unwrap_err();
    assert_eq!(error.raw_os_error(), Some(ERROR_LOCK_VIOLATION as _));
    let error = push_and_wait_lock(
        &mut driver,
        TryLockFile::exclusive(contender.clone(), 50, 10),
    )
    .0
    .unwrap_err();
    assert_eq!(error.raw_os_error(), Some(ERROR_LOCK_VIOLATION as _));
    push_and_wait_lock(
        &mut driver,
        TryLockFile::exclusive(contender.clone(), 30, 10),
    )
    .0
    .unwrap();
    push_and_wait_lock(
        &mut driver,
        TryLockFile::exclusive(contender.clone(), 70, 10),
    )
    .0
    .unwrap();
    push_and_wait_lock(&mut driver, UnlockFile::new(contender.clone(), 30, 10))
        .0
        .unwrap();
    push_and_wait_lock(&mut driver, UnlockFile::new(contender.clone(), 70, 10))
        .0
        .unwrap();

    push_and_wait_lock(&mut driver, UnlockFile::new(owner.clone(), 10, 10))
        .0
        .unwrap();
    push_and_wait_lock(
        &mut driver,
        TryLockFile::exclusive(contender.clone(), 10, 10),
    )
    .0
    .unwrap();
    let conflict = push_and_wait_lock(
        &mut driver,
        TryLockFile::exclusive(contender.clone(), 50, 10),
    )
    .0
    .unwrap_err();
    assert_eq!(conflict.raw_os_error(), Some(ERROR_LOCK_VIOLATION as _));
    push_and_wait_lock(&mut driver, UnlockFile::new(contender.clone(), 10, 10))
        .0
        .unwrap();
    push_and_wait_lock(&mut driver, UnlockFile::new(owner.clone(), 20, 10))
        .0
        .unwrap();
    push_and_wait_lock(&mut driver, UnlockFile::new(owner.clone(), 50, 10))
        .0
        .unwrap();
    push_and_wait_lock(
        &mut driver,
        TryLockFile::exclusive(contender.clone(), 20, 10),
    )
    .0
    .unwrap();
    push_and_wait_lock(&mut driver, UnlockFile::new(contender.clone(), 20, 10))
        .0
        .unwrap();
    push_and_wait_lock(
        &mut driver,
        TryLockFile::exclusive(contender.clone(), 50, 10),
    )
    .0
    .unwrap();
    push_and_wait_lock(&mut driver, UnlockFile::new(contender.clone(), 50, 10))
        .0
        .unwrap();
}

#[cfg(windows)]
#[test]
fn unlock_file_requires_exact_range_and_preserves_other_lock() {
    use windows_sys::Win32::Foundation::{ERROR_LOCK_VIOLATION, ERROR_NOT_LOCKED};

    let mut driver = Proactor::new().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("lock.tmp");
    std::fs::File::create(&path).unwrap();
    let owner = open_lock_test_handle(&path);
    let contender = open_lock_test_handle(&path);
    attach_lock_handles(&mut driver, &[&owner, &contender]);

    push_and_wait_lock(&mut driver, TryLockFile::exclusive(owner.clone(), 96, 16))
        .0
        .unwrap();
    push_and_wait_lock(&mut driver, TryLockFile::exclusive(owner.clone(), 160, 8))
        .0
        .unwrap();
    push_and_wait_lock(&mut driver, TryLockFile::exclusive(owner.clone(), 200, 8))
        .0
        .unwrap();
    push_and_wait_lock(&mut driver, TryLockFile::exclusive(owner.clone(), 208, 8))
        .0
        .unwrap();

    let error = push_and_wait_lock(&mut driver, UnlockFile::new(owner.clone(), 95, 16))
        .0
        .unwrap_err();
    assert_eq!(error.raw_os_error(), Some(ERROR_NOT_LOCKED as _));
    let error = push_and_wait_lock(&mut driver, UnlockFile::new(owner.clone(), 96, 8))
        .0
        .unwrap_err();
    assert_eq!(error.raw_os_error(), Some(ERROR_NOT_LOCKED as _));
    let error = push_and_wait_lock(&mut driver, UnlockFile::new(owner.clone(), 200, 16))
        .0
        .unwrap_err();
    assert_eq!(error.raw_os_error(), Some(ERROR_NOT_LOCKED as _));
    for offset in [96, 160, 200, 208] {
        let error = push_and_wait_lock(
            &mut driver,
            TryLockFile::exclusive(contender.clone(), offset, 1),
        )
        .0
        .unwrap_err();
        assert_eq!(error.raw_os_error(), Some(ERROR_LOCK_VIOLATION as _));
    }

    push_and_wait_lock(&mut driver, UnlockFile::new(owner.clone(), 96, 16))
        .0
        .unwrap();
    push_and_wait_lock(
        &mut driver,
        TryLockFile::exclusive(contender.clone(), 96, 16),
    )
    .0
    .unwrap();
    let error = push_and_wait_lock(
        &mut driver,
        TryLockFile::exclusive(contender.clone(), 160, 1),
    )
    .0
    .unwrap_err();
    assert_eq!(error.raw_os_error(), Some(ERROR_LOCK_VIOLATION as _));

    push_and_wait_lock(&mut driver, UnlockFile::new(owner.clone(), 160, 8))
        .0
        .unwrap();
    push_and_wait_lock(&mut driver, UnlockFile::new(owner.clone(), 200, 8))
        .0
        .unwrap();
    push_and_wait_lock(&mut driver, UnlockFile::new(owner.clone(), 208, 8))
        .0
        .unwrap();
    push_and_wait_lock(&mut driver, UnlockFile::new(contender.clone(), 96, 16))
        .0
        .unwrap();
    push_and_wait_lock(
        &mut driver,
        TryLockFile::exclusive(contender.clone(), 160, 8),
    )
    .0
    .unwrap();
    push_and_wait_lock(&mut driver, UnlockFile::new(contender.clone(), 160, 8))
        .0
        .unwrap();
    for offset in [200, 208] {
        push_and_wait_lock(
            &mut driver,
            TryLockFile::exclusive(contender.clone(), offset, 8),
        )
        .0
        .unwrap();
        push_and_wait_lock(&mut driver, UnlockFile::new(contender.clone(), offset, 8))
            .0
            .unwrap();
    }
}

#[cfg(windows)]
#[test]
fn lock_file_splits_high_offset_bits() {
    use windows_sys::Win32::Foundation::ERROR_LOCK_VIOLATION;

    let mut driver = Proactor::new().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("lock.tmp");
    std::fs::File::create(&path).unwrap();
    let owner = open_lock_test_handle(&path);
    let contender = open_lock_test_handle(&path);
    attach_lock_handles(&mut driver, &[&owner, &contender]);
    let high_offset = (1_u64 << 32) + 0x100;

    push_and_wait_lock(
        &mut driver,
        TryLockFile::exclusive(owner.clone(), high_offset, 8),
    )
    .0
    .unwrap();
    push_and_wait_lock(
        &mut driver,
        TryLockFile::exclusive(contender.clone(), 0x100, 8),
    )
    .0
    .unwrap();
    let conflict = push_and_wait_lock(
        &mut driver,
        TryLockFile::exclusive(contender.clone(), high_offset, 8),
    )
    .0
    .unwrap_err();
    assert_eq!(conflict.raw_os_error(), Some(ERROR_LOCK_VIOLATION as _));

    push_and_wait_lock(&mut driver, UnlockFile::new(owner.clone(), high_offset, 8))
        .0
        .unwrap();
    push_and_wait_lock(&mut driver, UnlockFile::new(contender.clone(), 0x100, 8))
        .0
        .unwrap();
    push_and_wait_lock(
        &mut driver,
        TryLockFile::exclusive(contender.clone(), high_offset, 8),
    )
    .0
    .unwrap();
    push_and_wait_lock(
        &mut driver,
        UnlockFile::new(contender.clone(), high_offset, 8),
    )
    .0
    .unwrap();
}

#[cfg(windows)]
#[test]
fn lock_file_splits_high_length_bits() {
    use windows_sys::Win32::Foundation::ERROR_LOCK_VIOLATION;

    let mut driver = Proactor::new().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("lock.tmp");
    std::fs::File::create(&path).unwrap();
    let owner = open_lock_test_handle(&path);
    let contender = open_lock_test_handle(&path);
    attach_lock_handles(&mut driver, &[&owner, &contender]);
    let high_length = (1_u64 << 32) + 0x100;
    let high_offset = (1_u64 << 32) + 0x80;

    push_and_wait_lock(
        &mut driver,
        TryLockFile::exclusive(owner.clone(), 0, high_length),
    )
    .0
    .unwrap();
    let conflict = push_and_wait_lock(
        &mut driver,
        TryLockFile::exclusive(contender.clone(), high_offset, 1),
    )
    .0
    .unwrap_err();
    assert_eq!(conflict.raw_os_error(), Some(ERROR_LOCK_VIOLATION as _));
    push_and_wait_lock(
        &mut driver,
        TryLockFile::exclusive(contender.clone(), high_length, 1),
    )
    .0
    .unwrap();
    push_and_wait_lock(
        &mut driver,
        UnlockFile::new(contender.clone(), high_length, 1),
    )
    .0
    .unwrap();
    push_and_wait_lock(&mut driver, UnlockFile::new(owner.clone(), 0, high_length))
        .0
        .unwrap();
    push_and_wait_lock(
        &mut driver,
        TryLockFile::exclusive(contender.clone(), high_offset, 1),
    )
    .0
    .unwrap();
    push_and_wait_lock(
        &mut driver,
        UnlockFile::new(contender.clone(), high_offset, 1),
    )
    .0
    .unwrap();
}

#[cfg(windows)]
#[test]
fn unlock_file_splits_high_length_bits() {
    use windows_sys::Win32::Foundation::{ERROR_LOCK_VIOLATION, ERROR_NOT_LOCKED};

    let mut driver = Proactor::new().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("lock.tmp");
    std::fs::File::create(&path).unwrap();
    let owner = open_lock_test_handle(&path);
    let contender = open_lock_test_handle(&path);
    attach_lock_handles(&mut driver, &[&owner, &contender]);
    let high_length = (1_u64 << 32) + 0x100;
    let high_offset = (1_u64 << 32) + 0x80;

    push_and_wait_lock(
        &mut driver,
        TryLockFile::exclusive(owner.clone(), 0, high_length),
    )
    .0
    .unwrap();
    let error = push_and_wait_lock(&mut driver, UnlockFile::new(owner.clone(), 0, 0x100))
        .0
        .unwrap_err();
    assert_eq!(error.raw_os_error(), Some(ERROR_NOT_LOCKED as _));
    let conflict = push_and_wait_lock(
        &mut driver,
        TryLockFile::exclusive(contender.clone(), high_offset, 1),
    )
    .0
    .unwrap_err();
    assert_eq!(conflict.raw_os_error(), Some(ERROR_LOCK_VIOLATION as _));

    push_and_wait_lock(&mut driver, UnlockFile::new(owner.clone(), 0, high_length))
        .0
        .unwrap();
    push_and_wait_lock(
        &mut driver,
        TryLockFile::exclusive(contender.clone(), high_offset, 1),
    )
    .0
    .unwrap();
    push_and_wait_lock(
        &mut driver,
        UnlockFile::new(contender.clone(), high_offset, 1),
    )
    .0
    .unwrap();
}

#[cfg(windows)]
#[test]
fn unlock_file_preserves_os_failure() {
    use windows_sys::Win32::Foundation::ERROR_NOT_LOCKED;

    let mut driver = Proactor::new().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("lock.tmp");
    std::fs::File::create(&path).unwrap();
    let handle = open_lock_test_handle(&path);
    attach_lock_handles(&mut driver, &[&handle]);

    let error = push_and_wait_lock(&mut driver, UnlockFile::new(handle.clone(), 128, 4))
        .0
        .unwrap_err();
    assert_eq!(error.raw_os_error(), Some(ERROR_NOT_LOCKED as _));
}

#[cfg(windows)]
#[test]
fn lock_file_records_native_submission_state() {
    use windows_sys::Win32::Foundation::ERROR_LOCK_VIOLATION;

    let mut driver = Proactor::new().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("lock.tmp");
    std::fs::File::create(&path).unwrap();
    let owner = open_lock_test_handle(&path);
    let contender = open_lock_test_handle(&path);
    attach_lock_handles(&mut driver, &[&owner, &contender]);

    let (submission, result) =
        push_and_wait_lock_observing(&mut driver, TryLockFile::exclusive(owner.clone(), 32, 8));
    eprintln!("native LockFileEx submission: {submission}");
    result.0.unwrap();

    let conflict = push_and_wait_lock(
        &mut driver,
        TryLockFile::exclusive(contender.clone(), 32, 8),
    )
    .0
    .unwrap_err();
    assert_eq!(conflict.raw_os_error(), Some(ERROR_LOCK_VIOLATION as _));

    let (submission, result) =
        push_and_wait_lock_observing(&mut driver, UnlockFile::new(owner.clone(), 32, 8));
    eprintln!("native UnlockFileEx submission: {submission}");
    result.0.unwrap();
    push_and_wait_lock(
        &mut driver,
        TryLockFile::exclusive(contender.clone(), 32, 8),
    )
    .0
    .unwrap();
    push_and_wait_lock(&mut driver, UnlockFile::new(contender.clone(), 32, 8))
        .0
        .unwrap();
}

#[cfg(windows)]
#[test]
fn controlled_pending_lock_retains_descriptor_until_completion_after_cancel() {
    use windows_sys::Win32::Foundation::ERROR_LOCK_VIOLATION;

    let mut driver = Proactor::new().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("lock.tmp");
    std::fs::File::create(&path).unwrap();
    let owner = open_lock_test_handle(&path);
    let contender = open_lock_test_handle(&path);
    attach_lock_handles(&mut driver, &[&owner, &contender]);

    let (submission, BufResult(result, operation)) =
        push_and_wait_lock_observing(&mut driver, TryLockFile::exclusive(owner.clone(), 48, 8));
    eprintln!("native LockFileEx submission for controlled retention: {submission}");
    result.unwrap();

    let op = ControlledRangeCompletion::lock(operation, driver.as_raw_fd());
    let key = match driver.push(op) {
        PushEntry::Pending(key) => key,
        PushEntry::Ready(_) => panic!("controlled completion must return pending"),
    };
    let owner = match owner.try_unwrap() {
        Err(owner) => owner,
        Ok(_) => panic!("the controlled pending operation must retain its descriptor"),
    };

    let conflict = push_and_wait_lock(
        &mut driver,
        TryLockFile::exclusive(contender.clone(), 48, 8),
    )
    .0
    .unwrap_err();
    assert_eq!(conflict.raw_os_error(), Some(ERROR_LOCK_VIOLATION as _));

    let token = driver.register_cancel(&key);
    assert!(driver.cancel_token(token));
    let conflict = push_and_wait_lock(
        &mut driver,
        TryLockFile::exclusive(contender.clone(), 48, 8),
    )
    .0
    .unwrap_err();
    assert_eq!(conflict.raw_os_error(), Some(ERROR_LOCK_VIOLATION as _));
    let completion = wait_for_controlled_completion(&mut driver, key);
    completion.0.unwrap();
    drop(completion.1);
    let owner_file = match owner.try_unwrap() {
        Ok(file) => file,
        Err(_) => panic!("the completed operation released its descriptor"),
    };
    let conflict = push_and_wait_lock(
        &mut driver,
        TryLockFile::exclusive(contender.clone(), 48, 8),
    )
    .0
    .unwrap_err();
    assert_eq!(conflict.raw_os_error(), Some(ERROR_LOCK_VIOLATION as _));
    drop(owner_file);

    push_and_wait_lock(
        &mut driver,
        TryLockFile::exclusive(contender.clone(), 48, 8),
    )
    .0
    .unwrap();
    push_and_wait_lock(&mut driver, UnlockFile::new(contender.clone(), 48, 8))
        .0
        .unwrap();
}

#[cfg(windows)]
#[test]
fn controlled_pending_unlock_cancel_reports_actual_range_state() {
    let mut driver = Proactor::new().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("lock.tmp");
    std::fs::File::create(&path).unwrap();
    let owner = open_lock_test_handle(&path);
    let contender = open_lock_test_handle(&path);
    attach_lock_handles(&mut driver, &[&owner, &contender]);

    push_and_wait_lock(&mut driver, TryLockFile::exclusive(owner.clone(), 64, 8))
        .0
        .unwrap();

    let (submission, BufResult(result, operation)) =
        push_and_wait_lock_observing(&mut driver, UnlockFile::new(owner.clone(), 64, 8));
    eprintln!("native UnlockFileEx submission for controlled retention: {submission}");
    result.unwrap();

    let op = ControlledRangeCompletion::unlock(operation, driver.as_raw_fd());
    let key = match driver.push(op) {
        PushEntry::Pending(key) => key,
        PushEntry::Ready(_) => panic!("controlled completion must return pending"),
    };
    let owner = match owner.try_unwrap() {
        Err(owner) => owner,
        Ok(_) => panic!("the controlled pending operation must retain its descriptor"),
    };

    let token = driver.register_cancel(&key);
    assert!(driver.cancel_token(token));
    push_and_wait_lock(
        &mut driver,
        TryLockFile::exclusive(contender.clone(), 64, 8),
    )
    .0
    .unwrap();
    push_and_wait_lock(&mut driver, UnlockFile::new(contender.clone(), 64, 8))
        .0
        .unwrap();
    let completion = wait_for_controlled_completion(&mut driver, key);
    completion.0.unwrap();
    drop(completion.1);
    let owner_file = match owner.try_unwrap() {
        Ok(file) => file,
        Err(_) => panic!("the completed operation released its descriptor"),
    };
    drop(owner_file);

    push_and_wait_lock(
        &mut driver,
        TryLockFile::exclusive(contender.clone(), 64, 8),
    )
    .0
    .unwrap();
    push_and_wait_lock(&mut driver, UnlockFile::new(contender.clone(), 64, 8))
        .0
        .unwrap();
}

#[cfg(windows)]
fn push_and_wait_lock_observing<O: OpCode + 'static>(
    driver: &mut Proactor,
    op: O,
) -> (&'static str, BufResult<usize, O>) {
    match driver.push(op) {
        PushEntry::Ready(result) => ("Ready", result),
        PushEntry::Pending(mut key) => {
            let deadline = std::time::Instant::now() + Duration::from_secs(5);
            loop {
                let now = std::time::Instant::now();
                assert!(now < deadline, "lock operation did not complete in time");
                let timeout = (deadline - now).min(Duration::from_millis(100));
                match driver.poll(Some(timeout)) {
                    Ok(()) => {}
                    Err(error) if error.kind() == io::ErrorKind::TimedOut => continue,
                    Err(error) => panic!("polling lock operation failed: {error}"),
                }
                match driver.pop(key) {
                    PushEntry::Pending(next) => key = next,
                    PushEntry::Ready(result) => return ("Pending", result),
                }
            }
        }
    }
}

#[cfg(windows)]
fn wait_for_controlled_completion(
    driver: &mut Proactor,
    mut key: compio_driver::Key<ControlledRangeCompletion>,
) -> BufResult<usize, ControlledRangeCompletion> {
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        let now = std::time::Instant::now();
        assert!(
            now < deadline,
            "controlled completion packet was not observed"
        );
        match driver.poll(Some((deadline - now).min(Duration::from_millis(100)))) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::TimedOut => continue,
            Err(error) => panic!("polling controlled completion failed: {error}"),
        }
        match driver.pop(key) {
            PushEntry::Pending(next) => key = next,
            PushEntry::Ready(result) => return result,
        }
    }
}

#[cfg(windows)]
enum ControlledRangeOperation {
    Lock(TryLockFile<SharedFd<std::fs::File>>),
    Unlock(UnlockFile<SharedFd<std::fs::File>>),
}

#[cfg(windows)]
// Tests complete the real syscall first. The synthetic packet then exercises
// queued retention and cancel-after-success, not native Pending completion.
struct ControlledRangeCompletion {
    operation: ControlledRangeOperation,
    completion_port: compio_driver::RawFd,
}

#[cfg(windows)]
impl ControlledRangeCompletion {
    fn lock(
        operation: TryLockFile<SharedFd<std::fs::File>>,
        completion_port: compio_driver::RawFd,
    ) -> Self {
        Self {
            operation: ControlledRangeOperation::Lock(operation),
            completion_port,
        }
    }

    fn unlock(
        operation: UnlockFile<SharedFd<std::fs::File>>,
        completion_port: compio_driver::RawFd,
    ) -> Self {
        Self {
            operation: ControlledRangeOperation::Unlock(operation),
            completion_port,
        }
    }
}

#[cfg(windows)]
// SAFETY: the driver pins the wrapper and its inner operation until the queued
// completion is consumed, and the wrapper forwards cancellation to that
// operation.
unsafe impl OpCode for ControlledRangeCompletion {
    type Control = ();

    unsafe fn operate(
        &mut self,
        _control: &mut Self::Control,
        optr: *mut windows_sys::Win32::System::IO::OVERLAPPED,
    ) -> std::task::Poll<io::Result<usize>> {
        // SAFETY: the driver pins this wrapper and provides its live OVERLAPPED
        // storage.
        let overlapped = unsafe { &mut *optr };
        overlapped.Internal = 0;
        overlapped.InternalHigh = 0;

        // SAFETY: this port belongs to the Proactor that supplied optr. No
        // native I/O is submitted here; the completed operation remains owned
        // by this wrapper.
        let posted = unsafe {
            windows_sys::Win32::System::IO::PostQueuedCompletionStatus(
                self.completion_port as _,
                0,
                0,
                optr,
            )
        };
        if posted == 0 {
            std::task::Poll::Ready(Err(io::Error::last_os_error()))
        } else {
            std::task::Poll::Pending
        }
    }

    fn cancel(
        &mut self,
        control: &mut Self::Control,
        optr: *mut windows_sys::Win32::System::IO::OVERLAPPED,
    ) -> io::Result<()> {
        match &mut self.operation {
            ControlledRangeOperation::Lock(op) => OpCode::cancel(op, control, optr),
            ControlledRangeOperation::Unlock(op) => OpCode::cancel(op, control, optr),
        }
    }
}

fn push_and_wait<O: OpCode + 'static>(driver: &mut Proactor, op: O) -> BufResult<usize, O> {
    match driver.push(op) {
        PushEntry::Ready(res) => res,
        PushEntry::Pending(mut user_data) => loop {
            driver.poll(None).unwrap();
            match driver.pop(user_data) {
                PushEntry::Pending(k) => user_data = k,
                PushEntry::Ready(res) => break res,
            }
        },
    }
}

fn push_and_wait_multi<O: OpCode + 'static>(
    driver: &mut Proactor,
    op: O,
) -> impl Iterator<Item = BufResult<usize, (Extra, Option<O>)>> + '_ {
    let mut op = Some(op);
    let mut user_data = None;
    let mut finished = false;

    std::iter::from_fn(move || {
        if finished {
            return None;
        }

        if user_data.is_none() {
            match driver.push(op.take().expect("operation should be pushed once")) {
                PushEntry::Ready(BufResult(res, op)) => {
                    finished = true;
                    return Some(BufResult(res, (driver.default_extra(), Some(op))));
                }
                PushEntry::Pending(k) => user_data = Some(k),
            }
        }

        loop {
            if let Some(res) = user_data.as_ref().and_then(|key| driver.pop_multishot(key)) {
                return Some(res.map_buffer(|extra| (extra, None)));
            }

            let key = user_data.take().expect("pending key should exist");
            match driver.pop_with_extra(key) {
                PushEntry::Pending(k) => user_data = Some(k),
                PushEntry::Ready((BufResult(res, op), extra)) => {
                    finished = true;
                    return Some(BufResult(res, (extra, Some(op))));
                }
            }

            driver.poll(None).unwrap();
        }
    })
}

#[test]
fn timeout() {
    let mut driver = Proactor::new().unwrap();

    let err = driver.poll(Some(Duration::from_secs(1))).unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::TimedOut);
}

#[test]
fn register_multiple() {
    const TASK_LEN: usize = 5;

    let mut driver = Proactor::new().unwrap();

    let fd = open_file(&mut driver);
    let fd = SharedFd::new(fd);
    driver.attach(fd.as_raw_fd()).unwrap();

    let mut keys = vec![];

    for _i in 0..TASK_LEN {
        match driver.push(ReadAt::new(fd.clone(), 0, Vec::with_capacity(1024))) {
            PushEntry::Pending(key) => keys.push(key),
            PushEntry::Ready(res) => {
                res.unwrap();
            }
        }
    }

    driver.poll(None).unwrap();

    // Cancel the entries to drop the ops, and decrease the ref count of fd.
    for key in keys {
        driver.cancel(key);
    }

    // Don't async close because the reading operations may have not completed.
}

#[cfg(io_uring)]
#[test]
fn poll_full_submission_queue() {
    const CAPACITY: u32 = 8;

    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let result = (|| {
            let mut driver = Proactor::builder()
                .capacity(CAPACITY)
                .driver_type(compio_driver::DriverType::IoUring)
                .build()?;
            let fd = SharedFd::new(std::fs::File::open("/dev/null")?);

            let _keys = (0..CAPACITY * 3)
                .map(|_| driver.push(Write::new(fd.clone(), "114514")))
                .collect::<Vec<_>>();

            driver.poll(None)
        })();
        tx.send(result).unwrap();
    });

    rx.recv_timeout(Duration::from_secs(5))
        .expect("poll blocked with a full submission queue")
        .unwrap();
}

#[test]
#[cfg(unix)]
fn cancel_token() {
    use compio_buf::IntoInner;

    let mut driver = Proactor::new().unwrap();

    let op = Pipe::new();
    let (_, op) = push_and_wait(&mut driver, op).unwrap();
    let (r, _w) = op.into_inner();

    let mut key = match driver.push(ReadAt::new(r, 0, Vec::with_capacity(1024))) {
        PushEntry::Pending(key) => key,
        PushEntry::Ready(res) => {
            res.unwrap();
            return;
        }
    };

    let token = driver.register_cancel(&key);
    let token2 = token.clone();
    let token3 = token.clone();

    assert!(driver.cancel_token(token));
    assert!(!driver.cancel_token(token2));

    let res = loop {
        driver.poll(None).unwrap();
        match driver.pop(key) {
            PushEntry::Pending(k) => key = k,
            PushEntry::Ready(res) => break res,
        }
    };

    assert!(res.0.is_err());
    assert!(!driver.cancel_token(token3));
}

#[test]
fn notify() {
    let mut driver = Proactor::new().unwrap();

    let waker = driver.waker();

    let thread = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_secs(1));
        waker.wake();
    });

    driver.poll(None).unwrap();

    thread.join().unwrap();
}

#[test]
fn asyncify() {
    let mut driver = Proactor::new().unwrap();

    let op = Asyncify::new(|| BufResult(Ok(114514), ()));
    let (res, _) = push_and_wait(&mut driver, op).unwrap();
    assert_eq!(res, 114514);
}

#[test]
fn managed() {
    let mut driver = Proactor::new().unwrap();

    let fd = open_file(&mut driver);
    let fd = SharedFd::new(fd);
    driver.attach(fd.as_raw_fd()).unwrap();

    let pool = driver.buffer_pool().unwrap();

    let op = ReadManagedAt::new(fd.clone(), 0, &pool, 1024).unwrap();
    let res = push_and_wait(&mut driver, op);

    let buffer = unsafe { res.take_buffer() }.unwrap().unwrap();
    println!("{}", std::str::from_utf8(&buffer).unwrap());
    drop(buffer);

    let op = CloseFile::new(fd.try_unwrap().unwrap());
    push_and_wait(&mut driver, op).unwrap();
}

#[test]
#[cfg(unix)]
fn read_multi() {
    use compio_buf::IntoInner;

    let mut driver = Proactor::new().unwrap();

    let pool = driver.buffer_pool().unwrap();

    let op = Pipe::new();
    let (_, op) = push_and_wait(&mut driver, op).unwrap();
    let (r, w) = op.into_inner();

    let op = Write::new(w, b"hello world");
    push_and_wait(&mut driver, op).unwrap();

    let op = ReadMulti::new(r, &pool, 0).unwrap();
    let buffer = push_and_wait_multi(&mut driver, op)
        .try_fold(Vec::new(), |mut acc, BufResult(res, (extra, op))| {
            let mut buf = if let Some(op) = op {
                op.take_buffer()
            } else {
                pool.take(extra.buffer_id()?)?
            };
            if let Some(ref mut buf) = buf {
                unsafe { buf.advance_to(res?) }
                acc.extend_from_slice(buf);
            };
            io::Result::Ok(acc)
        })
        .unwrap();

    assert_eq!(buffer, b"hello world");
}

#[test]
fn recv_multi() {
    let mut driver = Proactor::builder()
        .buffer_pool_size(NonZeroU16::new(16).unwrap())
        .build()
        .unwrap();

    let pool = driver.buffer_pool().unwrap();

    let server = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = server.local_addr().unwrap();
    std::thread::spawn(move || {
        let (mut stream, _) = server.accept().unwrap();
        stream.write_all(b"hello ").unwrap();
        stream.write_all(b"world").unwrap();
        stream.shutdown(std::net::Shutdown::Both).unwrap();
    });

    let stream = TcpStream::connect(addr).unwrap();
    let stream = socket2::Socket::from(stream);
    if driver.driver_type().is_polling() {
        stream.set_nonblocking(true).unwrap();
    }
    let stream = SharedFd::new(stream);

    driver.attach(stream.as_raw_fd()).unwrap();

    let mut buffer = vec![];
    loop {
        let op = RecvMulti::new(stream.clone(), &pool, 0, RecvFlags::empty()).unwrap();
        let slice = push_and_wait_multi(&mut driver, op)
            .try_fold(Vec::new(), |mut acc, BufResult(res, (extra, op))| {
                let mut buf = if let Some(op) = op {
                    op.take_buffer()
                } else {
                    pool.take(extra.buffer_id()?)?
                };
                if let Some(ref mut buf) = buf {
                    unsafe { buf.advance_to(res?) }
                    acc.extend_from_slice(buf);
                };
                io::Result::Ok(acc)
            })
            .unwrap();
        if slice.is_empty() {
            break;
        }
        buffer.extend_from_slice(&slice);
    }
    assert_eq!(buffer, b"hello world");

    let stream = stream.try_unwrap().unwrap();
    let op = CloseSocket::new(stream.into());
    push_and_wait(&mut driver, op).unwrap();
}

#[cfg(unix)]
#[test]
fn accept_multi() {
    let server = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = server.local_addr().unwrap();

    let handle = std::thread::spawn(move || {
        let mut driver = Proactor::new().unwrap();

        let server = socket2::Socket::from(server);
        if driver.driver_type().is_polling() {
            server.set_nonblocking(true).unwrap();
        }
        let server = SharedFd::new(server);

        driver.attach(server.as_raw_fd()).unwrap();

        let mut i = 0;
        loop {
            let op = AcceptMulti::new(server.clone());
            for BufResult(res, (_, op)) in push_and_wait_multi(&mut driver, op) {
                let mut client = if let Some(op) = op {
                    use compio_buf::IntoInner;

                    op.into_inner()
                } else {
                    unsafe {
                        use std::os::fd::FromRawFd;
                        socket2::Socket::from_raw_fd(res.unwrap() as _)
                    }
                };
                client
                    .write_all(format!("Hello, {}", i).as_bytes())
                    .unwrap();
                client.shutdown(std::net::Shutdown::Both).unwrap();
                i += 1;
                if i >= 2 {
                    return;
                }
            }
        }
    });
    for i in 0..2 {
        use std::io::Read;

        let mut client = TcpStream::connect(addr).unwrap();
        let mut s = String::new();
        client.read_to_string(&mut s).unwrap();
        assert_eq!(s, format!("Hello, {}", i));
    }
    if let Err(e) = handle.join() {
        std::panic::resume_unwind(e)
    }
}

#[test]
#[cfg(all(target_pointer_width = "64", any(io_uring, target_os = "windows")))]
fn read_len_over_u32() {
    let mut driver = Proactor::new().unwrap();

    let fd = open_file(&mut driver);
    let fd = SharedFd::new(fd);
    driver.attach(fd.as_raw_fd()).unwrap();

    let buffer = Vec::with_capacity(1 << 32);

    let op = ReadAt::new(fd.clone(), 0, buffer);
    let (res, _) = push_and_wait(&mut driver, op).unwrap();

    assert!(res > 0);

    let op = CloseFile::new(fd.try_unwrap().unwrap());
    push_and_wait(&mut driver, op).unwrap();
}

/// Dropping a Proactor with in-flight ops must release them on the drivers that
/// cancel on drop (io_uring, poll), and must leak them on IOCP, which cannot
/// cancel on drop
#[test]
fn drop_with_inflight_ops() {
    use std::net::UdpSocket;

    use compio_driver::op::Recv;

    let mut driver = Proactor::new().unwrap();

    // Nobody sends to this socket, so the recv op stays in-flight.
    let socket = UdpSocket::bind("127.0.0.1:0").unwrap();
    if driver.driver_type().is_polling() {
        socket.set_nonblocking(true).unwrap();
    }
    let socket = SharedFd::new(socket);
    driver.attach(socket.as_raw_fd()).unwrap();

    let op = Recv::new(socket.clone(), Vec::with_capacity(1024), RecvFlags::empty());
    let PushEntry::Pending(_) = driver.push(op) else {
        panic!("recv on an idle socket should be pending")
    };

    drop(driver);

    if cfg!(windows) {
        // IOCP is a real proactor: dropping the port does not cancel a
        // submitted op, so the kernel may still write into its buffer.
        // Releasing the key or the fd would be a use-after-free, so the
        // op is leaked on purpose.
        assert!(
            socket.try_unwrap().is_err(),
            "IOCP must not release in-flight ops"
        );
    } else {
        // io_uring closes the ring on drop, which makes the kernel cancel the
        // op, and the poll driver owns its keys outright. Both must
        // then release the op along with the fd clone it owned.
        assert!(socket.try_unwrap().is_ok(), "in-flight op leaked on drop");
    }
}

/// A multishot op stays registered across completions, so the completion queue
/// can hold several CQEs with the same key when the Proactor is dropped.
/// Dropping the driver must still release the op once, no matter how many of
/// those CQEs are queued.
#[cfg(io_uring)]
#[test]
fn drop_with_pending_multishot_cqes() {
    let mut driver = Proactor::new().unwrap();
    if driver.driver_type().is_polling() {
        return; // the polling driver has no completion queue
    }

    let server = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = server.local_addr().unwrap();
    // Connect before submitting, so both accepts succeed inside the submit
    // below instead of waiting on the kernel to notice the connections.
    let _clients = [
        TcpStream::connect(addr).unwrap(),
        TcpStream::connect(addr).unwrap(),
    ];
    let server = SharedFd::new(socket2::Socket::from(server));

    let PushEntry::Pending(_key) = driver.push(AcceptMulti::new(server.clone())) else {
        panic!("a multishot accept is never ready on push")
    };
    // Submit without reaping. Both accepts post a CQE carrying `more`, and
    // nothing consumes them.
    driver.flush();

    drop(driver);

    // `_key` still holds the op, so the driver released its own reference and
    // only that one.
    assert!(
        server.try_unwrap().is_err(),
        "the driver released a multishot key more than once"
    );
}
