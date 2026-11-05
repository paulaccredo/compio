use std::io::prelude::*;

use compio_fs::{File, OpenOptions};
use compio_io::{AsyncReadAtExt, AsyncReadExt, AsyncWriteAt, AsyncWriteAtExt};
use futures_util::StreamExt;
use tempfile::NamedTempFile;

async fn setlen_run(file: &File, size: u64) {
    file.set_len(size).await.unwrap();
    // For predictability. Give the uring just enough time to ensure that it's
    // completed
    compio_runtime::time::sleep(std::time::Duration::from_millis(0)).await;

    let meta = file.metadata().await.unwrap();
    assert_eq!(size, meta.len());
}

#[compio_macros::test]
async fn iouring_setlen_non_fixed() {
    let tempfile = tempfile();
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(tempfile.path())
        .await
        .unwrap();
    setlen_run(&file, 5).await;

    setlen_run(&file, 0).await;
}

#[compio_macros::test]
async fn metadata() {
    let meta = compio_fs::metadata("Cargo.toml").await.unwrap();
    assert!(meta.is_file());
    let size = meta.len();

    let file = File::open("Cargo.toml").await.unwrap();
    let meta = file.metadata().await.unwrap();
    assert!(meta.is_file());
    assert_eq!(size, meta.len());

    let std_meta = std::fs::metadata("Cargo.toml").unwrap();
    assert_eq!(size, std_meta.len());

    // `created()` must report the birth time and `ctime()` the inode change
    // time, matching std's semantics.
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;

        assert_eq!(std_meta.ctime(), meta.ctime());
        assert_eq!(std_meta.ctime_nsec(), meta.ctime_nsec());

        if let Ok(std_created) = std_meta.created() {
            assert_eq!(std_created, meta.created().unwrap());
        }
    }
}

#[compio_macros::test]
async fn file_stream() {
    let mut tempfile = tempfile();
    tempfile.write_all(HELLO).unwrap();

    let file = File::open(tempfile.path()).await.unwrap();
    let cursor = std::io::Cursor::new(file);
    let mut file = cursor.read_only().bytes();
    let mut s = String::new();
    while let Some(result) = file.next().await {
        let chunk = result.unwrap();
        s.push_str(str::from_utf8(&chunk).unwrap());
    }
    assert_eq!(s, String::from_utf8_lossy(HELLO));
}

const HELLO: &[u8] = b"hello world...";

async fn read_hello(file: &File) {
    let buf = Vec::with_capacity(1024);
    let (n, buf) = file.read_to_end_at(buf, 0).await.unwrap();

    assert_eq!(n, HELLO.len());
    assert_eq!(&buf, HELLO);
}

#[compio_macros::test]
async fn basic_read() {
    let mut tempfile = tempfile();
    tempfile.write_all(HELLO).unwrap();

    let file = File::open(tempfile.path()).await.unwrap();
    read_hello(&file).await;
}

#[compio_macros::test]
async fn basic_write() {
    let tempfile = tempfile();

    let mut file = File::create(tempfile.path()).await.unwrap();

    file.write_all_at(HELLO, 0).await.0.unwrap();
    file.sync_all().await.unwrap();

    let file = std::fs::read(tempfile.path()).unwrap();
    assert_eq!(file, HELLO);
}

#[compio_macros::test]
async fn writev() {
    let tempfile = tempfile();

    let mut file = File::create(tempfile.path()).await.unwrap();

    let (write, _) = file.write_vectored_at([HELLO, HELLO], 0).await.unwrap();
    assert!(write > 0);
}

#[compio_macros::test]
async fn cancel_read() {
    let mut tempfile = tempfile();
    tempfile.write_all(HELLO).unwrap();

    let file = File::open(tempfile.path()).await.unwrap();

    // Poll the future once, then cancel it
    poll_once(async { read_hello(&file).await }).await;

    read_hello(&file).await;
}

#[cfg(unix)]
#[compio_macros::test]
async fn timeout_read() {
    use std::time::Duration;

    use compio_fs::pipe::anonymous;
    use compio_io::AsyncReadExt;
    use compio_runtime::time::timeout;

    let (mut rx, _) = anonymous().await.unwrap();

    // Read a file with timeout.
    let _ = timeout(Duration::from_nanos(1), async move {
        rx.read_to_string(String::new()).await
    })
    .await
    .unwrap_err();
}

#[cfg(target_os = "linux")]
#[compio_macros::test]
async fn async_fd_file_position() {
    use compio_driver::DriverType;
    use compio_io::{AsyncWrite, AsyncWriteExt};
    use compio_runtime::{Runtime, fd::AsyncFd};

    if Runtime::with_current(|r| r.driver_type()) != DriverType::IoUring {
        return;
    }

    let mut input = tempfile();
    input.write_all(b"abcdefgh").unwrap();

    let mut fd = AsyncFd::new(std::fs::File::open(input.path()).unwrap()).unwrap();
    let ((), first) = fd.read_exact(Vec::with_capacity(2)).await.unwrap();
    let ((), second) = fd.read_exact(Vec::with_capacity(2)).await.unwrap();
    assert_eq!(first, b"ab");
    assert_eq!(second, b"cd");

    let output = tempfile();
    let mut fd = AsyncFd::new(std::fs::File::create(output.path()).unwrap()).unwrap();
    fd.write_all("first line\n").await.unwrap();
    fd.write_all("second\n").await.unwrap();
    fd.write_vectored(["third", "\n"]).await.unwrap();
    assert_eq!(
        std::fs::read(output.path()).unwrap(),
        b"first line\nsecond\nthird\n"
    );
}

#[cfg(unix)]
#[compio_macros::test]
async fn read_vectored_at_into_capacity() {
    use compio_io::AsyncReadAt;

    let mut input = tempfile();
    input.write_all(b"abcdefgh").unwrap();

    let file = File::open(input.path()).await.unwrap();
    let (n, bufs) = file
        .read_vectored_at([Vec::with_capacity(2), Vec::with_capacity(2)], 2)
        .await
        .unwrap();
    assert_eq!(n, 4);
    assert_eq!(bufs, [b"cd", b"ef"]);
}

#[cfg(unix)]
#[compio_macros::test]
async fn pipe_read_vectored_into_capacity() {
    use compio_fs::pipe::anonymous;
    use compio_io::{AsyncRead, AsyncWriteExt};

    let (mut rx, mut tx) = anonymous().await.unwrap();
    tx.write_all("abcd").await.unwrap();

    let (n, bufs) = rx
        .read_vectored([Vec::with_capacity(2), Vec::with_capacity(2)])
        .await
        .unwrap();
    assert_eq!(n, 4);
    assert_eq!(bufs, [b"ab", b"cd"]);
}

#[compio_macros::test]
async fn drop_open() {
    let tempfile = tempfile();
    let _ = File::create(tempfile.path()).await;

    // Do something else
    let mut file = File::create(tempfile.path()).await.unwrap();

    file.write_all_at(HELLO, 0).await.0.unwrap();

    let file = std::fs::read(tempfile.path()).unwrap();
    assert_eq!(file, HELLO);
}

#[cfg(windows)]
#[compio_macros::test]
async fn hidden_file_truncation() {
    let tmpdir = tempfile::tempdir().unwrap();
    let path = tmpdir.path().join("hidden_file.txt");

    // Create a hidden file.
    const FILE_ATTRIBUTE_HIDDEN: u32 = 2;
    let mut file = compio_fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .attributes(FILE_ATTRIBUTE_HIDDEN)
        .open(&path)
        .await
        .unwrap();
    file.write_all_at("hidden world!", 0).await.unwrap();
    file.close().await.unwrap();

    // Create a new file by truncating the existing one.
    let file = File::create(&path).await.unwrap();
    let metadata = file.metadata().await.unwrap();
    assert_eq!(metadata.len(), 0);
}

#[cfg(windows)]
#[compio_macros::test]
async fn file_locks_conflict_across_independent_handles() {
    let tempfile = tempfile();
    let first = OpenOptions::new()
        .read(true)
        .write(true)
        .open(tempfile.path())
        .await
        .unwrap();
    let second = OpenOptions::new()
        .read(true)
        .write(true)
        .open(tempfile.path())
        .await
        .unwrap();
    let third = OpenOptions::new()
        .read(true)
        .write(true)
        .open(tempfile.path())
        .await
        .unwrap();

    first.try_lock_shared(12, 8).await.unwrap();
    second.try_lock_shared(12, 8).await.unwrap();
    let error = third.try_lock_exclusive(12, 8).await.unwrap_err();
    assert_eq!(
        error.raw_os_error(),
        Some(windows_sys::Win32::Foundation::ERROR_LOCK_VIOLATION as i32)
    );
}

#[cfg(windows)]
#[compio_macros::test]
async fn file_clones_share_lock_state_and_failed_acquire_keeps_it() {
    let tempfile = tempfile();
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(tempfile.path())
        .await
        .unwrap();
    let clone = file.clone();

    file.try_lock_exclusive(20, 4).await.unwrap();
    clone.try_lock_shared(20, 4).await.unwrap();
    let error = clone.try_lock_exclusive(20, 4).await.unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::WouldBlock);
    assert_eq!(error.raw_os_error(), None);
    let error = file.try_lock_exclusive(20, 4).await.unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::WouldBlock);
    assert_eq!(error.raw_os_error(), None);
}

#[cfg(windows)]
#[compio_macros::test]
async fn unlock_releases_exclusive_before_shared_acquisitions() {
    let tempfile = tempfile();
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(tempfile.path())
        .await
        .unwrap();
    let contender = OpenOptions::new()
        .read(true)
        .write(true)
        .open(tempfile.path())
        .await
        .unwrap();

    file.try_lock_exclusive(96, 4).await.unwrap();
    file.try_lock_shared(96, 4).await.unwrap();
    file.unlock(96, 4).await.unwrap();
    assert!(contender.try_lock_exclusive(96, 4).await.is_err());
    contender.try_lock_shared(96, 4).await.unwrap();

    file.unlock(96, 4).await.unwrap();
    contender.unlock(96, 4).await.unwrap();
    contender.try_lock_exclusive(96, 4).await.unwrap();
}

#[cfg(windows)]
#[compio_macros::test]
async fn repeated_shared_unlocks_preserve_remaining_count_and_other_ranges() {
    let tempfile = tempfile();
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(tempfile.path())
        .await
        .unwrap();
    let contender = OpenOptions::new()
        .read(true)
        .write(true)
        .open(tempfile.path())
        .await
        .unwrap();

    file.try_lock_shared(104, 4).await.unwrap();
    file.try_lock_shared(104, 4).await.unwrap();
    file.try_lock_exclusive(120, 4).await.unwrap();
    file.unlock(104, 4).await.unwrap();
    assert!(contender.try_lock_exclusive(104, 4).await.is_err());
    assert!(contender.try_lock_shared(120, 4).await.is_err());

    file.unlock(104, 4).await.unwrap();
    contender.try_lock_exclusive(104, 4).await.unwrap();
    assert!(contender.try_lock_shared(120, 4).await.is_err());
}

#[cfg(windows)]
#[compio_macros::test]
async fn unlock_requires_an_exact_held_range_and_preserves_native_errors() {
    use compio_driver::{
        ToSharedFd,
        op::{TryLockFile, UnlockFile},
    };
    use compio_runtime::Runtime;
    use windows_sys::Win32::Foundation::{ERROR_LOCK_VIOLATION, ERROR_NOT_LOCKED};

    let tempfile = tempfile();
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(tempfile.path())
        .await
        .unwrap();
    let contender = OpenOptions::new()
        .read(true)
        .write(true)
        .open(tempfile.path())
        .await
        .unwrap();
    file.try_lock_exclusive(136, 4).await.unwrap();

    let error = file.unlock(136, 3).await.unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::WouldBlock);
    let error = file.unlock(140, 4).await.unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::WouldBlock);
    let error = file.unlock(137, 3).await.unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::WouldBlock);
    let error = contender.try_lock_exclusive(136, 4).await.unwrap_err();
    assert_eq!(error.raw_os_error(), Some(ERROR_LOCK_VIOLATION as i32));

    // Clear the native range through the retained driver operation while
    // leaving File's completion-backed ownership record unchanged.
    let native_unlock = Runtime::with_current(|runtime| {
        runtime.submit(UnlockFile::new(file.to_shared_fd(), 136, 4))
    });
    native_unlock.await.0.unwrap();

    let error = file.unlock(136, 4).await.unwrap_err();
    assert_eq!(error.raw_os_error(), Some(ERROR_NOT_LOCKED as i32));
    let error = file.unlock(136, 4).await.unwrap_err();
    assert_eq!(error.raw_os_error(), Some(ERROR_NOT_LOCKED as i32));
    let error = file.try_lock_exclusive(136, 4).await.unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::WouldBlock);
    assert_eq!(error.raw_os_error(), None);

    // Reacquire natively through the driver, then let public unlock reconcile
    // the still-held map record from a real successful completion.
    let native_lock = Runtime::with_current(|runtime| {
        runtime.submit(TryLockFile::exclusive(file.to_shared_fd(), 136, 4))
    });
    native_lock.await.0.unwrap();
    let error = contender.try_lock_exclusive(136, 4).await.unwrap_err();
    assert_eq!(error.raw_os_error(), Some(ERROR_LOCK_VIOLATION as i32));
    file.unlock(136, 4).await.unwrap();
    contender.try_lock_exclusive(136, 4).await.unwrap();
}

#[cfg(windows)]
#[compio_macros::test]
async fn dropping_unpolled_unlock_future_keeps_native_and_recorded_lock() {
    let tempfile = tempfile();
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(tempfile.path())
        .await
        .unwrap();
    let contender = OpenOptions::new()
        .read(true)
        .write(true)
        .open(tempfile.path())
        .await
        .unwrap();
    file.try_lock_exclusive(152, 4).await.unwrap();

    let unlock = file.unlock(152, 4);
    drop(unlock);

    let error = file.try_lock_exclusive(152, 4).await.unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::WouldBlock);
    assert_eq!(error.raw_os_error(), None);
    let error = contender.try_lock_exclusive(152, 4).await.unwrap_err();
    assert_eq!(
        error.raw_os_error(),
        Some(windows_sys::Win32::Foundation::ERROR_LOCK_VIOLATION as i32)
    );

    file.unlock(152, 4).await.unwrap();
    contender.try_lock_exclusive(152, 4).await.unwrap();
}

#[cfg(windows)]
#[compio_macros::test]
async fn native_acquire_failure_can_retry_without_losing_sibling_lock() {
    use windows_sys::Win32::Foundation::ERROR_LOCK_VIOLATION;

    let tempfile = tempfile();
    let target = OpenOptions::new()
        .read(true)
        .write(true)
        .open(tempfile.path())
        .await
        .unwrap();
    let foreign_first = OpenOptions::new()
        .read(true)
        .write(true)
        .open(tempfile.path())
        .await
        .unwrap();
    let foreign_second = OpenOptions::new()
        .read(true)
        .write(true)
        .open(tempfile.path())
        .await
        .unwrap();
    let fresh = OpenOptions::new()
        .read(true)
        .write(true)
        .open(tempfile.path())
        .await
        .unwrap();

    target.try_lock_exclusive(72, 4).await.unwrap();
    foreign_first.try_lock_shared(88, 4).await.unwrap();
    foreign_second.try_lock_shared(88, 4).await.unwrap();

    let error = target.try_lock_exclusive(88, 4).await.unwrap_err();
    assert_eq!(error.raw_os_error(), Some(ERROR_LOCK_VIOLATION as i32));

    drop(foreign_second);
    drop(foreign_first);
    target.try_lock_exclusive(88, 4).await.unwrap();

    let error = target.try_lock_exclusive(72, 4).await.unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::WouldBlock);
    assert_eq!(error.raw_os_error(), None);
    let error = fresh.try_lock_shared(72, 4).await.unwrap_err();
    assert_eq!(error.raw_os_error(), Some(ERROR_LOCK_VIOLATION as i32));
}

#[cfg(windows)]
#[compio_macros::test]
async fn dropping_unpolled_lock_future_does_not_reserve_range() {
    let tempfile = tempfile();
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(tempfile.path())
        .await
        .unwrap();

    drop(file.try_lock_exclusive(32, 4));
    file.try_lock_exclusive(32, 4).await.unwrap();
}

#[cfg(windows)]
#[compio_macros::test]
async fn completed_lock_future_drop_keeps_native_lock_held() {
    let tempfile = tempfile();
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(tempfile.path())
        .await
        .unwrap();
    let contender = OpenOptions::new()
        .read(true)
        .write(true)
        .open(tempfile.path())
        .await
        .unwrap();

    use std::{
        future::{Future, poll_fn},
        task::Poll,
    };

    let mut acquisition = Box::pin(file.try_lock_exclusive(40, 4));
    poll_fn(|cx| match acquisition.as_mut().poll(cx) {
        Poll::Ready(result) => {
            assert!(result.is_ok());
            Poll::Ready(())
        }
        Poll::Pending => Poll::Pending,
    })
    .await;
    drop(acquisition);

    assert!(contender.try_lock_shared(40, 4).await.is_err());
}

#[cfg(windows)]
#[compio_macros::test]
async fn invalid_lock_ranges_are_rejected_without_reserving_state() {
    let tempfile = tempfile();
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(tempfile.path())
        .await
        .unwrap();

    assert_eq!(
        file.try_lock_shared(0, 0).await.unwrap_err().kind(),
        std::io::ErrorKind::InvalidInput
    );
    assert_eq!(
        file.try_lock_exclusive(u64::MAX, 1)
            .await
            .unwrap_err()
            .kind(),
        std::io::ErrorKind::InvalidInput
    );
    file.try_lock_exclusive(0, 4).await.unwrap();
}

#[cfg(windows)]
#[compio_macros::test]
async fn file_io_works_while_a_range_lock_is_held() {
    let tempfile = tempfile();
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(tempfile.path())
        .await
        .unwrap();
    file.try_lock_exclusive(0, HELLO.len() as u64)
        .await
        .unwrap();
    file.write_all_at(HELLO, 0).await.0.unwrap();
    let (length, buffer) = file
        .read_to_end_at(Vec::with_capacity(HELLO.len()), 0)
        .await
        .unwrap();

    assert_eq!(length, HELLO.len());
    assert_eq!(buffer, HELLO);
}

fn tempfile() -> NamedTempFile {
    NamedTempFile::new().unwrap()
}

async fn poll_once(future: impl std::future::Future) {
    use std::{future::poll_fn, pin::pin, task::Poll};

    let mut future = pin!(future);

    poll_fn(|cx| {
        let _ = future.as_mut().poll(cx);
        Poll::Ready(())
    })
    .await;
}
