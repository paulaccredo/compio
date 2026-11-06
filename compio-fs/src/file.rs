#[cfg(windows)]
use std::os::windows::io::{AsHandle, AsRawHandle, FromRawHandle, RawHandle};
use std::{future::Future, io, mem::ManuallyDrop, path::Path};

use compio_buf::{BufResult, IntoInner, IoBuf, IoBufMut};
#[cfg(unix)]
use compio_driver::impl_raw_fd;
#[cfg(unix)]
use compio_driver::op::FileStat;
use compio_driver::{
    BufferRef, ResultTakeBuffer, ToSharedFd,
    op::{BufResultExt, CloseFile, ReadAt, ReadManagedAt, Sync, WriteAt},
};
#[cfg(windows)]
use compio_driver::{
    OpCode, OpType,
    op::{TryLockFile, UnlockFile},
};
use compio_io::{AsyncReadAt, AsyncReadManagedAt, AsyncWriteAt, util::Splittable};
use compio_runtime::{Runtime, fd::AsyncFd};
#[cfg(all(unix, not(solarish)))]
use {
    compio_buf::{IoVectoredBuf, IoVectoredBufMut},
    compio_driver::op::{ReadVectoredAt, WriteVectoredAt},
};

use crate::{Metadata, OpenOptions, Permissions};

/// A reference to an open file on the filesystem.
///
/// An instance of a `File` can be read and/or written depending on what options
/// it was opened with. The `File` type provides **positional** read and write
/// operations. The file does not maintain an internal cursor. The caller is
/// required to specify an offset when issuing an operation.
///
///
/// If you'd like to use methods from [`AsyncRead`](`compio_io::AsyncRead`) or
/// [`AsyncWrite`](`compio_io::AsyncWrite`) traits, you can wrap `File` with
/// [`std::io::Cursor`].
///
/// # Examples
/// ```no_run
/// // Most users depend on the `compio` crate rather than on the sub-crates
/// // directly. In that case, import these as `compio::fs::File`,
/// // `compio::buf::BufResult` and `compio::io::AsyncReadExt` instead.
/// use std::io::Cursor;
///
/// use compio_buf::BufResult;
/// use compio_fs::File;
/// use compio_io::AsyncReadExt;
///
/// # compio_runtime::Runtime::new().unwrap().block_on(async {
/// let file = File::open("foo.txt").await?;
/// let mut cursor = Cursor::new(file);
///
/// let int = cursor.read_u32().await?;
/// let float = cursor.read_f32().await?;
///
/// let string = String::new();
/// let BufResult(result, string) = cursor.read_to_string(string).await;
///
/// let buf = vec![0; 1024];
/// let BufResult(result, buf) = cursor.read_exact(buf).await;
/// # std::io::Result::Ok(()) });
/// ```
#[derive(Debug, Clone)]
pub struct File {
    pub(crate) inner: AsyncFd<std::fs::File>,
    #[cfg(windows)]
    locks: crate::locks::SharedLockMap,
}

impl File {
    pub(crate) fn from_std(file: std::fs::File) -> io::Result<Self> {
        Ok(Self {
            inner: AsyncFd::new(file)?,
            #[cfg(windows)]
            locks: crate::locks::SharedLockMap::default(),
        })
    }

    /// Attempts to acquire a shared lock on a byte range without waiting.
    ///
    /// The range must have nonzero length and its end must fit in `u64`.
    /// Dropping this future before its first poll has no effect. After
    /// submission, cancellation does not imply that the operating system
    /// rejected the lock; a successful completion remains recorded.
    /// Exact shared ranges may nest, including after an exclusive acquisition
    /// on the same file owner. Other overlapping ranges fail immediately.
    ///
    /// # Errors
    ///
    /// Returns [`io::ErrorKind::InvalidInput`] for an empty or overflowing
    /// range, and preserves operating-system errors including lock conflicts.
    #[cfg(windows)]
    pub async fn try_lock_shared(&self, offset: u64, length: u64) -> io::Result<()> {
        self.try_lock(offset, length, false).await
    }

    /// Attempts to acquire an exclusive lock on a byte range without waiting.
    ///
    /// The range must have nonzero length and its end must fit in `u64`.
    /// Dropping this future before its first poll has no effect. After
    /// submission, cancellation does not imply that the operating system
    /// rejected the lock; a successful completion remains recorded.
    /// Any overlapping range already held through this file owner fails
    /// immediately.
    ///
    /// # Errors
    ///
    /// Returns [`io::ErrorKind::InvalidInput`] for an empty or overflowing
    /// range, and preserves operating-system errors including lock conflicts.
    #[cfg(windows)]
    pub async fn try_lock_exclusive(&self, offset: u64, length: u64) -> io::Result<()> {
        self.try_lock(offset, length, true).await
    }

    #[cfg(windows)]
    async fn try_lock(&self, offset: u64, length: u64, exclusive: bool) -> io::Result<()> {
        if length == 0 || offset.checked_add(length).is_none() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "byte range must be nonempty and fit in u64",
            ));
        }
        let submit = Runtime::with_current(|runtime| {
            self.locks.ensure_cleanup_file(|| {
                self.as_handle()
                    .try_clone_to_owned()
                    .map(std::fs::File::from)
            })?;
            let Some(reservation) = self.locks.try_reserve(offset, length, exclusive) else {
                return Err(io::Error::new(
                    io::ErrorKind::WouldBlock,
                    "byte range conflicts with a lock held through this File",
                ));
            };
            let fd = self.to_shared_fd();
            // Keep the reservation in the submitted operation: dropping the
            // public future only requests cancellation, so only completion can
            // establish whether the native lock was acquired.
            let op = TryLockFileOp {
                inner: if exclusive {
                    TryLockFile::exclusive(fd, offset, length)
                } else {
                    TryLockFile::shared(fd, offset, length)
                },
                reservation: Some(reservation),
            };
            Ok(runtime.submit(op))
        })?;
        submit.await.0.map(|_| ())
    }

    /// Releases one acquisition of an exact byte range held through this file
    /// owner.
    ///
    /// Shared acquisitions are released one at a time. An exclusive acquisition
    /// is released before any shared acquisitions of the same range. If the
    /// future is dropped after submission, the held state is updated only when
    /// the driver reports successful completion.
    ///
    /// # Errors
    ///
    /// Returns [`io::ErrorKind::InvalidInput`] for an empty or overflowing
    /// range, [`io::ErrorKind::WouldBlock`] if the exact range is not currently
    /// releasable through this file owner, and preserves operating-system
    /// errors.
    #[cfg(windows)]
    pub async fn unlock(&self, offset: u64, length: u64) -> io::Result<()> {
        if length == 0 || offset.checked_add(length).is_none() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "byte range must be nonempty and fit in u64",
            ));
        }
        let submit = Runtime::with_current(|runtime| {
            let Some(reservation) = self.locks.try_reserve_release(offset, length) else {
                return Err(io::Error::new(
                    io::ErrorKind::WouldBlock,
                    "byte range is not releasable through this File",
                ));
            };
            let op = UnlockFileOp {
                inner: UnlockFile::new(self.to_shared_fd(), offset, length),
                reservation: Some(reservation),
            };
            Ok(runtime.submit(op))
        })?;
        submit.await.0.map(|_| ())
    }

    /// Attempts to open a file in read-only mode.
    ///
    /// See the [`OpenOptions::open`] method for more details.
    pub async fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        OpenOptions::new().read(true).open(path).await
    }

    /// Opens a file in write-only mode.
    ///
    /// This function will create a file if it does not exist,
    /// and will truncate it if it does.
    ///
    /// See the [`OpenOptions::open`] function for more details.
    pub async fn create(path: impl AsRef<Path>) -> io::Result<Self> {
        OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(path)
            .await
    }

    /// Close the file. If the returned future is dropped before polling, the
    /// file won't be closed.
    ///
    /// As [`File`] is clonable, users can call `close` on a clone, but the
    /// future will never complete until all clones are dropped. Some
    /// operations may keep a strong reference to the file, so the future
    /// may never complete if there are pending operations.
    ///
    /// It's OK to drop the [`File`] directly without calling `close`, but the
    /// file may not be closed immediately.
    pub fn close(self) -> impl Future<Output = io::Result<()>> {
        // Make sure that fd won't be dropped after `close` called.
        // Users may call this method and drop the future immediately. In that
        // way `close` should be cancelled.
        let this = ManuallyDrop::new(self);
        async move {
            let fd = ManuallyDrop::into_inner(this)
                .inner
                .into_inner()
                .take()
                .await;
            if let Some(fd) = fd {
                let op = CloseFile::new(fd.into());
                compio_runtime::submit(op).await.0?;
            }
            Ok(())
        }
    }

    /// Queries metadata about the underlying file.
    #[cfg(windows)]
    pub async fn metadata(&self) -> io::Result<Metadata> {
        crate::spawn_blocking_with(self.to_shared_fd(), |file| {
            file.metadata().map(Metadata::from_std)
        })
        .await
    }

    #[cfg(windows)]
    /// Truncates or extends the underlying file, updating the size of this file
    /// to become `size`.
    pub async fn set_len(&self, size: u64) -> io::Result<()> {
        crate::spawn_blocking_with(self.to_shared_fd(), move |file| file.set_len(size)).await
    }

    #[cfg(unix)]
    /// Truncates or extends the underlying file, updating the size of this file
    /// to become `size`.
    ///
    /// NOTE: On Linux kernel <= 6.9 or when io uring is disabled, the operation
    /// will be offloaded to the separate blocking thread
    pub async fn set_len(&self, size: u64) -> io::Result<()> {
        use compio_driver::op::TruncateFile;

        let op = TruncateFile::new(self.to_shared_fd(), size);
        compio_runtime::submit(op).await.0.map(|_| ())
    }

    /// Queries metadata about the underlying file.
    #[cfg(unix)]
    pub async fn metadata(&self) -> io::Result<Metadata> {
        let op = FileStat::new(self.to_shared_fd());
        let BufResult(res, op) = compio_runtime::submit(op).await;
        res.map(|_| Metadata::from_attr(op.into_inner()))
    }

    /// Reads an extended attribute from this open file, with `fgetxattr`
    /// semantics.
    ///
    /// The file descriptor identifies the object; no pathname is resolved
    /// again. In particular, this does not read attributes from a symbolic
    /// link itself.
    ///
    /// The value is written from the start of `buffer`, using its full
    /// capacity. On success, the result is the value's length and the
    /// buffer's initialized length advances to at least that length. With
    /// zero capacity, only the required length is returned and the buffer
    /// remains empty. The attribute may change between a sizing query and a
    /// subsequent read.
    ///
    /// A missing attribute returns `ENODATA`; insufficient nonzero capacity
    /// returns `ERANGE`. Other OS errors are preserved. A name containing a NUL
    /// byte returns [`io::ErrorKind::InvalidInput`]. Errors return the original
    /// buffer without advancing its initialized length.
    ///
    /// The submitted operation owns the name and buffer and holds a reference
    /// to the file descriptor until completion, even if this future is
    /// dropped. Cancellation does not return the buffer to the caller.
    #[cfg(any(target_os = "linux", target_os = "android"))]
    pub async fn get_xattr<T: IoBufMut>(
        &self,
        name: impl AsRef<std::ffi::OsStr>,
        buffer: T,
    ) -> BufResult<usize, T> {
        use std::{ffi::CString, os::unix::ffi::OsStrExt};

        use compio_buf::{IoBufMutExt, buf_try};
        use compio_driver::op::FGetXattr;

        let (name, mut buffer) = buf_try!(
            CString::new(name.as_ref().as_bytes()).map_err(io::Error::from),
            buffer
        );
        let query_size = buffer.buf_capacity() == 0;
        let op = FGetXattr::new(self.to_shared_fd(), name, buffer);
        let res = compio_runtime::submit(op).await.into_inner();
        if query_size {
            res
        } else {
            // SAFETY: A successful nonzero-capacity fgetxattr initializes
            // exactly the returned byte count, bounded by the
            // supplied buffer capacity. Size-only queries are
            // excluded because they do not write bytes.
            unsafe { res.map_advanced() }
        }
    }

    /// Changes the permissions on the underlying file.
    #[cfg(windows)]
    pub async fn set_permissions(&self, perm: Permissions) -> io::Result<()> {
        crate::spawn_blocking_with(self.to_shared_fd(), move |file| {
            if let Some(p) = perm.0.original {
                file.set_permissions(p)
            } else {
                let mut p = file.metadata()?.permissions();
                p.set_readonly(perm.readonly());
                file.set_permissions(p)
            }
        })
        .await
    }

    /// Changes the permissions on the underlying file.
    #[cfg(unix)]
    pub async fn set_permissions(&self, perm: Permissions) -> io::Result<()> {
        crate::spawn_blocking_with(self.to_shared_fd(), |file| file.set_permissions(perm.0)).await
    }

    async fn sync_impl(&self, datasync: bool) -> io::Result<()> {
        let op = Sync::new(self.to_shared_fd(), datasync);
        compio_runtime::submit(op).await.0?;
        Ok(())
    }

    /// Attempts to sync all OS-internal metadata to disk.
    ///
    /// This function will attempt to ensure that all in-memory data reaches the
    /// filesystem before returning.
    pub async fn sync_all(&self) -> io::Result<()> {
        self.sync_impl(false).await
    }

    /// This function is similar to [`sync_all`], except that it might not
    /// synchronize file metadata to the filesystem.
    ///
    /// This is intended for use cases that must synchronize content, but don't
    /// need the metadata on disk. The goal of this method is to reduce disk
    /// operations.
    ///
    /// Note that some platforms may simply implement this in terms of
    /// [`sync_all`].
    ///
    /// [`sync_all`]: File::sync_all
    pub async fn sync_data(&self) -> io::Result<()> {
        self.sync_impl(true).await
    }
}

impl AsyncReadAt for File {
    async fn read_at<T: IoBufMut>(&self, buffer: T, pos: u64) -> BufResult<usize, T> {
        let fd = self.inner.to_shared_fd();
        let op = ReadAt::new(fd, pos, buffer);
        let res = compio_runtime::submit(op).await.into_inner();
        unsafe { res.map_advanced() }
    }

    #[cfg(all(unix, not(solarish)))]
    async fn read_vectored_at<T: IoVectoredBufMut>(
        &self,
        buffer: T,
        pos: u64,
    ) -> BufResult<usize, T> {
        use compio_driver::op::VecBufResultExt;

        let fd = self.inner.to_shared_fd();
        let op = ReadVectoredAt::new(fd, pos, buffer);
        let res = compio_runtime::submit(op).await.into_inner();
        unsafe { res.map_vec_advanced() }
    }
}

impl AsyncReadManagedAt for File {
    type Buffer = BufferRef;

    async fn read_managed_at(&self, len: usize, pos: u64) -> io::Result<Option<Self::Buffer>> {
        let fd = self.inner.to_shared_fd();
        let res = Runtime::with_current(|rt| {
            let buffer_pool = rt.buffer_pool()?;
            let op = ReadManagedAt::new(fd, pos, &buffer_pool, len)?;
            io::Result::Ok(rt.submit(op))
        })?
        .await;
        unsafe { res.take_buffer() }
    }
}

impl AsyncWriteAt for File {
    #[inline]
    async fn write_at<T: IoBuf>(&mut self, buf: T, pos: u64) -> BufResult<usize, T> {
        (&*self).write_at(buf, pos).await
    }

    #[cfg(all(unix, not(solarish)))]
    #[inline]
    async fn write_vectored_at<T: IoVectoredBuf>(
        &mut self,
        buf: T,
        pos: u64,
    ) -> BufResult<usize, T> {
        (&*self).write_vectored_at(buf, pos).await
    }
}

impl AsyncWriteAt for &File {
    async fn write_at<T: IoBuf>(&mut self, buffer: T, pos: u64) -> BufResult<usize, T> {
        let fd = self.inner.to_shared_fd();
        let op = WriteAt::new(fd, pos, buffer);
        compio_runtime::submit(op).await.into_inner()
    }

    #[cfg(all(unix, not(solarish)))]
    async fn write_vectored_at<T: IoVectoredBuf>(
        &mut self,
        buffer: T,
        pos: u64,
    ) -> BufResult<usize, T> {
        let fd = self.inner.to_shared_fd();
        let op = WriteVectoredAt::new(fd, pos, buffer);
        compio_runtime::submit(op).await.into_inner()
    }
}

impl Splittable for File {
    type ReadHalf = File;
    type WriteHalf = File;

    fn split(self) -> (Self::ReadHalf, Self::WriteHalf) {
        (self.clone(), self)
    }
}

impl Splittable for &File {
    type ReadHalf = File;
    type WriteHalf = File;

    fn split(self) -> (Self::ReadHalf, Self::WriteHalf) {
        (self.clone(), self.clone())
    }
}

impl Splittable for &mut File {
    type ReadHalf = File;
    type WriteHalf = File;

    fn split(self) -> (Self::ReadHalf, Self::WriteHalf) {
        (self.clone(), self.clone())
    }
}

#[cfg(unix)]
impl_raw_fd!(File, std::fs::File, inner, file);

#[cfg(windows)]
// SAFETY: this wrapper retains and delegates the same driver operation for
// the runtime-owned operation lifetime. It forwards the runtime control,
// OVERLAPPED storage, cancellation, and completion result unchanged.
unsafe impl OpCode for TryLockFileOp {
    type Control = ();

    fn op_type(&self, control: &Self::Control) -> OpType {
        self.inner.op_type(control)
    }

    unsafe fn operate(
        &mut self,
        control: &mut Self::Control,
        overlapped: *mut windows_sys::Win32::System::IO::OVERLAPPED,
    ) -> std::task::Poll<io::Result<usize>> {
        // SAFETY: the wrapped driver operation owns the same fd and relies on
        // the IOCP operation storage retained by the runtime.
        unsafe { self.inner.operate(control, overlapped) }
    }

    fn cancel(
        &mut self,
        control: &mut Self::Control,
        overlapped: *mut windows_sys::Win32::System::IO::OVERLAPPED,
    ) -> io::Result<()> {
        self.inner.cancel(control, overlapped)
    }

    unsafe fn set_result(
        &mut self,
        control: &mut Self::Control,
        result: &io::Result<usize>,
        extra: &compio_driver::Extra,
    ) {
        // SAFETY: the result and extra are delivered by the runtime for this
        // wrapped operation, matching the driver's OpCode contract.
        unsafe { self.inner.set_result(control, result, extra) };
        if let Some(mut reservation) = self.reservation.take() {
            reservation.complete(result.is_ok());
        }
    }
}

#[cfg(windows)]
// SAFETY: the wrapper retains and delegates the same driver operation for
// the runtime-owned operation lifetime. It forwards the runtime control,
// OVERLAPPED storage, cancellation, and completion result unchanged.
unsafe impl OpCode for UnlockFileOp {
    type Control = ();

    fn op_type(&self, control: &Self::Control) -> OpType {
        self.inner.op_type(control)
    }

    unsafe fn operate(
        &mut self,
        control: &mut Self::Control,
        overlapped: *mut windows_sys::Win32::System::IO::OVERLAPPED,
    ) -> std::task::Poll<io::Result<usize>> {
        // SAFETY: the wrapped driver operation owns the same fd and relies on
        // the IOCP operation storage retained by the runtime.
        unsafe { self.inner.operate(control, overlapped) }
    }

    fn cancel(
        &mut self,
        control: &mut Self::Control,
        overlapped: *mut windows_sys::Win32::System::IO::OVERLAPPED,
    ) -> io::Result<()> {
        self.inner.cancel(control, overlapped)
    }

    unsafe fn set_result(
        &mut self,
        control: &mut Self::Control,
        result: &io::Result<usize>,
        extra: &compio_driver::Extra,
    ) {
        // SAFETY: the result and extra are delivered by the runtime for this
        // wrapped operation, matching the driver's OpCode contract.
        unsafe { self.inner.set_result(control, result, extra) };
        if let Some(mut reservation) = self.reservation.take() {
            reservation.complete(result.is_ok());
        }
    }
}

#[cfg(windows)]
struct UnlockFileOp {
    inner: UnlockFile<compio_driver::SharedFd<std::fs::File>>,
    reservation: Option<crate::locks::LockReleaseReservation>,
}

#[cfg(windows)]
struct TryLockFileOp {
    inner: TryLockFile<compio_driver::SharedFd<std::fs::File>>,
    reservation: Option<crate::locks::LockReservation>,
}

#[cfg(windows)]
impl compio_driver::AsRawFd for File {
    fn as_raw_fd(&self) -> compio_driver::RawFd {
        self.inner.as_raw_fd()
    }
}

#[cfg(windows)]
impl compio_driver::ToSharedFd<std::fs::File> for File {
    fn to_shared_fd(&self) -> compio_driver::SharedFd<std::fs::File> {
        self.inner.to_shared_fd()
    }
}

#[cfg(windows)]
impl FromRawHandle for File {
    unsafe fn from_raw_handle(handle: RawHandle) -> Self {
        Self {
            // SAFETY: the caller guarantees ownership of this raw handle; the
            // AsyncFd conversion preserves that contract.
            inner: unsafe { AsyncFd::from_raw_handle(handle) },
            locks: crate::locks::SharedLockMap::default(),
        }
    }
}

#[cfg(windows)]
impl AsHandle for File {
    fn as_handle(&self) -> std::os::windows::io::BorrowedHandle<'_> {
        self.inner.as_handle()
    }
}

#[cfg(windows)]
impl AsRawHandle for File {
    fn as_raw_handle(&self) -> RawHandle {
        self.inner.as_raw_handle()
    }
}

#[cfg(all(windows, test))]
mod tests {
    use std::{
        cell::Cell,
        future::Future,
        rc::Rc,
        task::Context,
        time::{Duration, Instant},
    };

    use compio_driver::AsRawFd;

    use super::*;

    async fn acquire_exclusive_after_cleanup(file: &File, offset: u64, length: u64) {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            match file.try_lock_exclusive(offset, length).await {
                Ok(()) => return,
                Err(error)
                    if error.raw_os_error()
                        == Some(windows_sys::Win32::Foundation::ERROR_LOCK_VIOLATION as i32) =>
                {
                    assert!(
                        std::time::Instant::now() < deadline,
                        "cleanup did not release the range"
                    );
                    std::thread::yield_now();
                }
                Err(error) => panic!("unexpected error while observing cleanup: {error:?}"),
            }
        }
    }

    #[test]
    fn reservation_keeps_cleanup_owner_alive_across_runtime_teardown() {
        let tempfile = tempfile::NamedTempFile::new().unwrap();
        let path = tempfile.path().to_path_buf();
        let runtime = Runtime::new().unwrap();
        let (file, mut reservation, keepalive) = runtime.block_on(async {
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .open(&path)
                .await
                .unwrap();
            file.try_lock_shared(80, 4).await.unwrap();
            let reservation = file
                .locks
                .try_reserve(80, 4, false)
                .expect("controlled reservation should share the held range");
            runtime
                .submit(TryLockFile::shared(file.to_shared_fd(), 80, 4))
                .await
                .0
                .unwrap();
            let keepalive = file.to_shared_fd();
            (file, reservation, keepalive)
        });

        drop(file);
        drop(runtime);

        let runtime = Runtime::new().unwrap();
        let contender = runtime.block_on(async {
            let contender = OpenOptions::new()
                .read(true)
                .write(true)
                .open(&path)
                .await
                .unwrap();
            let error = contender.try_lock_exclusive(80, 4).await.unwrap_err();
            assert_eq!(
                error.raw_os_error(),
                Some(windows_sys::Win32::Foundation::ERROR_LOCK_VIOLATION as i32)
            );
            contender
        });

        assert!(reservation.complete(true));
        drop(reservation);
        runtime.block_on(async {
            acquire_exclusive_after_cleanup(&contender, 80, 4).await;
            contender.unlock(80, 4).await.unwrap();
        });
        drop(keepalive);
    }

    #[compio_macros::test]
    async fn completion_callback_records_lock_before_operation_drop() {
        let tempfile = tempfile::NamedTempFile::new().unwrap();
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
        let reservation = file.locks.try_reserve(48, 4, true).unwrap();
        let op = TryLockFileOp {
            inner: TryLockFile::exclusive(file.to_shared_fd(), 48, 4),
            reservation: Some(reservation),
        };

        let BufResult(result, op) = Runtime::with_current(|runtime| runtime.submit(op)).await;
        result.unwrap();

        assert!(op.reservation.is_none());
        assert_eq!(file.locks.count_at(52), Some(1));
        assert!(contender.try_lock_shared(48, 4).await.is_err());
    }

    #[compio_macros::test]
    async fn discarded_submit_still_records_queued_successful_lock() {
        let tempfile = tempfile::NamedTempFile::new().unwrap();
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
        let reservation = file.locks.try_reserve(56, 4, true).unwrap();
        let queued = Rc::new(Cell::new(false));
        let completed = Rc::new(Cell::new(false));
        let op = ControlledTryLockCompletion {
            inner: TryLockFileOp {
                inner: TryLockFile::exclusive(file.to_shared_fd(), 56, 4),
                reservation: Some(reservation),
            },
            completion_port: Runtime::with_current(|runtime| runtime.as_raw_fd()),
            queued: queued.clone(),
            completed: completed.clone(),
        };
        let mut submit = Box::pin(Runtime::with_current(|runtime| runtime.submit(op)));
        let mut context = Context::from_waker(futures_util::task::noop_waker_ref());

        assert!(submit.as_mut().poll(&mut context).is_pending());
        assert!(
            queued.get(),
            "real lock success must queue the controlled packet"
        );
        assert!(
            !completed.get(),
            "completion must remain queued until the runtime polls"
        );
        drop(submit);
        assert_eq!(file.locks.count_at(60), Some(1));

        let deadline = Instant::now() + Duration::from_secs(5);
        while !completed.get() {
            let now = Instant::now();
            assert!(
                now < deadline,
                "controlled completion packet was not observed"
            );
            let timeout = (deadline - now).min(Duration::from_millis(100));
            Runtime::with_current(|runtime| runtime.poll_with(Some(timeout)));
        }

        assert!(completed.get());
        assert_eq!(file.locks.count_at(60), Some(1));
        let error = contender.try_lock_shared(56, 4).await.unwrap_err();
        assert_eq!(
            error.raw_os_error(),
            Some(windows_sys::Win32::Foundation::ERROR_LOCK_VIOLATION as i32)
        );
    }

    #[compio_macros::test]
    async fn discarded_queued_unlock_keeps_state_until_success_completion() {
        let tempfile = tempfile::NamedTempFile::new().unwrap();
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
        file.try_lock_exclusive(64, 4).await.unwrap();

        let reservation = file.locks.try_reserve_release(64, 4).unwrap();
        let queued = Rc::new(Cell::new(false));
        let completed = Rc::new(Cell::new(false));
        let op = ControlledUnlockCompletion {
            inner: UnlockFileOp {
                inner: UnlockFile::new(file.to_shared_fd(), 64, 4),
                reservation: Some(reservation),
            },
            completion_port: Runtime::with_current(|runtime| runtime.as_raw_fd()),
            queued: queued.clone(),
            completed: completed.clone(),
        };
        let mut submit = Box::pin(Runtime::with_current(|runtime| runtime.submit(op)));
        let mut context = Context::from_waker(futures_util::task::noop_waker_ref());

        assert!(submit.as_mut().poll(&mut context).is_pending());
        assert!(
            queued.get(),
            "real unlock success must queue the controlled packet"
        );
        assert!(!completed.get(), "unlock state must wait for completion");
        assert_eq!(file.locks.count_at(68), Some(1));
        drop(submit);

        let deadline = Instant::now() + Duration::from_secs(5);
        while !completed.get() {
            let now = Instant::now();
            assert!(
                now < deadline,
                "controlled unlock completion was not observed"
            );
            let timeout = (deadline - now).min(Duration::from_millis(100));
            Runtime::with_current(|runtime| runtime.poll_with(Some(timeout)));
        }

        assert!(completed.get());
        assert_eq!(file.locks.count_at(68), None);
        contender.try_lock_exclusive(64, 4).await.unwrap();
    }

    #[compio_macros::test]
    async fn controlled_unlock_callback_error_keeps_state_and_allows_retry() {
        use windows_sys::Win32::Foundation::{ERROR_LOCK_VIOLATION, ERROR_OPERATION_ABORTED};

        let tempfile = tempfile::NamedTempFile::new().unwrap();
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
        file.try_lock_exclusive(72, 4).await.unwrap();

        let reservation = file.locks.try_reserve_release(72, 4).unwrap();
        let callback_completed = Rc::new(Cell::new(false));
        let submit = Runtime::with_current(|runtime| {
            runtime.submit(ControlledUnlockCallbackError {
                inner: UnlockFileOp {
                    inner: UnlockFile::new(file.to_shared_fd(), 72, 4),
                    reservation: Some(reservation),
                },
                error: ERROR_OPERATION_ABORTED as i32,
                callback_completed: callback_completed.clone(),
            })
        });

        // This controlled operation supplies an OS error without submitting
        // native I/O, so it proves callback reconciliation without claiming
        // native Pending or cancellation behavior.
        let error = submit.await.0.unwrap_err();
        assert_eq!(error.raw_os_error(), Some(ERROR_OPERATION_ABORTED as i32));
        assert!(callback_completed.get());
        assert_eq!(file.locks.test_state_at(76), Some((1, true, 0, false)));
        let error = contender.try_lock_exclusive(72, 4).await.unwrap_err();
        assert_eq!(error.raw_os_error(), Some(ERROR_LOCK_VIOLATION as i32));

        file.unlock(72, 4).await.unwrap();
        assert_eq!(file.locks.test_state_at(76), None);
        contender.try_lock_exclusive(72, 4).await.unwrap();
    }

    struct ControlledTryLockCompletion {
        inner: TryLockFileOp,
        completion_port: compio_driver::RawFd,
        queued: Rc<Cell<bool>>,
        completed: Rc<Cell<bool>>,
    }

    // SAFETY: the runtime retains this wrapper and its inner operation until
    // it consumes the queued completion; every operation method forwards the
    // same control, OVERLAPPED storage, and result to the inner operation.
    unsafe impl OpCode for ControlledTryLockCompletion {
        type Control = ();

        fn op_type(&self, control: &Self::Control) -> OpType {
            self.inner.op_type(control)
        }

        unsafe fn operate(
            &mut self,
            control: &mut Self::Control,
            overlapped: *mut windows_sys::Win32::System::IO::OVERLAPPED,
        ) -> std::task::Poll<io::Result<usize>> {
            // SAFETY: the runtime pins this operation and provides its live
            // OVERLAPPED storage to both the real driver call and queued
            // packet.
            match unsafe { self.inner.operate(control, overlapped) } {
                std::task::Poll::Ready(Ok(_)) => {
                    // SAFETY: `overlapped` is live runtime-owned storage and
                    // the accepted driver test uses this controlled packet
                    // pattern only after the real syscall has completed.
                    unsafe {
                        (*overlapped).Internal = 0;
                        (*overlapped).InternalHigh = 0;
                    }
                    // SAFETY: this port belongs to the runtime supplying the
                    // OVERLAPPED pointer, which remains owned until dequeue.
                    let posted = unsafe {
                        windows_sys::Win32::System::IO::PostQueuedCompletionStatus(
                            self.completion_port as _,
                            0,
                            0,
                            overlapped,
                        )
                    };
                    if posted == 0 {
                        std::task::Poll::Ready(Err(io::Error::last_os_error()))
                    } else {
                        self.queued.set(true);
                        std::task::Poll::Pending
                    }
                }
                result => result,
            }
        }

        fn cancel(
            &mut self,
            control: &mut Self::Control,
            overlapped: *mut windows_sys::Win32::System::IO::OVERLAPPED,
        ) -> io::Result<()> {
            self.inner.cancel(control, overlapped)
        }

        unsafe fn set_result(
            &mut self,
            control: &mut Self::Control,
            result: &io::Result<usize>,
            extra: &compio_driver::Extra,
        ) {
            // SAFETY: completion is delivered by the runtime for this wrapper
            // and is forwarded to the same retained driver operation.
            unsafe { self.inner.set_result(control, result, extra) };
            self.completed.set(self.inner.reservation.is_none());
        }
    }

    struct ControlledUnlockCompletion {
        inner: UnlockFileOp,
        completion_port: compio_driver::RawFd,
        queued: Rc<Cell<bool>>,
        completed: Rc<Cell<bool>>,
    }

    struct ControlledUnlockCallbackError {
        inner: UnlockFileOp,
        error: i32,
        callback_completed: Rc<Cell<bool>>,
    }

    // SAFETY: this test wrapper completes immediately and forwards the same
    // operation storage to the callback; it does not submit native I/O.
    unsafe impl OpCode for ControlledUnlockCallbackError {
        type Control = ();

        fn op_type(&self, control: &Self::Control) -> OpType {
            self.inner.op_type(control)
        }

        unsafe fn operate(
            &mut self,
            _control: &mut Self::Control,
            _overlapped: *mut windows_sys::Win32::System::IO::OVERLAPPED,
        ) -> std::task::Poll<io::Result<usize>> {
            std::task::Poll::Ready(Err(io::Error::from_raw_os_error(self.error)))
        }

        fn cancel(
            &mut self,
            control: &mut Self::Control,
            overlapped: *mut windows_sys::Win32::System::IO::OVERLAPPED,
        ) -> io::Result<()> {
            self.inner.cancel(control, overlapped)
        }

        unsafe fn set_result(
            &mut self,
            control: &mut Self::Control,
            result: &io::Result<usize>,
            extra: &compio_driver::Extra,
        ) {
            // SAFETY: the runtime invokes this callback with the same control
            // and Extra that belong to the wrapped operation.
            unsafe { self.inner.set_result(control, result, extra) };
            self.callback_completed
                .set(self.inner.reservation.is_none());
        }
    }

    // SAFETY: the runtime retains this wrapper and its inner operation until
    // it consumes the queued completion; every operation method forwards the
    // same control, OVERLAPPED storage, and result to the inner operation.
    unsafe impl OpCode for ControlledUnlockCompletion {
        type Control = ();

        fn op_type(&self, control: &Self::Control) -> OpType {
            self.inner.op_type(control)
        }

        unsafe fn operate(
            &mut self,
            control: &mut Self::Control,
            overlapped: *mut windows_sys::Win32::System::IO::OVERLAPPED,
        ) -> std::task::Poll<io::Result<usize>> {
            // SAFETY: the runtime pins this operation and provides its live
            // OVERLAPPED storage to both the real driver call and queued
            // packet.
            match unsafe { self.inner.operate(control, overlapped) } {
                std::task::Poll::Ready(Ok(_)) => {
                    // SAFETY: `overlapped` is live runtime-owned storage and
                    // this controlled packet follows the successful syscall.
                    unsafe {
                        (*overlapped).Internal = 0;
                        (*overlapped).InternalHigh = 0;
                    }
                    // SAFETY: this port belongs to the runtime supplying the
                    // OVERLAPPED pointer, which remains owned until dequeue.
                    let posted = unsafe {
                        windows_sys::Win32::System::IO::PostQueuedCompletionStatus(
                            self.completion_port as _,
                            0,
                            0,
                            overlapped,
                        )
                    };
                    if posted == 0 {
                        std::task::Poll::Ready(Err(io::Error::last_os_error()))
                    } else {
                        self.queued.set(true);
                        std::task::Poll::Pending
                    }
                }
                result => result,
            }
        }

        fn cancel(
            &mut self,
            control: &mut Self::Control,
            overlapped: *mut windows_sys::Win32::System::IO::OVERLAPPED,
        ) -> io::Result<()> {
            self.inner.cancel(control, overlapped)
        }

        unsafe fn set_result(
            &mut self,
            control: &mut Self::Control,
            result: &io::Result<usize>,
            extra: &compio_driver::Extra,
        ) {
            // SAFETY: completion is delivered by the runtime for this wrapper
            // and is forwarded to the same retained driver operation.
            unsafe { self.inner.set_result(control, result, extra) };
            self.completed.set(self.inner.reservation.is_none());
        }
    }
}
