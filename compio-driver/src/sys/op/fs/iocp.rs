use windows_sys::Win32::{
    Foundation::{CloseHandle, ERROR_IO_PENDING, GetLastError},
    Storage::FileSystem::{
        FlushFileBuffers, LOCKFILE_EXCLUSIVE_LOCK, LOCKFILE_FAIL_IMMEDIATELY, LockFileEx,
        UnlockFileEx,
    },
    System::IO::OVERLAPPED,
};

use crate::{OpCode, OpType, sys::op::*};

unsafe impl OpCode for CloseFile {
    type Control = ();

    fn op_type(&self, _: &Self::Control) -> OpType {
        OpType::Blocking
    }

    unsafe fn operate(&mut self, _: &mut (), _optr: *mut OVERLAPPED) -> Poll<io::Result<usize>> {
        Poll::Ready(Ok(
            syscall!(BOOL, CloseHandle(self.fd.as_fd().as_raw_fd()))? as _,
        ))
    }
}

// SAFETY: the operation owns `fd` and uses only IOCP-provided OVERLAPPED
// storage, which the driver retains until the operation completes.
unsafe impl<S: AsFd> OpCode for TryLockFile<S> {
    type Control = ();

    unsafe fn operate(&mut self, _: &mut (), optr: *mut OVERLAPPED) -> Poll<io::Result<usize>> {
        // SAFETY: IOCP supplies operation-owned OVERLAPPED storage that remains
        // alive until this operation completes.
        let overlapped = unsafe { &mut *optr };
        overlapped.Anonymous.Anonymous.Offset = self.offset as u32;
        overlapped.Anonymous.Anonymous.OffsetHigh = (self.offset >> 32) as u32;

        let flags = LOCKFILE_FAIL_IMMEDIATELY
            | if self.exclusive {
                LOCKFILE_EXCLUSIVE_LOCK
            } else {
                0
            };
        // SAFETY: `fd` is retained by this operation and `optr` points to its
        // live IOCP OVERLAPPED storage for the duration of this call.
        let result = unsafe {
            LockFileEx(
                self.fd.as_fd().as_raw_fd(),
                flags,
                0,
                self.len as u32,
                (self.len >> 32) as u32,
                optr,
            )
        };
        if result != 0 {
            return Poll::Ready(Ok(0));
        }

        // SAFETY: GetLastError has no pointer or handle preconditions.
        let error = unsafe { GetLastError() };
        if error == ERROR_IO_PENDING {
            Poll::Pending
        } else {
            // Keep the raw lock error; read-oriented win32_result maps some
            // Windows errors to successful end-of-stream results.
            Poll::Ready(Err(io::Error::from_raw_os_error(error as _)))
        }
    }

    fn cancel(&mut self, _: &mut (), optr: *mut OVERLAPPED) -> io::Result<()> {
        cancel(self.fd.as_fd().as_raw_fd(), optr)
    }
}

// SAFETY: the operation owns `fd` and uses only IOCP-provided OVERLAPPED
// storage, which the driver retains until the operation completes.
unsafe impl<S: AsFd> OpCode for UnlockFile<S> {
    type Control = ();

    unsafe fn operate(&mut self, _: &mut (), optr: *mut OVERLAPPED) -> Poll<io::Result<usize>> {
        // SAFETY: IOCP supplies operation-owned OVERLAPPED storage that remains
        // alive until this operation completes.
        let overlapped = unsafe { &mut *optr };
        overlapped.Anonymous.Anonymous.Offset = self.offset as u32;
        overlapped.Anonymous.Anonymous.OffsetHigh = (self.offset >> 32) as u32;

        // SAFETY: `fd` is retained by this operation and `optr` points to its
        // live IOCP OVERLAPPED storage for the duration of this call.
        let result = unsafe {
            UnlockFileEx(
                self.fd.as_fd().as_raw_fd(),
                0,
                self.len as u32,
                (self.len >> 32) as u32,
                optr,
            )
        };
        if result != 0 {
            return Poll::Ready(Ok(0));
        }

        // SAFETY: GetLastError has no pointer or handle preconditions.
        let error = unsafe { GetLastError() };
        if error == ERROR_IO_PENDING {
            Poll::Pending
        } else {
            // Keep the raw lock error; read-oriented win32_result maps some
            // Windows errors to successful end-of-stream results.
            Poll::Ready(Err(io::Error::from_raw_os_error(error as _)))
        }
    }

    fn cancel(&mut self, _: &mut (), optr: *mut OVERLAPPED) -> io::Result<()> {
        cancel(self.fd.as_fd().as_raw_fd(), optr)
    }
}

unsafe impl<S: AsFd> OpCode for Sync<S> {
    type Control = ();

    fn op_type(&self, _: &Self::Control) -> OpType {
        OpType::Blocking
    }

    unsafe fn operate(&mut self, _: &mut (), _optr: *mut OVERLAPPED) -> Poll<io::Result<usize>> {
        Poll::Ready(Ok(
            syscall!(BOOL, FlushFileBuffers(self.fd.as_fd().as_raw_fd()))? as _,
        ))
    }
}
