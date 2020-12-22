use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Mutex;

/// Use for locking of the structure, ensuring that inner fields are locked properly, but also
/// allow for releasing of internal references
#[derive(Default)]
pub struct Lock {
    internal_lock: Mutex<()>,
    write: AtomicBool,
    read: AtomicUsize,
}

impl Lock {
    pub fn try_read(&self) -> Option<LockRead<'_>> {
        let _mutex = self.internal_lock.lock().unwrap();
        if self.write.load(Ordering::Relaxed) {
            None
        } else {
            self.read.fetch_add(1, Ordering::Acquire);
            Some(LockRead(self))
        }
    }

    pub fn read(&self) -> LockRead<'_> {
        loop {
            if let Some(read) = self.try_read() {
                return read;
            }
        }
    }

    pub fn try_write(&self) -> Option<LockWrite<'_>> {
        let _mutex = self.internal_lock.lock().unwrap();
        if self.read.load(Ordering::Relaxed) > 0 {
            return None;
        }
        if self.write.compare_and_swap(false, true, Ordering::Relaxed) {
            None
        } else {
            Some(LockWrite(self))
        }
    }

    pub fn write(&self) -> LockWrite<'_> {
        loop {
            if let Some(write) = self.try_write() {
                return write;
            }
        }
    }
}

/// Locks a write lock from being formed, but still allows for more than one read to be made
pub struct LockRead<'a>(&'a Lock);

impl Clone for LockRead<'_> {
    /// Can create more reads from a single read, and will extend the read until all LockRead instances
    /// have been dropped
    fn clone(&self) -> Self {
        self.0.read.fetch_add(1, Ordering::Acquire);
        LockRead(self.0)
    }
}

impl Drop for LockRead<'_> {
    fn drop(&mut self) {
        self.0.read.fetch_sub(1, Ordering::Release);
    }
}

/// Prevents read locks from being formed, and no other write lock can be made
pub struct LockWrite<'a>(&'a Lock);

impl Drop for LockWrite<'_> {
    fn drop(&mut self) {
        self.0.write.store(false, Ordering::Release)
    }
}
