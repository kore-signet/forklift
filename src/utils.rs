use std::sync::{atomic::AtomicUsize, Arc};
#[repr(transparent)]
pub struct SimplePermit {
    inner: Arc<AtomicUsize>,
}

impl SimplePermit {
    pub fn new(semaphore: &Arc<AtomicUsize>) -> SimplePermit {
        let inner = Arc::clone(semaphore);
        inner.fetch_add(1, std::sync::atomic::Ordering::Acquire);
        SimplePermit { inner }
    }
}

impl Drop for SimplePermit {
    fn drop(&mut self) {
        self.inner.fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
    }
}
