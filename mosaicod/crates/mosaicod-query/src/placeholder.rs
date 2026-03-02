use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

/// Used to handle placeholders across different stacked compilers
#[derive(Clone)]
pub struct Placeholder {
    counter: Arc<AtomicUsize>,
}

impl Placeholder {
    /// Creates a new placeholder
    pub fn new() -> Self {
        Self {
            counter: Arc::new(AtomicUsize::new(1)),
        }
    }

    /// Create a new placeholder starting from a given index
    pub fn from_index(from: usize) -> Self {
        Self {
            counter: Arc::new(AtomicUsize::new(from)),
        }
    }

    /// Consumes a polaceholder, i.e. returns the current placeholder
    /// and increase th eplaceholder value
    pub fn consume(&mut self) -> usize {
        self.counter.fetch_add(1, Ordering::SeqCst)
    }

    /// Returns the current placeholder without increasing the internal number
    pub fn current(&self) -> usize {
        self.counter.load(Ordering::SeqCst)
    }
}

impl Default for Placeholder {
    fn default() -> Self {
        Self::new()
    }
}
