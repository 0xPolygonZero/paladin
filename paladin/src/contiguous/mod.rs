//! Contiguous behavior.

/// Contiguous behavior.
///
/// An implementing type must be able to determine if it is adjacent to another
/// instance of the same type. Useful for situations in which an values muse be
/// paired contiguously.
pub trait Contiguous {
    /// The key type. Used for indexing.
    type Key: Ord + Copy;

    /// Returns true if the implementing type is contiguous to the other
    /// instance.
    fn is_contiguous(&self, other: &Self) -> bool;

    /// Returns the key for the implementing type. Used for indexing.
    fn key(&self) -> &Self::Key;
}

mod queue;
pub use queue::ContiguousQueue;
