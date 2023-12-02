use crossbeam::atomic::AtomicCell;
use crossbeam_skiplist::SkipMap;

use super::Contiguous;

/// Utility for queueing and assembling values in a monoidal fashion.
///
/// In particular, `ContiguousQueue` provides functions for queueing values and
/// attempting to acquire adjacent values.
pub struct ContiguousQueue<T: Contiguous> {
    values: SkipMap<T::Key, AtomicCell<Option<T>>>,
}

/// Represents the position of a value relative to another value.
pub enum Position<T> {
    Lhs(T),
    Rhs(T),
}

impl<T: Contiguous> ContiguousQueue<T> {
    pub fn new() -> Self {
        Self {
            values: SkipMap::new(),
        }
    }
}

impl<T> ContiguousQueue<T>
where
    T: Contiguous + Send + 'static,
    T::Key: Send,
{
    /// Searches for a value that is contiguous to the provided `next_value`.
    ///
    /// Contiguity is stricter than mere ordering. Two values are considered
    /// contiguous if they are immediately next to each other without any
    /// gap. For example, in a series of integers, 1 and 2 are contiguous,
    /// but 1 and 3 are not.
    ///
    /// Returns the position of the contiguous value relative to `next_value` if
    /// found, otherwise `None`.
    pub fn find_contiguous(&self, next_value: &T) -> Option<Position<T>> {
        let key = next_value.key();
        self.values
            // Check the next_value immediately greater than the given range
            .range(key..)
            .next()
            .and_then(|entry| {
                let value = entry.value();

                // This is safe because:
                // 1. We are only reading the value.
                // 2. crossbeam's Entry ensures that the value is not removed while the lifetime
                //    is in scope.
                // 3. AtomicCell::take() is atomic. If another thread attempts to take the value
                //    only one thread will succeed.
                match unsafe { &*value.as_ptr() } {
                    Some(adjacent_value) if next_value.is_contiguous(adjacent_value) => {
                        let value = value.take()?;
                        self.values.remove(entry.key());
                        Some(Position::Rhs(value))
                    }
                    _ => None,
                }
            })
            .or_else(|| {
                // Check the range immediately smaller than the given range by iterating in
                // reverse
                self.values.range(..key).next_back().and_then(|entry| {
                    let value = entry.value();

                    match unsafe { &*value.as_ptr() } {
                        Some(adjacent_value) if next_value.is_contiguous(adjacent_value) => {
                            let value = value.take()?;
                            self.values.remove(entry.key());
                            Some(Position::Lhs(value))
                        }
                        _ => None,
                    }
                })
            })
    }

    /// Adds the provided value to the queue.
    ///
    /// This method is useful when a value doesn't have an immediately
    /// contiguous counterpart in the queue. It ensures the value is stored
    /// and can be paired later when its contiguous counterpart arrives.
    #[inline]
    pub fn queue(&self, value: T) {
        self.values
            .insert(*value.key(), AtomicCell::new(Some(value)));
    }

    /// Removes the value with the given key from the queue.
    #[inline]
    pub fn dequeue(&self, key: &T::Key) -> Option<T> {
        self.values.remove(key)?.value().take()
    }

    /// Attempts to find a value contiguous to the provided `next_value` and
    /// pair them.
    ///
    /// If a contiguous value is found, it returns the pair. If not, the
    /// `next_value` is queued for future pairing.
    pub fn acquire_contiguous_pair_or_queue(&self, next_value: T) -> Option<(T, T)> {
        match self.find_contiguous(&next_value) {
            Some(position) => match position {
                Position::Lhs(contiguous) => Some((contiguous, next_value)),
                Position::Rhs(contiguous) => Some((next_value, contiguous)),
            },
            None => {
                self.queue(next_value);
                None
            }
        }
    }
}

impl<T: Contiguous> Default for ContiguousQueue<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: Contiguous, Iter: IntoIterator<Item = T>> From<Iter> for ContiguousQueue<T> {
    fn from(iter: Iter) -> Self {
        Self {
            values: iter
                .into_iter()
                .map(|x| (*x.key(), AtomicCell::new(Some(x))))
                .collect(),
        }
    }
}

#[cfg(test)]
mod test {
    use std::ops::RangeInclusive;

    use super::{Contiguous, ContiguousQueue};

    #[derive(Clone, PartialEq, Eq, Debug)]
    struct TestValue {
        range: RangeInclusive<usize>,
    }

    fn make_value(range: RangeInclusive<usize>) -> TestValue {
        TestValue { range }
    }

    impl Contiguous for TestValue {
        type Key = usize;

        fn is_contiguous(&self, other: &Self) -> bool {
            self.range.end() + 1 == *other.range.start()
                || *other.range.end() + 1 == *self.range.start()
        }

        fn key(&self) -> &Self::Key {
            self.range.start()
        }
    }

    #[test]
    fn it_finds_lesser_range() {
        let x = ContiguousQueue::from([
            make_value(0..=1),
            make_value(2..=3),
            make_value(4..=5),
            make_value(10..=11),
        ]);

        let next = make_value(6..=7);
        let expected = (make_value(4..=5), next.clone());

        assert_eq!(x.acquire_contiguous_pair_or_queue(next), Some(expected));
    }

    #[test]
    fn it_finds_greater_range() {
        let x = ContiguousQueue::from([
            make_value(0..=1),
            make_value(2..=3),
            make_value(4..=5),
            make_value(10..=11),
        ]);

        let next = make_value(8..=9);
        let expected = (next.clone(), make_value(10..=11));

        assert_eq!(x.acquire_contiguous_pair_or_queue(next), Some(expected));
    }

    #[test]
    fn it_queues_ranges_with_no_adjacency() {
        let x = ContiguousQueue::from([
            make_value(0..=1),
            make_value(2..=3),
            make_value(4..=5),
            make_value(10..=11),
        ]);

        let queued = make_value(20..=21);
        let result = x.acquire_contiguous_pair_or_queue(queued.clone());

        assert_eq!(result, None);
        let next = make_value(22..=25);
        let expected = (queued, next.clone());
        assert_eq!(x.acquire_contiguous_pair_or_queue(next), Some(expected));
    }
}
