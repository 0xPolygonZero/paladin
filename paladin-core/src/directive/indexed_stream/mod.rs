use std::{
    pin::Pin,
    task::{Context, Poll},
};

use anyhow::Result;
use futures::{Stream, StreamExt, TryStreamExt};

/// A [`Stream`] optimized for parallel processing of ordered data.
///
/// The optimization is achieved by associating each item in the stream with
/// its order. This can be leveraged by consumers of [`IndexedStream`]s to
/// reason about the original ordering of the items as they become available.
/// This can obviate the need to drive a stream to completion before operating
/// on the results.
///
/// # From an [`IntoIterator`]
///
/// A [`From`] implementation is provided for any `IntoIterator`, allowing for
/// easy conversion from an arbitrary collection into an [`IndexedStream`].
///
/// ## Example
/// ```
/// # use paladin::directive::IndexedStream;
/// let stream = IndexedStream::from(vec![1, 2, 3]);
/// ```
///
/// # As a [`Stream`]
///
/// A [`Stream`] implementation is also provided for [`IndexedStream`], such
/// that can be consumed as a normal stream.
///
/// ## Example
/// ```
/// # use futures::StreamExt;
/// # use paladin::directive::IndexedStream;
/// #
/// # async fn example() {
/// let mut stream = IndexedStream::from(vec![1, 2, 3]);
/// while let Some(Ok((idx, item))) = stream.next().await {
///     println!("{}: {}", idx, item);
/// }
/// # }
/// ```
///
/// # [`Directive`](crate::directive::Directive), [`Functor`](crate::directive::Functor), and [`Foldable`](crate::directive::Foldable)
///
/// [`IndexedStream`] implements [`Directive`](crate::directive::Directive),
/// [`Functor`](crate::directive::Functor), and
/// [`Foldable`](crate::directive::Foldable), allowing it to be used directly to
/// form a [`Directive`](crate::directive::Directive) chain. Note that because
/// the stream is indexed, the [`Foldable`](crate::directive::Foldable)
/// implementation satisfies associativity of combination while folding in
/// parallel.
///
/// ## Example
/// ```
/// # use paladin::{
/// #    RemoteExecute,
/// #    operation::{Operation, Monoid, Result},
/// #    directive::{Directive, IndexedStream},
/// #    runtime::Runtime,
/// #    AbortSignal
/// # };
/// # use serde::{Deserialize, Serialize};
/// #
/// # #[derive(Serialize, Deserialize, RemoteExecute)]
/// struct Multiply;
/// impl Monoid for Multiply {
///     type Elem = i32;
///
///     fn combine(&self, a: i32, b: i32, abort: AbortSignal) -> Result<i32> {
///         use paladin::AbortSignal;
/// Ok(a * b)
///     }
///
///     fn empty(&self) -> i32 {
///         1
///     }
/// }
///
/// # #[derive(Serialize, Deserialize, RemoteExecute)]
/// struct MultiplyBy(i32);
/// impl Operation for MultiplyBy {
///     type Input = i32;
///     type Output = i32;
///
///     fn execute(&self, input: i32, abort: AbortSignal) -> Result<i32> {
///         Ok(self.0 * input)
///     }
/// }
///
/// # #[tokio::main]
/// # async fn main() -> anyhow::Result<()> {
/// # let runtime = Runtime::in_memory().await?;
/// let computation = IndexedStream::from([1, 2, 3, 4, 5])
///     .map(&MultiplyBy(2))
///     .fold(&Multiply);
/// let result = computation.run(&runtime).await?;
/// assert_eq!(result, 3840);
/// # Ok(())
/// # }
/// ```
pub struct IndexedStream<'a, T> {
    inner: StreamInner<'a, T>,
}

type StreamInner<'a, Item> = Pin<Box<dyn Stream<Item = Result<(usize, Item)>> + Send + 'a>>;

impl_hkt!(IndexedStream<'a>);
impl_lit!(IndexedStream<'a, T>);

impl<'a, T> IndexedStream<'a, T> {
    /// Create a new [`IndexedStream`].
    pub fn new(inner: (impl Stream<Item = Result<(usize, T)>> + Send + 'a)) -> Self {
        Self {
            inner: Box::pin(inner),
        }
    }

    /// Convert this [`IndexedStream`] into an [`IntoIterator`] of values,
    /// sorted by their index.
    ///
    /// Note that this will drive the stream to completion.
    ///
    /// # Example
    /// ```
    /// # use paladin::directive::IndexedStream;
    /// # async fn example() -> anyhow::Result<()> {
    /// let stream = IndexedStream::from(vec![1, 2, 3]);
    /// let values = stream.into_values_sorted().await?
    ///     .into_iter()
    ///     .collect::<Vec<_>>();
    /// assert_eq!(vec![1, 2, 3], values);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn into_values_sorted(self) -> Result<impl IntoIterator<Item = T>> {
        let mut vec = self.inner.try_collect::<Vec<_>>().await?;
        vec.sort_unstable_by(|a, b| a.0.cmp(&b.0));
        Ok(vec.into_iter().map(|(_, v)| v))
    }
}

/// Create an [`IndexedStream`] from an [`IntoIterator`].
///
/// # Example
/// ```
/// # use paladin::directive::IndexedStream;
/// let stream = IndexedStream::from(vec![1, 2, 3]);
/// ```
pub fn from_into_iterator<'a, T: 'a, I: IntoIterator<Item = T>>(iter: I) -> IndexedStream<'a, T>
where
    <I as IntoIterator>::IntoIter: Send + 'a,
{
    IndexedStream::new(futures::stream::iter(iter.into_iter().enumerate().map(Ok)))
}

/// Create an [`IndexedStream`] from an [`IntoIterator`] containing `Result`s.
///
/// # Example
/// ```
/// # use paladin::directive::indexed_stream::try_from_into_iterator;
/// let stream = try_from_into_iterator(vec![Ok(1), Ok(2), Ok(3)]);
/// ```
pub fn try_from_into_iterator<'a, Item, I: IntoIterator<Item = Result<Item>>>(
    iter: I,
) -> IndexedStream<'a, Item>
where
    <I as IntoIterator>::IntoIter: Send + 'a,
{
    IndexedStream::new(futures::stream::iter(
        iter.into_iter()
            .enumerate()
            .map(|(idx, item)| item.map(|item| (idx, item))),
    ))
}

/// Create an [`IndexedStream`] from an [`IntoIterator`].
impl<'a, T: 'a, I: IntoIterator<Item = T>> From<I> for IndexedStream<'a, T>
where
    <I as IntoIterator>::IntoIter: Send + Sync + 'a,
{
    fn from(iter: I) -> Self {
        from_into_iterator(iter)
    }
}

/// [`Stream`] implementation for [`IndexedStream`].
///
/// This is a passthrough implementation, which simply delegates to the inner
/// stream.
impl<'a, T> Stream for IndexedStream<'a, T> {
    type Item = Result<(usize, T)>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.get_mut().inner.poll_next_unpin(cx)
    }
}

mod foldable;
mod functor;
