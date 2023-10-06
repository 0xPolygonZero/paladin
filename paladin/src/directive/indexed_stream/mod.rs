use std::{
    pin::Pin,
    task::{Context, Poll},
};

use futures::{Stream, StreamExt};
use pin_project::pin_project;

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
/// while let Some((idx, item)) = stream.next().await {
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
/// #    operation::{Operation, Monoid},
/// #    directive::{Directive, IndexedStream},
/// #    opkind_derive::OpKind,
/// #    runtime::Runtime,
/// # };
/// # use serde::{Deserialize, Serialize};
/// # use anyhow::Result;
/// #
/// # #[derive(Clone, Copy, Debug, Deserialize, Serialize)]
/// struct Multiply;
/// impl Monoid for Multiply {
///     type Elem = i32;
///     type Kind = MyOps;
///
///     fn combine(&self, a: i32, b: i32) -> Result<i32> {
///         Ok(a * b)
///     }
///
///     fn empty(&self) -> i32 {
///         1
///     }
/// }
///
/// # #[derive(Clone, Copy, Debug, Deserialize, Serialize)]
/// struct MultiplyBy(i32);
/// impl Operation for MultiplyBy {
///     type Input = i32;
///     type Output = i32;
///     type Kind = MyOps;
///
///     fn execute(&self, input: i32) -> Result<i32> {
///         Ok(self.0 * input)
///     }
/// }
/// #
/// # #[derive(OpKind, Copy, Clone, Debug, Deserialize, Serialize)]
/// # enum MyOps {
/// #    Multiply(Multiply),
/// #    MultiplyBy(MultiplyBy),
/// # }
///
/// # #[tokio::main]
/// # async fn main() -> Result<()> {
/// # let runtime = Runtime::in_memory().await?;
/// let computation = IndexedStream::from([1, 2, 3, 4, 5])
///     .map(MultiplyBy(2))
///     .fold(Multiply);
/// let result = computation.run(&runtime).await?;
/// assert_eq!(result, 3840);
/// # Ok(())
/// # }
/// ```
#[pin_project]
pub struct IndexedStream<Item> {
    #[pin]
    inner: Box<dyn Stream<Item = (usize, Item)> + Send + Unpin>,
}

impl_lit!(IndexedStream<Item>);
impl_hkt!(IndexedStream);

impl<Item> IndexedStream<Item> {
    /// Create a new [`IndexedStream`].
    pub fn new(inner: impl Stream<Item = (usize, Item)> + Send + Unpin + 'static) -> Self {
        Self {
            inner: Box::new(inner),
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
    /// # async fn example() {
    /// let stream = IndexedStream::from(vec![1, 2, 3]);
    /// let values = stream.into_values_sorted().await
    ///     .into_iter()
    ///     .collect::<Vec<_>>();
    /// assert_eq!(vec![1, 2, 3], values);
    /// # }
    /// ```
    pub async fn into_values_sorted(self) -> impl IntoIterator<Item = Item> {
        let mut vec = self.collect::<Vec<_>>().await;
        vec.sort_unstable_by(|a, b| a.0.cmp(&b.0));
        vec.into_iter().map(|(_, v)| v)
    }
}

/// Create an [`IndexedStream`] from an [`IntoIterator`].
///
/// # Example
/// ```
/// # use paladin::directive::IndexedStream;
/// let stream = IndexedStream::from(vec![1, 2, 3]);
/// ```
pub fn from_into_iterator<Item, IntoIter: IntoIterator<Item = Item> + Send + Sync + 'static>(
    iter: IntoIter,
) -> IndexedStream<Item>
where
    <IntoIter as IntoIterator>::IntoIter: Send + Sync,
{
    IndexedStream::new(futures::stream::iter(iter.into_iter().enumerate()))
}

/// Create an [`IndexedStream`] from an [`IntoIterator`].
impl<Item, IntoIter: IntoIterator<Item = Item> + Send + Sync + 'static> From<IntoIter>
    for IndexedStream<Item>
where
    <IntoIter as IntoIterator>::IntoIter: Send + Sync,
{
    fn from(iter: IntoIter) -> Self {
        from_into_iterator(iter)
    }
}

/// [`Stream`] implementation for [`IndexedStream`].
///
/// This is a passthrough implementation, which simply delegates to the inner
/// stream.
impl<Item> Stream for IndexedStream<Item> {
    type Item = (usize, Item);

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        this.inner.poll_next(cx)
    }
}

mod foldable;
mod functor;
