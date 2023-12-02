use std::{
    ops::{Deref, DerefMut},
    pin::Pin,
};

use anyhow::Result;
use async_trait::async_trait;
use futures::{TryStream, TryStreamExt};

/// Publisher for the [`PublisherExt::with`] function.
pub struct With<P, F, O> {
    p: P,
    f: F,
    _marker: std::marker::PhantomData<O>,
}

#[async_trait]
impl<P, F, Input, Output> Publisher<Input> for With<P, F, Output>
where
    P: Publisher<Output> + Sync,
    Input: Sync,
    Output: Send + Sync,
    F: Fn(&Input) -> Result<Output> + Sync,
{
    async fn publish(&self, payload: &Input) -> Result<()> {
        let transformed = (self.f)(payload)?;
        self.p.publish(&transformed).await
    }

    async fn close(&self) -> Result<()> {
        self.p.close().await
    }
}

impl<T: ?Sized, Item> PublisherExt<Item> for T where T: Publisher<Item> {}

/// Extension trait for [`Publisher`].
///
/// This trait provides additional functionality for [`Publisher`], similar to
/// how [`futures::SinkExt`] does for [`futures::Sink`].
#[async_trait]
pub trait PublisherExt<T>: Publisher<T> {
    /// Compose a function in _front_ of the publisher.
    fn with<F, Input, Output>(self, f: F) -> With<Self, F, Output>
    where
        F: Fn(&Input) -> Result<Output>,
        Self: Sized,
    {
        With {
            p: self,
            f,
            _marker: std::marker::PhantomData,
        }
    }

    /// Drive the given stream to completion, publishing each yielded item
    /// concurrently.
    async fn publish_all<S: TryStream<Ok = T, Error = anyhow::Error> + Send + Unpin>(
        &self,
        stream: S,
        max_concurrency: impl Into<Option<usize>> + Send,
    ) -> Result<()>
    where
        T: Send,
    {
        stream
            .try_for_each_concurrent(max_concurrency, |item| async move {
                self.publish(&item).await?;
                Ok(())
            })
            .await?;

        Ok(())
    }
}

/// A non-locking replacement for [`Sink`](futures::Sink).
///
/// Interactions with a [`Sink`](futures::Sink) generally require exclusive
/// mutable access to the underlying sink, which is problematic when working in
/// a highly concurrent system. While implementations of
/// [`Sink`](futures::Sink), like
/// [`futures::channel::mpsc::Sender`](futures::channel::mpsc::Sender), are
/// generally cloneable, the [`Sink`](futures::Sink) trait itself does not
/// require any such guarantees, which means that code that simply relies on
/// [`Sink`](futures::Sink) may not make any assumptions about the ability to
/// clone the underlying [`Sink`](futures::Sink). As such, we provide a trait
/// that is functionally equivalent to [`Sink`](futures::Sink), but does not
/// require exclusive mutable access to publish messages.
#[async_trait]
pub trait Publisher<T> {
    /// Publish a message.
    ///
    /// The implementation should be take care of serializing the payload before
    /// publishing.
    async fn publish(&self, payload: &T) -> Result<()>;

    /// Close the publisher, signaling to any receivers that no more messages
    /// will be published.
    async fn close(&self) -> Result<()>;
}

#[async_trait]
impl<T: Sync, P: ?Sized + Publisher<T> + Sync> Publisher<T> for Box<P> {
    async fn publish(&self, payload: &T) -> Result<()> {
        (**self).publish(payload).await
    }

    async fn close(&self) -> Result<()> {
        (**self).close().await
    }
}

#[async_trait]
impl<T: Sync, P> Publisher<T> for Pin<P>
where
    P: DerefMut + Unpin + Sync,
    P::Target: Publisher<T> + Sync,
{
    async fn publish(&self, payload: &T) -> Result<()> {
        self.deref().publish(payload).await
    }

    async fn close(&self) -> Result<()> {
        self.deref().close().await
    }
}
