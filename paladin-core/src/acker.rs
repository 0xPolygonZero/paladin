//! Provides a trait for acknowledging messages in an asynchronous context.
//!
//! Acknowledgement is a common pattern in distributed systems, especially in
//! message-driven architectures. This module introduces a trait, `Acker`, which
//! abstracts the act of acknowledging a message. This abstraction allows for a
//! consistent interface across different components or systems that require
//! message acknowledgement.
//!
//! The trait is designed with async/await in mind, ensuring that
//! acknowledgements can be handled in an asynchronous manner without blocking
//! the main thread.
//!
//! # Features:
//! - **Asynchronous**: Uses async/await for non-blocking operations.
//! - **Generic Implementation for Boxed Types**: Allows for easy integration
//!   with boxed types, ensuring flexibility in usage.
//!
//! # Examples
//!
//! Implementing the `Acker` trait for a custom type:
//!
//! ```
//! use paladin::acker::Acker;
//! use anyhow::Result;
//! use async_trait::async_trait;
//!
//! struct MyAcker;
//!
//! #[async_trait]
//! impl Acker for MyAcker {
//!     async fn ack(&self) -> Result<()> {
//!         // Custom acknowledgement logic here...
//!         Ok(())
//!     }
//!
//!     async fn nack(&self) -> Result<()> {
//!         // Custom negative acknowledgement logic here...
//!         Ok(())
//!     }
//! }
//! ```
//!
//! Using the `Acker` trait with boxed types:
//!
//! ```
//! # use paladin::acker::Acker;
//! # use anyhow::Result;
//! # use async_trait::async_trait;
//! # struct MyAcker;
//! #
//! # #[async_trait]
//! # impl Acker for MyAcker {
//! #    async fn ack(&self) -> Result<()> {
//! #        // Custom acknowledgement logic here...
//! #        Ok(())
//! #    }
//! #
//! #    async fn nack(&self) -> Result<()> {
//! #        // Custom negative acknowledgement logic here...
//! #        Ok(())
//! #    }
//! # }
//! # #[tokio::main]
//! # async fn main() -> Result<()> {
//! let my_acker: Box<MyAcker> = Box::new(MyAcker);
//! my_acker.ack().await?;
//! # Ok(())
//! # }
//! ```
use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use futures::TryFutureExt;

/// Represents a generic behavior for acknowledging messages.
///
/// This trait provides a unified interface for components that require message
/// acknowledgement. Implementers of this trait should provide the actual logic
/// for acknowledging a message in the `ack`  and `nack` methods.
#[async_trait]
pub trait Acker: Send + Sync + 'static {
    async fn ack(&self) -> Result<()>;

    async fn nack(&self) -> Result<()>;
}

/// Provides an implementation of the `Acker` trait for boxed types.
///
/// This allows for flexibility in using the `Acker` trait with dynamically
/// dispatched types.
#[async_trait]
impl<T: Acker + ?Sized> Acker for Box<T> {
    async fn ack(&self) -> Result<()> {
        (**self).ack().await
    }

    async fn nack(&self) -> Result<()> {
        (**self).nack().await
    }
}

/// Provides an implementation of the `Acker` trait for `Arc` types.
#[async_trait]
impl<T: Acker + ?Sized> Acker for Arc<T> {
    async fn ack(&self) -> Result<()> {
        (**self).ack().await
    }

    async fn nack(&self) -> Result<()> {
        (**self).nack().await
    }
}

/// An `Acker` implementation that composes two `Acker` instances.
///
/// The `ack` and `nack` methods on the second `Acker` instance will only be
/// called if the first `Acker` instance succeeds.
pub struct ComposedAcker<A, B> {
    fst: A,
    snd: B,
}

impl<A, B> ComposedAcker<A, B> {
    pub fn new(fst: A, snd: B) -> Self {
        Self { fst, snd }
    }
}

#[async_trait]
impl<A: Acker, B: Acker> Acker for ComposedAcker<A, B> {
    /// Acknowledge the second `Acker` instance if the first `Acker` instance
    /// succeeds.
    async fn ack(&self) -> Result<()> {
        self.fst.ack().and_then(|_| self.snd.ack()).await
    }

    /// Negative acknowledge the second `Acker` instance if the first `Acker`
    /// instance succeeds.
    async fn nack(&self) -> Result<()> {
        self.fst.nack().and_then(|_| self.snd.nack()).await
    }
}

/// Acker implementation that does nothing.
///
/// Useful for testing purposes where channels are emulated in-memory.
#[derive(Default)]
pub struct NoopAcker;

impl NoopAcker {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl Acker for NoopAcker {
    async fn ack(&self) -> Result<()> {
        // noop
        Ok(())
    }

    async fn nack(&self) -> Result<()> {
        // noop
        Ok(())
    }
}
