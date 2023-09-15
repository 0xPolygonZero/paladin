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
//! # }
//! # #[tokio::main]
//! # async fn main() -> Result<()> {
//! let my_acker: Box<MyAcker> = Box::new(MyAcker);
//! my_acker.ack().await?;
//! # Ok(())
//! # }
//! ```

use anyhow::Result;
use async_trait::async_trait;

/// Represents a generic behavior for acknowledging messages.
///
/// This trait provides a unified interface for components that require message
/// acknowledgement. Implementers of this trait should provide the actual logic
/// for acknowledging a message in the `ack` method.
#[async_trait]
pub trait Acker: Send + Sync + 'static {
    async fn ack(&self) -> Result<()>;
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
}

/// Provides an implementation of an `Acker` that allows for custom logic to be
/// executed on successful acknowledgement.
pub struct ComposedAcker<A: Acker, F: FnOnce() + Send + Sync + 'static> {
    acker: A,
    on_ack: F,
}

impl<A: Acker, F: Fn() + Send + Sync + 'static> ComposedAcker<A, F> {
    pub fn new(acker: A, on_ack: F) -> Self {
        Self { acker, on_ack }
    }
}

#[async_trait]
impl<A: Acker, F: Fn() + Send + Sync + 'static> Acker for ComposedAcker<A, F> {
    async fn ack(&self) -> Result<()> {
        let result = self.acker.ack().await;
        if result.is_ok() {
            (self.on_ack)();
        }
        result
    }
}
