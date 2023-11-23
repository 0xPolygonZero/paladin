//! Operation error types.
//!
//! Given the distributed nature of the Paladin system, it is important to be
//! able to specify how the system should respond to various flavors of errors.
//! This module provides the error types and strategies for handling errors.
//!
//! Paladin recognizes two types of errors: transient and fatal. Transient
//! errors are those that are expected to be resolved by retrying the operation.
//! For example, a transient error might be a network timeout or a database
//! deadlock. Fatal errors are those that are not expected to be resolved by
//! retrying the operation. For example, a fatal error might be a malformed
//! request or a database connection error.
use std::{num::NonZeroU32, time::Duration};

use futures::Future;
use thiserror::Error;
use tracing::error;

/// A retry strategy for handling transient errors.
///
/// This enum is used to specify how the system should respond to transient
/// errors. A transient error is one that is expected to be resolved by retrying
/// the operation.
///
/// A transient error may turn into a fatal error if the maximum number of
/// retries is exceeded. As such, a transient error may additionally specify a
/// fatal strategy.
///
/// The following strategies are supported:
/// - `Immediate`: Retry the operation immediately (default).
/// - `After`: Retry the operation after a specified duration.
/// - `Exponential`: Retry the operation with the provided exponential backoff.
///   This strategy is only available when the `backoff` feature is enabled.
#[derive(Debug, Clone, Copy)]
pub enum RetryStrategy {
    /// Retry the operation immediately.
    Immediate { max_retries: NonZeroU32 },
    /// Retry the operation after a specified duration.
    After {
        max_retries: NonZeroU32,
        duration: Duration,
    },
    /// Retry the operation with the provided exponential backoff.
    Exponential {
        min_duration: Duration,
        max_duration: Duration,
    },
}

impl Default for RetryStrategy {
    fn default() -> Self {
        Self::Immediate {
            max_retries: NonZeroU32::new(3).unwrap(),
        }
    }
}

type DynTracer<E> = Box<dyn Fn(E) + Send + Sync>;

impl RetryStrategy {
    /// Retry the operation according to the strategy.
    pub async fn retry<O, E, Fut, F>(self, f: F) -> std::result::Result<O, E>
    where
        E: std::fmt::Debug,
        Fut: Future<Output = std::result::Result<O, E>>,
        F: Fn() -> Fut,
    {
        retry_with_strategy(f, self, None as Option<DynTracer<E>>).await
    }

    pub async fn retry_trace<O, E, Fut, F, T>(self, f: F, tracer: T) -> std::result::Result<O, E>
    where
        E: std::fmt::Debug,
        Fut: Future<Output = std::result::Result<O, E>>,
        F: Fn() -> Fut,
        T: Fn(E),
    {
        retry_with_strategy(f, self, Some(tracer)).await
    }

    pub fn into_backoff(self) -> anyhow::Result<backoff::ExponentialBackoff> {
        match self {
            Self::Exponential {
                min_duration,
                max_duration,
            } => {
                let mut backoff = backoff::ExponentialBackoffBuilder::new();
                backoff.with_initial_interval(min_duration);
                backoff.with_max_elapsed_time(Some(max_duration));
                Ok(backoff.build())
            }
            _ => anyhow::bail!("retry strategy is not exponential"),
        }
    }
}

impl TryFrom<RetryStrategy> for backoff::ExponentialBackoff {
    type Error = anyhow::Error;

    fn try_from(value: RetryStrategy) -> anyhow::Result<Self, Self::Error> {
        value.into_backoff()
    }
}

/// A strategy for handling fatal errors.
///
/// This enum is used to specify how the system should respond to fatal errors.
/// A fatal error is one that is not expected to be resolved by retrying the
/// operation.
///
/// One should take special care when using the `Ignore` strategy. This strategy
/// will cause the system to hang indefinitely if the operation is never
/// fulfilled by some other means. Practically speaking, one should consider
/// using a dead letter exchange to handle these cases. The reason the system
/// doesn't simply "move on" in the ignore case is that the omission of a result
/// would violate the semantics of most directives. For example, a `Map`
/// resulting in fewer elements than the input would violate the semantics of
/// the `Map` directive.
///
/// The following strategies are supported:
/// - `Terminate`: Terminate the entire distributed program (default). This will
///   bubble up to the leader process and cause the directive chain containing
///   the operation to return an error.
/// - `Ignore`: Ignore the error and continue with the rest of the directive
///   chain.
#[derive(Debug, Default, Clone, Copy)]
pub enum FatalStrategy {
    /// Terminate the entire distributed program.
    #[default]
    Terminate,
    /// Ignore the error and continue with the rest of the directive chain.
    Ignore,
}

/// Operation error types.
///
/// Given the distributed nature of the Paladin system, it is important to be
/// able to specify how the system should respond to various flavors of errors.
/// This type provides the error types and strategies for handling errors.
///
/// Paladin recognizes two types of errors: transient and fatal. Transient
/// errors are those that are expected to be resolved by retrying the operation.
/// For example, a transient error might be a network timeout or a database
/// deadlock. Fatal errors are those that are not expected to be resolved by
/// retrying the operation. For example, a fatal error might be a malformed
/// request or a database connection error.
/// ## Example
///
/// ### An [`Operation`](crate::operation::Operation) failing with a fatal error:
///
/// ```
/// use paladin::{
///     runtime::Runtime,
///     operation::{Operation, Result, FatalError, FatalStrategy},
///     directive::{Directive, IndexedStream},
///     opkind_derive::OpKind
/// };
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Serialize, Deserialize, Debug, Clone, Copy)]
/// struct WillFail;
///
/// impl Operation for WillFail {
///     type Input = i64;
///     type Output = i64;
///     type Kind = MyOps;
///     
///     fn execute(&self, _: Self::Input) -> Result<Self::Output> {
///         FatalError::from_str(
///             "This operation will always fail.",
///             FatalStrategy::default()
///         )
///         .into()
///     }
/// }
///
/// #[derive(OpKind, Serialize, Deserialize, Debug, Clone, Copy)]
/// enum MyOps {
///     WillFail(WillFail),
/// }
///
/// #[tokio::main]
/// async fn main() -> anyhow::Result<()> {
/// # let runtime = Runtime::in_memory().await?;
///     let computation = IndexedStream::from([1, 2, 3]).map(WillFail);
///     let result = computation
///         .run(&runtime).await?
///         .into_values_sorted().await
///         .map(|values| values.into_iter().collect::<Vec<_>>());
///
///     assert_eq!(result.unwrap_err().to_string(), "Fatal operation error: This operation will always fail.");
/// # Ok(())
/// }
/// ```
///
/// ### A [`Monoid`](crate::operation::Monoid) failing with a fatal error:
///
/// ```
/// use paladin::{
///     runtime::Runtime,
///     operation::{Operation, Monoid, Result, FatalError, FatalStrategy},
///     directive::{Directive, IndexedStream},
///     opkind_derive::OpKind
/// };
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Serialize, Deserialize, Debug, Clone, Copy)]
/// struct WillFail;
///
/// impl Monoid for WillFail {
///     type Elem = i64;
///     type Kind = MyOps;
///
///     fn empty(&self) -> Self::Elem {
///         0
///     }
///
///     fn combine(&self, _: Self::Elem, _: Self::Elem) -> Result<Self::Elem> {
///         FatalError::from_str(
///             "This operation will always fail.",
///             FatalStrategy::default()
///         )
///         .into()
///     }
/// }
///
/// #[derive(OpKind, Serialize, Deserialize, Debug, Clone, Copy)]
/// enum MyOps {
///     WillFail(WillFail),
/// }
///
/// #[tokio::main]
/// async fn main() -> anyhow::Result<()> {
/// # let runtime = Runtime::in_memory().await?;
///     let computation = IndexedStream::from([1, 2, 3]).fold(WillFail);
///     let result = computation.run(&runtime).await;
///
///     assert_eq!(result.unwrap_err().to_string(), "Fatal operation error: This operation will always fail.");
/// # Ok(())
/// }
/// ```
///
/// ### An [`IndexedStream`](crate::directive::IndexedStream) containing errors:
///
/// ```
/// use paladin::{
///     runtime::Runtime,
///     operation::{Operation, Monoid, Result},
///     directive::{Directive, IndexedStream, indexed_stream::try_from_into_iterator},
///     opkind_derive::OpKind
/// };
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Serialize, Deserialize, Debug, Clone, Copy)]
/// struct Multiply;
///
/// impl Monoid for Multiply {
///     type Elem = i64;
///     type Kind = MyOps;
///
///     fn empty(&self) -> Self::Elem {
///         0
///     }
///
///     fn combine(&self, a: Self::Elem, b: Self::Elem) -> Result<Self::Elem> {
///         Ok(a * b)
///     }
/// }
///
/// #[derive(OpKind, Serialize, Deserialize, Debug, Clone, Copy)]
/// enum MyOps {
///     Multiply(Multiply),
/// }
///
/// #[tokio::main]
/// async fn main() -> anyhow::Result<()> {
/// # let runtime = Runtime::in_memory().await?;
///     let computation = try_from_into_iterator([
///         Ok(1), Err(anyhow::anyhow!("Failure")), Ok(3)
///     ])
///     .fold(Multiply);
///     let result = computation.run(&runtime).await;
///
///     assert_eq!(result.unwrap_err().to_string(), "Failure");
/// # Ok(())
/// }
/// ```
///
/// ### A [`TransientError`] recovering after retry:
/// ```
/// use paladin::{
///     runtime::Runtime,
///     operation::{
///         Operation,
///         Monoid,
///         Result,
///         OperationError,
///         TransientError,
///         RetryStrategy,
///         FatalStrategy
///     },
///     directive::{Directive, IndexedStream},
///     opkind_derive::OpKind
/// };
/// use serde::{Deserialize, Serialize};
/// use std::sync::{Arc, atomic::{Ordering, AtomicBool}};
///
/// #[derive(Serialize, Deserialize, Debug, Clone, Default)]
/// struct Multiply {
///     #[serde(skip)]
///     did_try: Arc<AtomicBool>,
/// }
///
/// impl Monoid for Multiply {
///     type Elem = i64;
///     type Kind = MyOps;
///
///     fn empty(&self) -> Self::Elem {
///         0
///     }
///
///     fn combine(&self, a: Self::Elem, b: Self::Elem) -> Result<Self::Elem> {
///         if self.did_try.swap(true, Ordering::SeqCst) {
///             return Ok(a * b);
///         }
///
///         TransientError::from_str(
///             "will retry",
///             RetryStrategy::default(),
///             FatalStrategy::default()
///         )
///         .into()
///     }
/// }
///
/// #[derive(OpKind, Serialize, Deserialize, Debug, Clone)]
/// enum MyOps {
///     Multiply(Multiply),
/// }
///
/// #[tokio::main]
/// async fn main() -> anyhow::Result<()> {
/// # let runtime = Runtime::in_memory().await?;
///     let computation = IndexedStream::from([1, 2, 3]).fold(Multiply::default());
///     let result = computation.run(&runtime).await?;
///
///     assert_eq!(result, 6);
/// # Ok(())
/// }
/// ```
///
/// ### A [`TransientError`] timing out after exhausting retries:
/// ```
/// use std::{num::NonZeroU32, sync::{Arc, atomic::{Ordering, AtomicU32}}};
///
/// use paladin::{
///     runtime::Runtime,
///     operation::{
///         Operation,
///         Monoid,
///         Result,
///         OperationError,
///         TransientError,
///         RetryStrategy,
///         FatalStrategy
///     },
///     directive::{Directive, IndexedStream},
///     opkind_derive::OpKind
/// };
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Serialize, Deserialize, Debug, Clone, Default)]
/// struct Multiply {
///     #[serde(skip)]
///     num_tries: Arc<AtomicU32>,
/// }
///
/// const NUM_RETRIES: u32 = 3;
///
/// impl Monoid for Multiply {
///     type Elem = i64;
///     type Kind = MyOps;
///
///     fn empty(&self) -> Self::Elem {
///         0
///     }
///
///     fn combine(&self, a: Self::Elem, b: Self::Elem) -> Result<Self::Elem> {
///         let prev = self.num_tries.fetch_add(1, Ordering::SeqCst);
///
///         TransientError::from_str(
///             &format!("tried {}/{}", prev, NUM_RETRIES + 1),
///             RetryStrategy::Immediate {
///                 max_retries: NonZeroU32::new(NUM_RETRIES).unwrap()
///             },
///             FatalStrategy::default()
///         )
///         .into()
///     }
/// }
///
/// #[derive(OpKind, Serialize, Deserialize, Debug, Clone)]
/// enum MyOps {
///     Multiply(Multiply),
/// }
///
/// #[tokio::main]
/// async fn main() -> anyhow::Result<()> {
/// # let runtime = Runtime::in_memory().await?;
///     let op = Multiply::default();
///     let computation = IndexedStream::from([1, 2, 3]).fold(op.clone());
///     let result = computation.run(&runtime).await;
///
///     let expected = format!(
///         "Fatal operation error: tried {}/{}",
///         NUM_RETRIES + 1,
///         NUM_RETRIES + 1
///     );
///     assert_eq!(result.unwrap_err().to_string(), expected);
/// # Ok(())
/// }
#[derive(Error, Debug)]
pub enum OperationError {
    #[error("Transient operation error: {err}")]
    /// An error that is expected to be resolved by retrying the operation.
    Transient {
        /// The underlying error.
        err: anyhow::Error,
        /// The retry strategy.
        retry_strategy: RetryStrategy,
        /// The strategy to employ once the maximum number of retries is
        /// exceeded.
        fatal_strategy: FatalStrategy,
    },
    /// An error that is not expected to be resolved by retrying the operation.
    #[error("Fatal operation error: {err}")]
    Fatal {
        /// The underlying error.
        err: anyhow::Error,
        /// The failure strategy.
        strategy: FatalStrategy,
    },
}

impl OperationError {
    async fn retry_internal<O, Fut, F, T>(self, f: F, tracer: Option<T>) -> Result<O>
    where
        Fut: Future<Output = Result<O>>,
        F: Fn() -> Fut,
        T: Fn(OperationError),
    {
        match self {
            Self::Transient {
                retry_strategy,
                fatal_strategy,
                ..
            } => {
                let result = retry_with_strategy(f, retry_strategy, tracer).await;
                result.map_err(|err| Self::Fatal {
                    err: err.into_err(),
                    strategy: fatal_strategy,
                })
            }
            _ => Err(self),
        }
    }

    /// Retry the operation according to the strategy.
    ///
    /// If the error is not a transient error, it is returned unchanged.
    /// If the error is a transient error, it is retried according to the
    /// provided strategy, and if the retry policy is exhausted, the error is
    /// converted into a fatal error.
    pub async fn retry<O, Fut, F>(self, f: F) -> Result<O>
    where
        Fut: Future<Output = Result<O>>,
        F: Fn() -> Fut,
    {
        self.retry_internal(f, None as Option<DynTracer<OperationError>>)
            .await
    }

    /// Retry the operation according to the strategy and the provided tracer.
    ///
    /// If the error is not a transient error, it is returned unchanged.
    /// If the error is a transient error, it is retried according to the
    /// provided strategy, and if the retry policy is exhausted, the error is
    /// converted into a fatal error.
    pub async fn retry_trace<O, Fut, F, T>(self, f: F, tracer: T) -> Result<O>
    where
        Fut: Future<Output = Result<O>>,
        F: Fn() -> Fut,
        T: Fn(OperationError),
    {
        self.retry_internal(f, Some(tracer)).await
    }

    /// Extract the underlying error.
    pub fn into_err(self) -> anyhow::Error {
        match self {
            Self::Transient { err, .. } => err,
            Self::Fatal { err, .. } => err,
        }
    }

    /// Extract the underlying error as a reference.
    pub fn as_err(&self) -> &anyhow::Error {
        match self {
            Self::Transient { err, .. } => err,
            Self::Fatal { err, .. } => err,
        }
    }

    /// Convert an error into a fatal error.
    ///
    /// If the error is already a fatal error, it is returned unchanged.
    pub fn into_fatal(self) -> Self {
        match self {
            Self::Transient {
                err,
                fatal_strategy,
                ..
            } => Self::Fatal {
                err,
                strategy: fatal_strategy,
            },
            _ => self,
        }
    }

    pub fn fatal_strategy(&self) -> FatalStrategy {
        match self {
            Self::Transient { fatal_strategy, .. } => *fatal_strategy,
            Self::Fatal { strategy, .. } => *strategy,
        }
    }
}

/// An error that is expected to be resolved by retrying the operation.
///
/// See [`OperationError`] for more information.
#[derive(Debug)]
pub struct TransientError {
    err: anyhow::Error,
    retry_strategy: RetryStrategy,
    fatal_strategy: FatalStrategy,
}

impl TransientError {
    pub fn new(
        err: impl std::error::Error + Send + Sync + 'static,
        retry_strategy: RetryStrategy,
        fatal_strategy: FatalStrategy,
    ) -> Self {
        Self {
            err: err.into(),
            retry_strategy,
            fatal_strategy,
        }
    }

    pub fn from_anyhow(
        err: anyhow::Error,
        retry_strategy: RetryStrategy,
        fatal_strategy: FatalStrategy,
    ) -> Self {
        Self {
            err,
            retry_strategy,
            fatal_strategy,
        }
    }

    pub fn from_str(
        err: &str,
        retry_strategy: RetryStrategy,
        fatal_strategy: FatalStrategy,
    ) -> Self {
        Self {
            err: anyhow::Error::msg(err.to_string()),
            retry_strategy,
            fatal_strategy,
        }
    }
}

impl<E> From<E> for TransientError
where
    E: std::error::Error + Send + Sync + 'static,
{
    fn from(value: E) -> Self {
        Self {
            err: value.into(),
            retry_strategy: RetryStrategy::default(),
            fatal_strategy: FatalStrategy::default(),
        }
    }
}

impl From<TransientError> for OperationError {
    fn from(value: TransientError) -> Self {
        Self::Transient {
            err: value.err,
            retry_strategy: value.retry_strategy,
            fatal_strategy: value.fatal_strategy,
        }
    }
}

impl<T> From<TransientError> for Result<T> {
    fn from(value: TransientError) -> Self {
        Err(value.into())
    }
}

/// An error that is not expected to be resolved by retrying the operation.
///
/// See [`OperationError`] for more information.
#[derive(Debug)]
pub struct FatalError {
    err: anyhow::Error,
    strategy: FatalStrategy,
}

impl FatalError {
    pub fn new(
        err: impl std::error::Error + Send + Sync + 'static,
        strategy: FatalStrategy,
    ) -> Self {
        Self {
            err: err.into(),
            strategy,
        }
    }

    pub fn from_anyhow(err: anyhow::Error, strategy: FatalStrategy) -> Self {
        Self { err, strategy }
    }

    pub fn from_str(err: &str, strategy: FatalStrategy) -> Self {
        Self {
            err: anyhow::Error::msg(err.to_string()),
            strategy,
        }
    }
}

impl<E> From<E> for FatalError
where
    E: std::error::Error + Send + Sync + 'static,
{
    fn from(value: E) -> Self {
        Self {
            err: value.into(),
            strategy: FatalStrategy::default(),
        }
    }
}

impl From<FatalError> for OperationError {
    fn from(value: FatalError) -> Self {
        Self::Fatal {
            err: value.err,
            strategy: value.strategy,
        }
    }
}

impl<T> From<FatalError> for Result<T> {
    fn from(value: FatalError) -> Self {
        Err(value.into())
    }
}

pub type Result<T> = std::result::Result<T, OperationError>;

/// Retries a future with the given maximum number of retries and duration.
async fn retry_simple<O, E, Fut, F, T>(
    f: F,
    max_retries: NonZeroU32,
    duration: Option<Duration>,
    tracer: Option<T>,
) -> std::result::Result<O, E>
where
    E: std::fmt::Debug,
    Fut: Future<Output = std::result::Result<O, E>>,
    F: Fn() -> Fut,
    T: Fn(E),
{
    let mut num_retries = 0;
    let mut result = f().await;
    while let Err(err) = result {
        if num_retries >= max_retries.get() {
            return Err(err);
        }
        if let Some(tracer) = tracer.as_ref() {
            tracer(err);
        }
        num_retries += 1;
        if let Some(duration) = duration {
            tokio::time::sleep(duration).await;
        }
        result = f().await;
    }
    Ok(result.unwrap())
}

/// Retries a future with the provided [`RetryStrategy`].
async fn retry_with_strategy<O, E, Fut, F, T>(
    f: F,
    strategy: RetryStrategy,
    tracer: Option<T>,
) -> std::result::Result<O, E>
where
    E: std::fmt::Debug,
    Fut: Future<Output = std::result::Result<O, E>>,
    F: Fn() -> Fut,
    T: Fn(E),
{
    match strategy {
        RetryStrategy::Immediate { max_retries } => {
            retry_simple(f, max_retries, None, tracer).await
        }

        RetryStrategy::After {
            max_retries,
            duration,
        } => retry_simple(f, max_retries, Some(duration), tracer).await,

        exp @ RetryStrategy::Exponential { .. } => {
            let backoff = exp.into_backoff().unwrap();
            let result = backoff::future::retry_notify(
                backoff,
                || async { Ok(f().await?) },
                |err, _| {
                    if let Some(t) = tracer.as_ref() {
                        t(err)
                    }
                },
            )
            .await?;

            Ok(result)
        }
    }
}
