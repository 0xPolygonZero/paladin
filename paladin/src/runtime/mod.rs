//! Utility for managing the routing of distributed tasks and their results.
//!
//! This module provides utilities for orchestrating distributed task execution,
//! focusing on the interplay between operations, their invocations as tasks,
//! and higher-order directives that manage these tasks.
//!
//! - [`Operation`]: Defines the signature and semantics of a computation. Akin
//!   to a function that maps an input to an output.
//! - [`Task`]: Represents the invocation of an operation with specific
//!   arguments. The payloads used for coordination.
//! - [`Directive`](crate::directive::Directive): Manages and orchestrates the
//!   results of multiple tasks, implementing higher-order semantics.
//!
//! In the context of our system, we assume:
//! - [`Operation`]s are expensive as they usually encode hefty computations.
//! - [`Directive`](crate::directive::Directive)s are cheap since they primarily
//!   manage and coordinate the results of those computations.
//!
//! A key concept is the information asymmetry between
//! [`Directive`](crate::directive::Directive)s and [`Task`]s:
//! - A [`Directive`](crate::directive::Directive) knows about the [`Task`]s it
//!   manages.
//! - A [`Task`], however, is oblivious to any
//!   [`Directive`](crate::directive::Directive) overseeing it.
//!
//! This asymmetry highlights distinct channeling requirements:
//! - [`Directive`](crate::directive::Directive)s require a discrete homogenous
//!   stream of results.
//! - [`Task`] channels must accommodate arbitrary operations, listening on a
//!   single, heterogenous stream of results.
//!
//! The [`Runtime`] is implements the semantics of this architecture.
//! Fundamentally, the [`Runtime`] is responsible for managing the channels that
//! [`Task`]s and [`Directive`](crate::directive::Directive)s use to
//! communicate, and provides a simple interface for interacting with these
//! channels.
use crate::{
    acker::{Acker, ComposedAcker},
    channel::{
        coordinated_channel::coordinated_channel,
        dynamic::{DynamicChannel, DynamicChannelFactory},
        Channel, ChannelFactory, LeaseGuard,
    },
    operation::{OpKind, Operation},
    serializer::{Serializable, Serializer},
    task::{AnyTask, AnyTaskResult, Task, TaskResult},
};
use anyhow::Result;
use futures::{Sink, SinkExt, Stream, StreamExt};
use tokio::try_join;
use tracing::{error, instrument};

type Receiver<Item> = Box<dyn Stream<Item = (Item, Box<dyn Acker>)> + Send + Unpin>;
type Sender<Item> = Box<dyn Sink<Item, Error = anyhow::Error> + Send + Unpin>;
type CoordinatedTaskChannel<Op, Metadata> = (
    String,
    Sender<Task<Op, Metadata>>,
    LeaseGuard<
        AnyTaskResult<Op>,
        DynamicChannel<AnyTaskResult<Op>>,
        Receiver<TaskResult<Op, Metadata>>,
    >,
);

/// The core of the distributed task management system.
///
/// Fundamentally, the [`Runtime`] is responsible for managing the channels that
/// [`Task`]s and [`Directive`](crate::directive::Directive)s use to
/// communicate, and provides a simple interface for interacting with these
/// channels.
///
/// See the [runtime module documentation](crate::runtime) for more information
/// on runtime semantics.
pub struct Runtime<Kind: OpKind> {
    channel_factory: DynamicChannelFactory,
    task_channel: DynamicChannel<AnyTask<Kind>>,
    serializer: Serializer,
    _kind: std::marker::PhantomData<Kind>,
}

impl<Kind: OpKind> Runtime<Kind> {
    /// Initializes the [`Runtime`].
    pub async fn initialize(
        channel_factory: DynamicChannelFactory,
        task_bus_routing_key: &str,
        serializer: Serializer,
    ) -> Result<Self> {
        let task_channel = channel_factory.get(task_bus_routing_key).await?;
        Ok(Self {
            channel_factory,
            task_channel,
            serializer,
            _kind: std::marker::PhantomData,
        })
    }

    /// Provides a [`Stream`] incoming [`AnyTask`]s.
    ///
    /// Typically the the worker node's first interaction with the [`Runtime`].
    /// This is how workers receive [`Task`]s for remote execution.
    ///
    /// # Example
    /// ```no_run
    /// use clap::Parser;
    /// use paladin::{config::Config, get_runtime, operation::Operation, opkind_derive::OpKind};
    /// use serde::{Deserialize, Serialize};
    /// use anyhow::Result;
    /// use futures::StreamExt;
    ///
    /// # #[derive(Serialize, Deserialize, Debug, Clone, Copy)]
    /// # struct StringLength;
    /// #
    /// # impl Operation for StringLength {
    /// #    type Input = String;
    /// #    type Output = usize;
    /// #    type Kind = MyOps;
    /// #    
    /// #    fn execute(&self, input: Self::Input) -> Result<Self::Output> {
    /// #        Ok(input.len())
    /// #    }
    /// # }
    /// #
    /// #[derive(OpKind, Serialize, Deserialize, Debug, Clone, Copy)]
    /// enum MyOps {
    ///     // ... your operations
    /// #   StringLength(StringLength),
    /// }
    ///
    /// #[derive(Parser, Debug)]
    /// pub struct Cli {
    ///     #[command(flatten)]
    ///     pub options: Config,
    /// }
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<()> {
    ///     let args = Cli::parse();
    ///     let runtime = get_runtime::<MyOps>(&args.options).await?;
    ///     
    ///     let mut task_stream = runtime.get_task_receiver().await?;
    ///     while let Some((task, delivery)) = task_stream.next().await {
    ///         // ... handle task   
    ///     }
    /// #  Ok(())
    /// }
    /// ```
    #[instrument(skip_all, level = "debug")]
    pub async fn get_task_receiver(&self) -> Result<Receiver<AnyTask<Kind>>> {
        self.task_channel.receiver().await
    }

    /// Provides a [`Sink`] for dispatching [`Task`]s of the specified
    /// [`Operation`] and its associated `Metadata`.
    ///
    /// This generally shouldn't be used by itself if the caller is interested
    /// in receiving the results associated with dispatched [`Task`]s.
    /// Instead, consider using
    /// [`lease_coordinated_task_channel`](Self::lease_coordinated_task_channel),
    /// which uses this function internally and provides additional
    /// coordination guarantees.
    ///
    /// # Metadata
    /// `Metadata` is a generic parameter that implements [`Serializable`], used
    /// to encode additional information into a [`Task`] that is relevant to
    /// the particular [`Directive`](crate::directive::Directive) orchestrating
    /// it. This can be useful in cases where a
    /// [`Directive`](crate::directive::Directive) needs to coordinate the
    /// results of multiple [`Task`]s.
    #[instrument(skip_all, level = "debug")]
    pub async fn get_task_sender<Op: Operation<Kind = Kind>, Metadata: Serializable>(
        &self,
    ) -> Result<Sender<Task<Op, Metadata>>> {
        // Get a sink for the task channel, which accepts `AnyTask`.
        let sink = self.task_channel.sender().await?;
        let serializer = self.serializer;
        // Transform the sink to accept typed `Task`s by serializing them into
        // `AnyTask`s on behalf of the caller. This allows the caller to pass in
        // a typed `Task` without having to worry about serialization.
        let transformed_sink = sink.with(move |task: Task<Op, Metadata>| {
            Box::pin(async move { task.into_any_task(serializer) })
        });

        Ok(Box::new(transformed_sink))
    }

    /// Leases a new discrete
    /// [coordinated](crate::channel::coordinated_channel::coordinated_channel)
    /// channel for asynchronously dispatching [`Task`]s and receiving their
    /// [`TaskResult`]s.
    ///
    /// # Design notes
    ///
    /// - The returned receiver is wrapped in a [`LeaseGuard`] that will release
    ///   the leased [`Channel`] once dropped.
    /// - A unique identifier is returned for the newly leased [`Channel`] that
    ///   is bound to the receiver. This can be used to route [`TaskResult`]s
    ///   back to that channel such that they're propagated to provided
    ///   receiver.
    /// - Even though the `sender` and `receiver` interact with two distinct
    ///   channels, they are bound by
    ///   [`coordinated_channel`](crate::channel::coordinated_channel::coordinated_channel),
    ///   which ensures that open/closed state and pending message state are
    ///   synchronized between them.
    /// - The provided receiver automatically deserializes [`AnyTaskResult`]s
    ///   into typed [`TaskResult`]s on behalf of the caller.
    ///
    /// Users should take care to close the sender once all tasks have been
    /// dispatched. This will ensure that the receiver is terminated once
    /// all results have been received. Without this, the receiver will block
    /// indefinitely. Alternatively, returning early without closing the
    /// sender will will be fine too, as the sender will be closed once dropped.
    ///
    /// # Example
    /// ```
    /// use anyhow::Result;
    /// use paladin::{runtime::Runtime, task::Task, operation::Operation, opkind_derive::OpKind};
    /// use serde::{Deserialize, Serialize};
    /// use futures::{StreamExt, SinkExt};
    ///
    /// #[derive(OpKind, Serialize, Deserialize, Debug, Clone, Copy)]
    /// enum MyOps {
    ///     // ... your operations
    /// #   StringLength(StringLength),
    /// }
    ///
    /// #[derive(Serialize, Deserialize)]
    /// struct Metadata {
    ///     id: usize,
    /// }
    /// # #[derive(Serialize, Deserialize, Debug, Clone, Copy)]
    /// # struct StringLength;
    /// # impl Operation for StringLength {
    /// #    type Input = String;
    /// #    type Output = usize;
    /// #    type Kind = MyOps;
    /// #    
    /// #    fn execute(&self, input: Self::Input) -> Result<Self::Output> {
    /// #       Ok(input.len())
    /// #    }
    /// # }
    ///
    /// async fn run_task<Op: Operation<Kind = MyOps>>(op: Op, runtime: Runtime<MyOps>, input: Op::Input) -> Result<()> {
    ///     let (identifier, mut sender, mut receiver) = runtime.lease_coordinated_task_channel::<Op, Metadata>().await?;
    ///
    ///     // Issue a task with the identifier of the receiver
    ///     sender.send(Task {
    ///         routing_key: identifier,
    ///         metadata: Metadata { id: 0 },
    ///         op,
    ///         input,
    ///     })
    ///     .await?;
    ///     sender.close().await?;
    ///
    ///
    ///     while let Some((result, acker)) = receiver.next().await {
    ///             // ... handle result
    ///     }
    ///
    ///     Ok(())
    /// }
    /// ```
    ///
    /// # Metadata
    /// `Metadata` is a generic parameter that implements [`Serializable`], used
    /// to encode additional information into a [`Task`] that is relevant to
    /// the particular [`Directive`](crate::directive::Directive) orchestrating
    /// it. This can be useful in cases where a
    /// [`Directive`](crate::directive::Directive) needs to coordinate the
    /// results of multiple [`Task`]s.
    #[instrument(skip_all, level = "debug")]
    pub async fn lease_coordinated_task_channel<
        Op: Operation<Kind = Kind>,
        Metadata: Serializable,
    >(
        &self,
    ) -> Result<CoordinatedTaskChannel<Op, Metadata>> {
        // Issue a new channel and return its identifier paired with a stream of
        // results.
        let (task_sender, (result_channel_identifier, result_channel)) = try_join!(
            self.get_task_sender(),
            self.channel_factory.issue::<AnyTaskResult<Op>>()
        )?;

        // Transform the stream to deserialize the results into typed `TaskResult`s on
        // behalf of the caller. This allows the caller to receive typed
        // `TaskResult`s without having to worry about deserialization.
        let receiver = result_channel
            .receiver()
            .await?
            .filter_map(move |(result, acker)| {
                Box::pin(async move {
                    match result.into_task_result::<Metadata>() {
                        Ok(result) => Some((result, acker)),
                        Err(e) => {
                            error!("Failed to deserialize result: {}", e);
                            None
                        }
                    }
                })
            });

        // Enable coordination between the task and result channels.
        let (sender, receiver) = coordinated_channel(task_sender, receiver);

        // The stream returned by `coordinated_channel` has its own `Acker` that is used
        // to keep track of the number of successfully processed results. This
        // is done to ensure the Stream can be properly drained when a Sender is closed.
        //
        // We wrap the original receiver's `Acker` in a `ComposedAcker` that will also
        // ack the coordinated Stream if it is successful. This prevents
        // out-of-sync issues where the `CoordinatedStream` removes a pending send from
        // its state, but the original message failed to ack.
        let ack_composed_receiver =
            receiver.map(|((result, original_acker), coordinated_acker)| {
                (
                    result,
                    Box::new(ComposedAcker::new(original_acker, move || {
                        coordinated_acker.ack()
                    })) as Box<dyn Acker>,
                )
            });

        Ok((
            result_channel_identifier,
            Box::new(sender),
            // Wrap the stream in a `LeaseGuard` that will release the channel once dropped.
            LeaseGuard::new(result_channel, Box::new(ack_composed_receiver)),
        ))
    }

    /// Provides a [`Sink`] for dispatching [`AnyTaskResult`]s.
    ///
    /// Typically used by a worker node for send back the results of a [`Task`]
    /// execution.
    #[instrument(skip(self), level = "debug")]
    pub async fn get_result_sender<Op: Operation>(
        &self,
        identifier: &str,
    ) -> Result<Sender<AnyTaskResult<Op>>> {
        self.channel_factory.get(identifier).await?.sender().await
    }
}

pub mod worker;
