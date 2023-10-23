//! Utility for managing the routing of distributed tasks and their results.
//!
//! This module provides utilities for orchestrating distributed task execution,
//! focusing on the interplay between operations, their invocations as tasks,
//! and higher-order directives that manage these tasks.
//!
//! It provides two runtimes:
//! - [`Runtime`]: Used by the orchestrator to manage the execution of tasks.
//! - [`WorkerRuntime`]: Used by worker nodes to execute tasks.
//!
//! # Semantic Overview
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
//! Practically, this is implemented by using a single, stable channel for
//! [`Task`]s, and dynamically issuing new channels in context of each
//! [`Directive`](crate::directive::Directive)'s evaluation. This allows workers
//! to listen on a single channel and thus execute arbitrary [`Task`]s, while
//! [`Directive`](crate::directive::Directive)s can listen on an isolated
//! channel for their results.
//!
//! The [`Runtime`] is implements the semantics of this architecture.
//! Fundamentally, the [`Runtime`] is responsible for managing the channels that
//! [`Task`]s and [`Directive`](crate::directive::Directive)s use to
//! communicate, and provides a simple interface for interacting with these
//! channels.
use anyhow::Result;
use futures::{Sink, SinkExt, Stream, StreamExt};
use tokio::{task::JoinHandle, try_join};
use tracing::{debug_span, error, instrument, Instrument};

use self::dynamic_channel::{DynamicChannel, DynamicChannelFactory};
use crate::{
    acker::{Acker, ComposedAcker},
    channel::{coordinated_channel::coordinated_channel, Channel, ChannelFactory, LeaseGuard},
    config::Config,
    operation::{OpKind, Operation},
    serializer::{Serializable, Serializer},
    task::{AnyTask, AnyTaskResult, RemoteExecute, Task, TaskResult},
};

type Receiver<Item> = Box<dyn Stream<Item = (Item, Box<dyn Acker>)> + Send + Sync + Unpin>;
type Sender<Item> = Box<dyn Sink<Item, Error = anyhow::Error> + Send + Sync + Unpin>;
type CoordinatedTaskChannel<Op, Metadata> = (
    String,
    Sender<Task<Op, Metadata>>,
    LeaseGuard<DynamicChannel, Receiver<TaskResult<Op, Metadata>>>,
);

/// The core of the distributed task management system.
///
/// Fundamentally, the [`Runtime`] is responsible for managing the channels that
/// [`Task`]s and [`Directive`](crate::directive::Directive)s use to
/// communicate, and provides a simple interface for interacting with these
/// channels.
///
/// This runtime should be used in an orchestration context, where a single node
/// is responsible for managing the execution of tasks. This is in contrast to
/// the [`WorkerRuntime`], which is used by worker nodes that execute tasks.
///
/// The primary purpose of this runtime is to facilitate the instantiation of
/// new coordination channels for dispatching [`Task`]s and receiving their
/// associated [`TaskResult`]s. It takes care of namespacing new result channels
/// such that users can consume them in an isolated manner.
/// [`Directive`](crate::directive::Directive)s will use this to create a siloed
/// channel upon which they can listen to only the results of the [`Task`]s they
/// manage. It also takes care of synchronizing disparate sender and receiver
/// channels such that closing the sender terminates the receiver.
///
/// ## Emulation
/// The main [`Runtime`] provides emulation functionality for the worker. This
/// can be enabled by passing the
/// [`config::Runtime::InMemory`](crate::config::Runtime::InMemory) field as
/// part of the configuration to [`Runtime::from_config`]. This will spin up a
/// multi-threaded worker emulator that will execute multiple [`WorkerRuntime`]s
/// concurrently. This can be used to simulate a distributed environment
/// in-memory, and finds immediate practical use in writing tests.
///
/// See the [runtime module documentation](crate::runtime) for more information
/// on runtime semantics.
pub struct Runtime {
    channel_factory: DynamicChannelFactory,
    task_channel: DynamicChannel,
    serializer: Serializer,
    worker_emulator: Option<Vec<JoinHandle<Result<()>>>>,
}

impl Runtime {
    /// Initializes the [`Runtime`] with the provided [`Config`].
    pub async fn from_config<K: OpKind>(config: &Config) -> Result<Self>
    where
        AnyTask<K>: RemoteExecute<K>,
    {
        let channel_factory = DynamicChannelFactory::from_config(config).await?;
        let task_channel = channel_factory.get(&config.task_bus_routing_key).await?;
        let serializer = Serializer::from(config);

        // Spin up an emulator for the worker runtime if we're running in-memory.
        let worker_emulator = match config.runtime {
            crate::config::Runtime::InMemory => Some(Self::spawn_emulator(
                channel_factory.clone(),
                task_channel.clone(),
                None,
            )),
            _ => None,
        };

        Ok(Self {
            channel_factory,
            task_channel,
            serializer,
            worker_emulator,
        })
    }

    /// Short-hand for initializing an in-memory [`Runtime`].
    pub async fn in_memory<K: OpKind>() -> Result<Self>
    where
        AnyTask<K>: RemoteExecute<K>,
    {
        let config = Config {
            runtime: crate::config::Runtime::InMemory,
            ..Default::default()
        };
        Self::from_config::<K>(&config).await
    }

    /// Spawns an emulator for the worker runtime.
    ///
    /// This is used to emulate the worker runtime when running in-memory.
    fn spawn_emulator<K: OpKind>(
        channel_factory: DynamicChannelFactory,
        task_channel: DynamicChannel,
        num_threads: Option<usize>,
    ) -> Vec<JoinHandle<Result<()>>>
    where
        AnyTask<K>: RemoteExecute<K>,
    {
        (0..(num_threads.unwrap_or(10)))
            .map(|_| {
                let channel_factory = channel_factory.clone();
                let task_channel = task_channel.clone();
                tokio::spawn(async move {
                    let worker_runtime: WorkerRuntime<K> = WorkerRuntime {
                        channel_factory,
                        task_channel,
                        _kind: std::marker::PhantomData,
                    };
                    worker_runtime.main_loop().await?;
                    Ok(())
                })
            })
            .collect()
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
    async fn get_task_sender<Op: Operation, Metadata: Serializable>(
        &self,
    ) -> Result<Sender<Task<Op, Metadata>>> {
        // Get a sink for the task channel, which accepts `AnyTask`.
        let sink = self.task_channel.sender::<AnyTask<Op::Kind>>().await?;
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
    /// async fn run_task<Op: Operation>(op: Op, runtime: Runtime, input: Op::Input) -> Result<()> {
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
    pub async fn lease_coordinated_task_channel<Op: Operation, Metadata: Serializable>(
        &self,
    ) -> Result<CoordinatedTaskChannel<Op, Metadata>> {
        // Issue a new channel and return its identifier paired with a stream of
        // results.
        let (task_sender, (result_channel_identifier, result_channel)) =
            try_join!(self.get_task_sender(), self.channel_factory.issue())?;

        // Transform the stream to deserialize the results into typed `TaskResult`s on
        // behalf of the caller. This allows the caller to receive typed
        // `TaskResult`s without having to worry about deserialization.
        let receiver = result_channel
            .receiver::<AnyTaskResult<Op>>()
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
}

/// Drop the worker emulator when the runtime is dropped.
impl Drop for Runtime {
    fn drop(&mut self) {
        if let Some(worker_emulator) = self.worker_emulator.take() {
            for handle in worker_emulator {
                handle.abort();
            }
        }
    }
}

/// A runtime for worker nodes.
///
/// This runtime provides functionality for the three responsibilities of a
/// worker process.
/// 1. Listen for new [`Task`](crate::task::Task)s.
/// 2. Execute those [`Task`](crate::task::Task)s.
/// 3. Send back the results of a [`Task`](crate::task::Task) execution.
pub struct WorkerRuntime<Kind: OpKind> {
    channel_factory: DynamicChannelFactory,
    task_channel: DynamicChannel,
    _kind: std::marker::PhantomData<Kind>,
}

impl<Kind: OpKind> WorkerRuntime<Kind>
where
    AnyTask<Kind>: RemoteExecute<Kind>,
{
    /// Initializes the [`WorkerRuntime`] with the provided [`Config`].
    pub async fn from_config(config: &Config) -> Result<Self> {
        let channel_factory = DynamicChannelFactory::from_config(config).await?;
        let task_channel = channel_factory.get(&config.task_bus_routing_key).await?;

        Ok(Self {
            channel_factory,
            task_channel,
            _kind: std::marker::PhantomData,
        })
    }

    /// Provides a [`Sink`] for dispatching [`AnyTaskResult`]s.
    ///
    /// Typically used by a worker node for send back the results of a [`Task`]
    /// execution.
    ///
    /// The [`opkind_derive::OpKind`] macro uses this internally to provide a
    /// [`RemoteExecute`] implementation for [`AnyTask`].
    #[instrument(skip(self), level = "debug")]
    pub async fn get_result_sender<Op: Operation>(
        &self,
        identifier: &str,
    ) -> Result<Sender<AnyTaskResult<Op>>> {
        self.channel_factory.get(identifier).await?.sender().await
    }

    /// Provides a [`Stream`] incoming [`AnyTask`]s.
    ///
    /// Typically the the worker node's first interaction with the [`Runtime`].
    /// This is how workers receive [`Task`]s for remote execution.
    ///
    /// # Example
    /// ```no_run
    /// use clap::Parser;
    /// use paladin::{config::Config, runtime::WorkerRuntime, operation::Operation, opkind_derive::OpKind};
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
    ///     let runtime: WorkerRuntime<MyOps> = WorkerRuntime::from_config(&args.options).await?;
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

    /// A default worker loop that can be used to process
    /// [`Task`](crate::task::Task)s.
    ///
    /// Worker implementations generally wont vary, as the their
    /// primary responsibility is to process incoming tasks. We provide one
    /// out of the box that will work for most use cases. Users are free to
    /// implement their own if they need to.
    ///
    /// # Example
    /// ```no_run
    /// use anyhow::Result;
    /// use paladin::{
    ///     runtime::WorkerRuntime,
    ///     config::Config,
    ///     task::Task,
    ///     operation::Operation,
    ///     opkind_derive::OpKind,
    /// };
    /// use clap::Parser;
    /// use serde::{Deserialize, Serialize};
    ///
    /// #[derive(OpKind, Serialize, Deserialize, Debug, Clone, Copy)]
    /// enum MyOps {
    ///     // ... your operations
    /// #   StringLength(StringLength),
    /// }
    ///
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
    /// #[derive(Parser, Debug)]
    /// pub struct Cli {
    ///     #[command(flatten)]
    ///     pub options: Config,
    /// }
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<()> {
    ///     let args = Cli::parse();
    ///     let runtime = WorkerRuntime::from_config(&args.options).await?;
    ///     runtime.main_loop().await?;
    ///
    ///     Ok(())
    /// }
    /// ```
    pub async fn main_loop(&self) -> Result<()> {
        let mut task_stream = self.get_task_receiver().await?;

        while let Some((payload, delivery)) = task_stream.next().await {
            let span = debug_span!("remote_execute", routing_key = %payload.routing_key, op = ?payload.op_kind);

            let execution = {
                payload.remote_execute(self).instrument(span).await?;

                delivery.ack().await?;

                Ok::<(), anyhow::Error>(())
            };

            if let Err(e) = execution {
                error!("Failed to process task {e}");
            }
        }

        Ok(())
    }
}

mod dynamic_channel;
