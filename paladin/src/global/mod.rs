use crate::{
    channel::{dynamic::DynamicChannelFactory, queue::QueueChannelFactory},
    config::{self, Config},
    operation::OpKind,
    queue::{
        amqp::{AMQPQueue, AMQPQueueOptions},
        Queue,
    },
    runtime::Runtime,
    serializer::Serializer,
};
use anyhow::Result;

/// Constructs and returns a [`Runtime`] instance based on the provided
/// configuration.
///
/// This function abstracts away the details of setting up different runtime
/// environments and serialization formats. It uses the provided [`Config`] to
/// determine the appropriate setup and returns a ready-to-use [`Runtime`]
/// instance.
///
/// A [`Runtime`] instance is generic over an [`OpKind`]. This is what enables
/// dynamic execution behavior across all available operations. It is highly
/// recommended to use the [derive macro](crate::opkind_derive::OpKind)
/// to implement [`OpKind`] for your operation registry.
///
/// See the [runtime module documentation](crate::runtime) for more information
/// on runtime semantics.
///
/// # Usage:
/// Given a [`Config`] instance, you can easily obtain a [`Runtime`] instance
/// tailored to the specified configuration.
///
/// ## Examples:
///
/// Setting up the runtime based on a configuration:
///
/// ```no_run
/// use paladin::{get_runtime, config::Config, operation::Operation, opkind_derive::OpKind};
/// use serde::{Deserialize, Serialize};
/// use anyhow::Result;
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
/// # #[tokio::main]
/// # async fn main() -> Result<()> {
/// let config = Config {
///     # runtime: paladin::config::Runtime::AMQP,
///     # serializer: paladin::config::Serializer::Cbor,
///     # task_bus_routing_key: "task".to_string(),
///     # amqp_uri: Some("amqp://localhost:5672".to_string()),
///     // ... populate the config fields ...
/// };
/// let runtime = get_runtime::<MyOps>(&config).await?;
/// # Ok(())
/// # }
pub async fn get_runtime<Kind: OpKind>(config: &Config) -> Result<Runtime<Kind>> {
    let serializer = match config.serializer {
        config::Serializer::Cbor => Serializer::Cbor,
    };

    let channel_factory = match config.runtime {
        config::Runtime::AMQP => {
            let queue = AMQPQueue::new(AMQPQueueOptions {
                uri: config.amqp_uri.as_ref().expect("amqp_uri is required"),
                qos: Some(1),
                serializer,
            });
            let channel_factory = QueueChannelFactory::new(queue.get_connection().await?);
            DynamicChannelFactory::AMQP(channel_factory)
        }
    };

    Runtime::initialize(channel_factory, &config.task_bus_routing_key, serializer).await
}
