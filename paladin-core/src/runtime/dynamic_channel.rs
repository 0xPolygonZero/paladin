//! [`Channel`] and [`ChannelFactory`] adapters for dynamically specified
//! [`Channel`] and [`ChannelFactory`] implementations.
//!
//! [`Channel`]s and [`ChannelFactory`]s are strongly typed.
//! If implementations are to be specified by some external configuration, the
//! types cannot be known at compile time. This module provides adapters,
//! [`DynamicChannel`] and [`DynamicChannelFactory`] to accommodate for this.
//!
//! Both [`DynamicChannel`] and [`DynamicChannelFactory`] are enumerations of
//! the available implementations, which dynamically delegate to the appropriate
//! implementation.

use anyhow::{Context, Result};
use async_trait::async_trait;
use futures::{Sink, Stream, StreamExt};

use crate::{
    acker::Acker,
    channel::{
        queue::{QueueChannel, QueueChannelFactory},
        Channel, ChannelFactory,
    },
    config::{self, Config},
    queue::{
        amqp::{AMQPConnection, AMQPQueue, AMQPQueueOptions},
        in_memory::{InMemoryConnection, InMemoryQueue},
        Queue,
    },
    serializer::{Serializable, Serializer},
};

/// A [`Channel`] implementation that dynamically delegates to the given
/// implementation.
#[derive(Clone)]
pub enum DynamicChannel {
    Amqp(QueueChannel<AMQPConnection>),
    InMemory(QueueChannel<InMemoryConnection>),
}

#[async_trait]
impl Channel for DynamicChannel {
    type Acker = Box<dyn Acker>;
    type Sender<T: Serializable> = Box<dyn Sink<T, Error = anyhow::Error> + Send + Unpin>;
    type Receiver<T: Serializable> = Box<dyn Stream<Item = (T, Self::Acker)> + Send + Sync + Unpin>;

    async fn close(&self) -> Result<()> {
        match self {
            Self::Amqp(channel) => channel.close().await,
            Self::InMemory(channel) => channel.close().await,
        }
    }

    async fn sender<T: Serializable>(&self) -> Result<Self::Sender<T>> {
        match self {
            Self::Amqp(channel) => Ok(Box::new(channel.sender().await?)),
            Self::InMemory(channel) => Ok(Box::new(channel.sender().await?)),
        }
    }

    async fn receiver<T: Serializable>(&self) -> Result<Self::Receiver<T>> {
        match self {
            Self::Amqp(channel) => {
                Ok(Box::new(channel.receiver().await?.map(
                    |(payload, acker)| (payload, Box::new(acker) as Box<dyn Acker>),
                )))
            }
            Self::InMemory(channel) => {
                Ok(Box::new(channel.receiver().await?.map(
                    |(payload, acker)| (payload, Box::new(acker) as Box<dyn Acker>),
                )))
            }
        }
    }

    async fn release(&self) -> Result<()> {
        match self {
            Self::Amqp(channel) => {
                channel.release().await?;
            }
            Self::InMemory(channel) => {
                channel.release().await?;
            }
        }

        Ok(())
    }
}

/// A [`ChannelFactory`] implementation that dynamically delegates to the given
/// implementation.
#[derive(Clone)]
pub enum DynamicChannelFactory {
    Amqp(QueueChannelFactory<AMQPConnection>),
    InMemory(QueueChannelFactory<InMemoryConnection>),
}

#[async_trait]
impl ChannelFactory for DynamicChannelFactory {
    type Channel = DynamicChannel;

    async fn get(&self, identifier: &str) -> Result<DynamicChannel> {
        match self {
            Self::Amqp(factory) => Ok(DynamicChannel::Amqp(factory.get(identifier).await?)),
            Self::InMemory(factory) => Ok(DynamicChannel::InMemory(factory.get(identifier).await?)),
        }
    }

    async fn issue(&self) -> Result<(String, DynamicChannel)> {
        match self {
            Self::Amqp(factory) => {
                let (identifier, channel) = factory.issue().await?;
                Ok((identifier, DynamicChannel::Amqp(channel)))
            }
            Self::InMemory(factory) => {
                let (identifier, channel) = factory.issue().await?;
                Ok((identifier, DynamicChannel::InMemory(channel)))
            }
        }
    }
}

impl DynamicChannelFactory {
    pub async fn from_config(config: &Config) -> Result<Self> {
        let serializer = Serializer::from(config);
        match config.runtime {
            config::Runtime::Amqp => {
                let queue = AMQPQueue::new(AMQPQueueOptions {
                    uri: config.amqp_uri.as_ref().expect("amqp_uri is required"),
                    qos: Some(1),
                    serializer,
                });
                let channel_factory = QueueChannelFactory::new(
                    queue
                        .get_connection()
                        .await
                        .context("connecting to AMQP host")?,
                );
                Ok(DynamicChannelFactory::Amqp(channel_factory))
            }
            config::Runtime::InMemory => {
                let queue = InMemoryQueue::new(serializer);
                let channel_factory = QueueChannelFactory::new(queue.get_connection().await?);
                Ok(DynamicChannelFactory::InMemory(channel_factory))
            }
        }
    }
}
