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
use futures::{Stream, StreamExt};
use uuid::Uuid;

use crate::{
    acker::Acker,
    channel::{
        queue::{QueueChannel, QueueChannelFactory},
        Channel, ChannelFactory, ChannelType,
    },
    config::{self, Config},
    queue::{
        amqp::{AMQPConnection, AMQPConnectionOptions},
        in_memory::InMemoryConnection,
        Publisher,
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
    type Sender<'a, T: Serializable + 'a> = Box<dyn Publisher<T> + Send + Unpin + Sync + 'a>;
    type Receiver<'a, T: Serializable + 'a> =
        Box<dyn Stream<Item = (T, Self::Acker)> + Send + Unpin + 'a>;

    async fn close(&self) -> Result<()> {
        match self {
            Self::Amqp(channel) => channel.close().await,
            Self::InMemory(channel) => channel.close().await,
        }
    }

    async fn sender<'a, T: Serializable + 'a>(&self) -> Result<Self::Sender<'a, T>> {
        match self {
            Self::Amqp(channel) => Ok(Box::new(channel.sender().await?)),
            Self::InMemory(channel) => Ok(Box::new(channel.sender().await?)),
        }
    }

    async fn receiver<'a, T: Serializable + 'a>(&self) -> Result<Self::Receiver<'a, T>> {
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

    fn release(&self) {
        match self {
            Self::Amqp(channel) => {
                channel.release();
            }
            Self::InMemory(channel) => {
                channel.release();
            }
        }
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

    async fn get(&self, identifier: Uuid, channel_type: ChannelType) -> Result<DynamicChannel> {
        match self {
            Self::Amqp(factory) => Ok(DynamicChannel::Amqp(
                factory.get(identifier, channel_type).await?,
            )),
            Self::InMemory(factory) => Ok(DynamicChannel::InMemory(
                factory.get(identifier, channel_type).await?,
            )),
        }
    }

    async fn issue(&self, channel_type: ChannelType) -> Result<(Uuid, DynamicChannel)> {
        match self {
            Self::Amqp(factory) => {
                let (identifier, channel) = factory.issue(channel_type).await?;
                Ok((identifier, DynamicChannel::Amqp(channel)))
            }
            Self::InMemory(factory) => {
                let (identifier, channel) = factory.issue(channel_type).await?;
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
                let connection = AMQPConnection::new(AMQPConnectionOptions {
                    uri: config.amqp_uri.as_ref().expect("amqp_uri is required"),
                    qos: Some(1),
                    serializer,
                })
                .await
                .context("connecting to AMQP host")?;
                let channel_factory = QueueChannelFactory::new(connection);
                Ok(DynamicChannelFactory::Amqp(channel_factory))
            }
            config::Runtime::InMemory => {
                let channel_factory = QueueChannelFactory::new(InMemoryConnection::new(serializer));
                Ok(DynamicChannelFactory::InMemory(channel_factory))
            }
        }
    }
}
