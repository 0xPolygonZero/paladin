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
use super::{
    queue::{QueueChannel, QueueChannelFactory},
    Acker, Channel, ChannelFactory,
};
use crate::{queue::amqp::AMQPConnection, serializer::Serializable};
use anyhow::Result;
use async_trait::async_trait;
use futures::{Sink, Stream, StreamExt};

/// A [`Channel`] implementation that dynamically delegates to the given
/// implementation.
pub enum DynamicChannel {
    AMQP(QueueChannel<AMQPConnection>),
}

#[async_trait]
impl Channel for DynamicChannel {
    type Acker = Box<dyn Acker>;
    type Sender<T: Serializable> = Box<dyn Sink<T, Error = anyhow::Error> + Send + Unpin>;
    type Receiver<T: Serializable> = Box<dyn Stream<Item = (T, Self::Acker)> + Send + Unpin>;

    async fn sender<T: Serializable>(&self) -> Result<Self::Sender<T>> {
        match self {
            Self::AMQP(channel) => Ok(Box::new(channel.sender().await?)),
        }
    }

    async fn receiver<T: Serializable>(&self) -> Result<Self::Receiver<T>> {
        match self {
            Self::AMQP(channel) => {
                Ok(Box::new(channel.receiver().await?.map(
                    |(payload, acker)| (payload, Box::new(acker) as Box<dyn Acker>),
                )))
            }
        }
    }

    async fn release(&self) -> Result<()> {
        match self {
            Self::AMQP(channel) => {
                channel.release().await?;
                Ok(())
            }
        }
    }
}

/// A [`ChannelFactory`] implementation that dynamically delegates to the given
/// implementation.
pub enum DynamicChannelFactory {
    AMQP(QueueChannelFactory<AMQPConnection>),
}

#[async_trait]
impl ChannelFactory for DynamicChannelFactory {
    type Channel = DynamicChannel;

    async fn get(&self, identifier: &str) -> Result<DynamicChannel> {
        match self {
            Self::AMQP(factory) => Ok(DynamicChannel::AMQP(factory.get(identifier).await?)),
        }
    }

    async fn issue(&self) -> Result<(String, DynamicChannel)> {
        match self {
            Self::AMQP(factory) => {
                let (identifier, channel) = factory.issue().await?;
                Ok((identifier, DynamicChannel::AMQP(channel)))
            }
        }
    }
}
