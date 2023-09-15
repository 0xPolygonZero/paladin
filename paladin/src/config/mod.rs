//! Shared runtime configuration.
//!
//! This module introduces types to facilitate the configuration of the runtime
//! environment.
//!
//! # Features:
//! - [`Config`]: Represents the main configuration structure. It's adorned with
//!   [`clap`] attributes to allow easy setup via command-line arguments.
//! - [`Serializer`]: Specifies the serialization format to use.
//! - [`Runtime`]: Enumerates the available runtime environments.
//!
//! # Usage:
//! Both the orchestrator and worker binaries require these configurations since
//! they both utilize the same [`Runtime`](crate::runtime::Runtime).
//! The [`Config`] struct can be passed to [`get_runtime`](crate::get_runtime)
//! to dynamically construct a [`Runtime`](crate::runtime::Runtime) based on the
//! provided configuration.

use clap::{Args, ValueEnum};

/// Represents the main configuration structure for the runtime.
#[derive(Args, Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Default)]
pub struct Config {
    /// Specifies the routing key for publishing task messages. In most cases,
    /// the default value should suffice.
    #[arg(long, short, default_value = "task")]
    pub task_bus_routing_key: String,

    /// Determines the serialization format to be used.
    #[arg(long, short, value_enum, default_value_t = Serializer::Cbor)]
    pub serializer: Serializer,

    /// Specifies the runtime environment to use.
    #[arg(long, short, value_enum, default_value_t = Runtime::AMQP)]
    pub runtime: Runtime,

    /// Provides the URI for the AMQP broker, if the AMQP runtime is selected.
    #[arg(long, env = "AMQP_URI", required_if_eq("runtime", "amqp"))]
    pub amqp_uri: Option<String>,
}

/// Enumerates the available serialization formats.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug, ValueEnum, Default)]
pub enum Serializer {
    #[default]
    Cbor,
}

/// Enumerates the available runtime environments.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug, ValueEnum, Default)]
pub enum Runtime {
    #[default]
    AMQP,
}
