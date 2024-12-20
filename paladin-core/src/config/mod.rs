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
//! The [`Config`] struct can be passed to
//! [`Runtime::from_config`](crate::runtime::Runtime::from_config)
//! to dynamically construct a [`Runtime`](crate::runtime::Runtime) based on the
//! provided configuration.

use clap::{Args, ValueEnum};

const HELP_HEADING: &str = "Paladin options";

/// Represents the main configuration structure for the runtime.
#[derive(Args, Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Default)]
pub struct Config {
    /// Determines the serialization format to be used.
    #[arg(long, short, help_heading = HELP_HEADING, value_enum, env = "PALADIN_SERIALIZER", default_value_t = Serializer::Postcard)]
    pub serializer: Serializer,

    /// Specifies the runtime environment to use.
    #[arg(long, short, help_heading = HELP_HEADING, value_enum, env = "PALADIN_RUNTIME", default_value_t = Runtime::Amqp)]
    pub runtime: Runtime,

    /// Specifies the number of worker threads to spawn (in memory runtime
    /// only).
    #[arg(long, short, help_heading = HELP_HEADING, env = "PALADIN_NUM_WORKERS")]
    pub num_workers: Option<usize>,

    /// Provides the URI for the AMQP broker, if the AMQP runtime is selected.
    #[arg(long, help_heading = HELP_HEADING, env = "PALADIN_AMQP_URI", required_if_eq("runtime", "amqp"))]
    pub amqp_uri: Option<String>,

    /// Provides the routing key for workers to listen on, if the AMQP runtime
    /// is used in configuration.
    #[arg(long, help_heading = HELP_HEADING, env = "PALADIN_TASK_BUS_ROUTING_KEY")]
    pub task_bus_routing_key: Option<String>,
}

/// Enumerates the available serialization formats.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug, ValueEnum, Default)]
pub enum Serializer {
    #[default]
    Postcard,
    Cbor,
}

/// Enumerates the available runtime environments.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug, ValueEnum, Default)]
pub enum Runtime {
    #[default]
    Amqp,
    InMemory,
}
