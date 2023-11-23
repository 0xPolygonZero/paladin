//! Provides utilities for serialization and deserialization of data.
//!
//! This module introduces traits and enums to facilitate the serialization and
//! deserialization of data in various formats. It abstracts away the underlying
//! serialization libraries and provides a unified interface for different
//! serialization formats.
//!
//! # Features:
//! - **Serializable Trait**: A shorthand trait that encapsulates common
//!   serialization and deserialization behaviors.
//! It's designed to be used in asynchronous or threaded contexts.
//! - **Serializer Enum**: Provides a generic way to serialize and deserialize
//!   binary data. It supports multiple serialization formats and can be easily
//!   extended.
//!
//! # Examples
//!
//! Serializing and deserializing using the `Serializer` enum:
//!
//! ```rust
//! use paladin::serializer::Serializer;
//! use serde::{Serialize, Deserialize};
//!
//! #[derive(Serialize, Deserialize)]
//! struct MyData {
//!    field: String,
//! }
//!
//! let data = MyData { field: "Hello, World!".to_string() };
//! let serialized = Serializer::Cbor.to_bytes(&data).unwrap();
//! let deserialized: MyData = Serializer::Cbor.from_bytes(&serialized).unwrap();
//! ```

use anyhow::Result;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use tracing::instrument;

use crate::config::{self, Config};

/// Represents a shorthand for common serialization and deserialization
/// behaviors.
///
/// This trait is designed to be used in asynchronous or threaded contexts,
/// hence the requirements for `Send`, `Sync`, and `Unpin`. As such, it's
/// recommended to use owned types for serialization and deserialization to
/// ensure compatibility with this trait.
pub trait Serializable: Serialize + DeserializeOwned + Send + Sync + Unpin + 'static {}
impl<T> Serializable for T where T: Serialize + DeserializeOwned + Send + Sync + Unpin + 'static {}

/// Provides a unified interface for serializing and deserializing binary data.
///
/// This enum abstracts away the underlying serialization libraries and offers
/// methods to serialize and deserialize data in different formats.
/// It can be easily extended to support additional serialization formats in the
/// future.
#[derive(Clone, Copy, Serialize, Deserialize, Debug, Default)]
pub enum Serializer {
    #[default]
    Postcard,
    Cbor,
}

impl std::fmt::Display for Serializer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Postcard => write!(f, "postcard"),
            Self::Cbor => write!(f, "cbor"),
        }
    }
}

impl Serializer {
    /// Serializes the given value into binary data using the specified format.
    #[instrument(skip(value), level = "trace")]
    pub fn to_bytes<T: Serialize>(&self, value: &T) -> Result<Vec<u8>> {
        match self {
            Self::Postcard => Ok(postcard::to_allocvec(value)?),
            Self::Cbor => {
                let mut result = Vec::new();
                ciborium::into_writer(value, &mut result)?;
                Ok(result)
            }
        }
    }

    /// Deserializes the given binary data into a value of the specified type
    /// using the specified format.
    #[instrument(skip(bytes), level = "trace")]
    pub fn from_bytes<T: for<'a> Deserialize<'a>>(&self, bytes: &[u8]) -> Result<T> {
        match self {
            Self::Postcard => Ok(postcard::from_bytes(bytes)?),
            Self::Cbor => Ok(ciborium::from_reader(bytes)?),
        }
    }
}

impl From<&Config> for Serializer {
    fn from(config: &Config) -> Self {
        match config.serializer {
            config::Serializer::Postcard => Self::Postcard,
            config::Serializer::Cbor => Self::Cbor,
        }
    }
}
