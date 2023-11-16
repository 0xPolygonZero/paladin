//! Task and TaskResult types.
//!
//! Tasks encode an [`Operation`] paired with arguments and additional metadata.
//! They represent the payloads used to communicate between
//! [`Runtime`](crate::runtime::Runtime)s and [`WorkerRuntime`]s.
use std::fmt::Debug;

use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::{
    operation::{OpKind, Operation},
    runtime::WorkerRuntime,
    serializer::{Serializable, Serializer},
};

/// A [`Task`] encodes an [`Operation`] paired with arguments.
///
/// In addition to the [`Operation`] and its arguments, a [`Task`] also contains
/// metadata and routing information. The routing information is used to
/// identify the [`Channel`](crate::channel::Channel) to which execution results
/// should be sent.
///
/// Metadata can be any arbitrary [`Serializable`] type.
/// It's typically used by [`Directive`](crate::directive::Directive)s to encode
/// additional information about the computation.
#[derive(Serialize, Deserialize, Debug)]
#[serde(bound = "Op: Operation")]
pub struct Task<Op: Operation, Metadata: Serializable> {
    /// The routing key used to identify the
    /// [`Channel`](crate::channel::Channel) to which execution results should
    /// be sent.
    pub routing_key: String,
    /// Metadata associated with the [`Task`].
    pub metadata: Metadata,
    /// The [`Operation`] to be executed.
    pub op: Op,
    /// The arguments to the [`Operation`].
    pub input: Op::Input,
}

/// A [`TaskResult`] encodes the result of executing a [`Task`].
///
/// The [`TaskResult`] passes back whatever metadata was associated with the
/// [`Task`] that produced it.
#[derive(Serialize, Deserialize, Debug)]
#[serde(bound = "Op: Operation")]
pub struct TaskOutput<Op: Operation, Metadata: Serializable> {
    /// Metadata associated with the [`Task`] that produced this result.
    pub metadata: Metadata,
    /// The [`Operation`] that was executed.
    pub op: Op,
    /// The output of the [`Operation`] execution.
    pub output: Op::Output,
}

pub type TaskResult<Op, Metadata> = Result<TaskOutput<Op, Metadata>>;

/// A [`Task`] that has been serialized for remote execution.
///
/// This type is used to facilitate opaque execution of [`Operation`]s, such
/// that executors can execute arbitrary [`Operation`]s.
#[derive(Serialize, Deserialize, Debug)]
#[serde(bound = "Kind: OpKind")]
pub struct AnyTask<Kind: OpKind> {
    /// The routing key used to identify the
    /// [`Channel`](crate::channel::Channel) to which execution results should
    /// be sent.
    pub routing_key: String,
    /// Serialized metadata associated with the [`Task`].
    pub metadata: Vec<u8>,
    /// The [`OpKind`] of the [`Operation`] to be
    /// executed.
    pub op_kind: Kind,
    /// Serialized arguments to the [`Operation`].
    pub input: Vec<u8>,
    /// The [`Serializer`] used to serialize and deserialize the [`Operation`]
    /// arguments.
    pub serializer: Serializer,
}

/// Serialized output of a [`Task`].
#[derive(Serialize, Deserialize, Debug)]
pub struct AnyTaskOutput {
    /// Serialized metadata associated with the [`Task`].
    pub metadata: Vec<u8>,
    /// Serialized output of the [`Operation`] execution.
    pub output: Vec<u8>,
    /// Serialized [`Operation`] that was executed.
    pub op: Vec<u8>,
    /// The [`Serializer`] used to serialize and deserialize the [`Operation`].
    pub serializer: Serializer,
}

impl<Op: Operation, Metadata: Serializable> TryFrom<AnyTaskOutput> for TaskOutput<Op, Metadata> {
    type Error = anyhow::Error;

    fn try_from(
        AnyTaskOutput {
            metadata,
            output,
            op,
            serializer,
        }: AnyTaskOutput,
    ) -> Result<Self> {
        let metadata = serializer.from_bytes(&metadata)?;
        let output = serializer.from_bytes(&output)?;
        let op = serializer.from_bytes(&op)?;

        Ok(TaskOutput {
            metadata,
            op,
            output,
        })
    }
}

/// A serializable `Result` type for [`AnyTaskOutput`].
///
/// `Result` isn't serializable, so we need to wrap it in a type that is.
#[derive(Serialize, Deserialize, Debug)]
pub enum AnyTaskResult {
    Ok(AnyTaskOutput),
    Err(String),
}

impl<Op: Operation, Metadata: Serializable> Task<Op, Metadata> {
    /// Convert a [`Task`] into an opaque [`AnyTask`].
    pub fn into_any_task(self, serializer: Serializer) -> Result<AnyTask<Op::Kind>> {
        let op = self.op;
        let routing_key = self.routing_key;
        let metadata = serializer.to_bytes(&self.metadata)?;
        let input = serializer.to_bytes(&self.input)?;

        Ok(AnyTask {
            routing_key,
            metadata,
            op_kind: op.into(),
            input,
            serializer,
        })
    }
}

impl AnyTaskResult {
    /// Convert an opaque [`AnyTaskResult`] into a typed [`TaskResult`].
    pub fn into_task_result<Op: Operation, Metadata: Serializable>(
        self,
    ) -> TaskResult<Op, Metadata> {
        match self {
            Self::Ok(any_task_output) => Ok(any_task_output.try_into()?),
            Self::Err(msg) => Err(anyhow::anyhow!(msg)),
        }
    }
}

impl<Op: Operation, Metadata: Serializable> From<AnyTaskResult>
    for Result<TaskOutput<Op, Metadata>>
{
    fn from(value: AnyTaskResult) -> Self {
        value.into_task_result()
    }
}

/// Trait for opaque execution of a [`Task`].
///
/// This trait will be automatically implemented for `OpKind`s who use the the
/// [derive macro](crate::opkind_derive::OpKind). It is highly recommended that
/// you use the derive macro rather than implementing this trait manually,
/// unless you really want to do something custom.
///
/// In summary, this trait is used to execute a [`Task`] on a
/// [`WorkerRuntime`](crate::runtime::WorkerRuntime). Once the task has been
/// executed, the result should be sent back to the caller via the
/// [`Channel`](crate::channel::Channel) at the [`Task`]s identifier.
#[async_trait]
pub trait RemoteExecute<Kind: OpKind> {
    async fn remote_execute(
        &self,
        runtime: &WorkerRuntime<Kind>,
    ) -> crate::operation::Result<AnyTaskOutput>;
}
