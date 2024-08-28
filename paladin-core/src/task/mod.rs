//! Task and TaskResult types.
//!
//! Tasks encode an [`Operation`] paired with arguments and additional metadata.
//! They represent the payloads used to communicate between
//! [`Runtime`](crate::runtime::Runtime)s and
//! [`WorkerRuntime`](crate::runtime::WorkerRuntime)s.
use std::fmt::Debug;

use anyhow::Result;
use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::{
    __private::OPERATIONS,
    operation::Operation,
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
#[derive(Debug)]
pub struct Task<'a, Op: Operation, Metadata: Serializable> {
    /// The routing key used to identify the
    /// [`Channel`](crate::channel::Channel) to which execution results should
    /// be sent.
    pub routing_key: String,
    /// Metadata associated with the [`Task`].
    pub metadata: Metadata,
    /// The [`Operation`] to be executed.
    pub op: &'a Op,
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
    /// The output of the [`Operation`] execution.
    pub output: Op::Output,
}

pub type TaskResult<Op, Metadata> = Result<TaskOutput<Op, Metadata>>;

/// A [`Task`] that has been serialized for remote execution.
///
/// This type is used to facilitate opaque execution of [`Operation`]s, such
/// that executors can execute arbitrary [`Operation`]s.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct AnyTask {
    /// The routing key used to identify the
    /// [`Channel`](crate::channel::Channel) to which execution results should
    /// be sent.
    pub routing_key: String,
    /// Serialized metadata associated with the [`Task`].
    pub metadata: Bytes,
    /// The serialized [`Operation`] to be executed.
    pub op: Bytes,
    /// The unique identifier of the [`Operation`] to be executed.
    pub operation_id: u8,
    /// Serialized arguments to the [`Operation`].
    pub input: Bytes,
    /// The [`Serializer`] used to serialize and deserialize the [`Operation`]
    /// arguments.
    pub serializer: Serializer,
}

/// Serialized output of a [`Task`].
#[derive(Serialize, Deserialize, Debug)]
pub struct AnyTaskOutput {
    /// Serialized metadata associated with the [`Task`].
    pub metadata: Bytes,
    /// Serialized output of the [`Operation`] execution.
    pub output: Bytes,
    /// The [`Serializer`] used to serialize and deserialize the [`Operation`].
    pub serializer: Serializer,
}

impl<Op: Operation, Metadata: Serializable> TryFrom<AnyTaskOutput> for TaskOutput<Op, Metadata> {
    type Error = anyhow::Error;

    fn try_from(
        AnyTaskOutput {
            metadata,
            output,
            serializer,
        }: AnyTaskOutput,
    ) -> Result<Self> {
        let metadata = serializer.from_bytes(&metadata)?;
        let output = serializer.from_bytes(&output)?;

        Ok(TaskOutput { metadata, output })
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

impl<'a, Op: Operation, Metadata: Serializable> Task<'a, Op, Metadata> {
    /// Convert a [`Task`] into an opaque [`AnyTask`].
    pub fn as_any_task(&self, serializer: Serializer) -> Result<AnyTask> {
        let routing_key = self.routing_key.clone();
        let metadata = serializer.to_bytes(&self.metadata)?;
        let input = serializer.to_bytes(&self.input)?;
        let op = serializer.to_bytes(self.op)?;

        Ok(AnyTask {
            routing_key,
            metadata,
            operation_id: Op::ID,
            op,
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

impl AnyTask {
    /// Opaque execution of a [`Task`].
    ///
    /// This function is used to execute arbitrary [`Operation`]s. It uses the
    /// [`RemoteExecute::ID`](crate::operation::RemoteExecute::ID) field to
    /// acquire the correct execution pointer from the [`static@OPERATIONS`]
    /// slice.
    pub async fn remote_execute(self) -> crate::operation::Result<AnyTaskOutput> {
        OPERATIONS[self.operation_id as usize](self).await
    }
}
