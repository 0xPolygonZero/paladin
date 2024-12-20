use std::thread::sleep;

use tracing::info;
use paladin::operation::{FatalStrategy, OperationError};
use paladin::{operation::{Monoid, Operation, Result}, registry, AbortSignal, RemoteExecute};
use serde::{Deserialize, Serialize};

registry!();

#[derive(Serialize, Deserialize, RemoteExecute)]
pub struct CharToString;

impl Operation for CharToString {
    type Input = char;
    type Output = String;

    fn execute(
        &self,
        input: Self::Input,
        abort: AbortSignal,
    ) -> Result<Self::Output> {
        for i in 1..10 {
            // Simulate some long job. Check occasionally for the abort signal
            // to terminate the job prematurely
            sleep(std::time::Duration::from_millis(100));
            if abort.is_some()
                && abort
                    .as_ref()
                    .unwrap()
                    .load(std::sync::atomic::Ordering::SeqCst)
            {
                return Err(OperationError::Fatal {
                    err: anyhow::anyhow!("aborted per request at CharToString iteration {i} for input {input:?}"),
                    strategy: FatalStrategy::Terminate,
                });
            }
        }
        info!("CharToString operation finished for input: {:?}", input);
        Ok(input.to_string())
    }
}

#[derive(Serialize, Deserialize, RemoteExecute)]
pub struct StringConcat;

impl Monoid for StringConcat {
    type Elem = String;

    fn empty(&self) -> Self::Elem {
        String::new()
    }

    fn combine(
        &self,
        a: Self::Elem,
        b: Self::Elem,
        _abort: AbortSignal,
    ) -> Result<Self::Elem> {
        Ok(a + &b)
    }
}
