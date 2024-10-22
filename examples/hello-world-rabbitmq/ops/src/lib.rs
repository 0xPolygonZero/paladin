use std::thread::sleep;

use paladin::__private::tracing::info;
use paladin::operation::{FatalStrategy, OperationError};
use paladin::{
    operation::{Monoid, Operation, Result},
    registry, RemoteExecute,
};
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
        abort_signal: Option<std::sync::Arc<std::sync::atomic::AtomicBool>>,
    ) -> Result<Self::Output> {
        for i in 1..10 {
            // Simulate some long job. Check occasionally for the abort signal
            // to terminate the job prematurely
            sleep(std::time::Duration::from_millis(100));
            if abort_signal.is_some()
                && abort_signal
                    .as_ref()
                    .unwrap()
                    .load(std::sync::atomic::Ordering::SeqCst)
            {
                return Err(OperationError::Fatal {
                    err: anyhow::anyhow!("aborted on command at CharToString iteration {i} for input {input:?}"),
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
        _abort_signal: Option<std::sync::Arc<std::sync::atomic::AtomicBool>>,
    ) -> Result<Self::Elem> {
        Ok(a + &b)
    }
}
