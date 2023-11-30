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

    fn execute(&self, input: Self::Input) -> Result<Self::Output> {
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

    fn combine(&self, a: Self::Elem, b: Self::Elem) -> Result<Self::Elem> {
        Ok(a + &b)
    }
}
