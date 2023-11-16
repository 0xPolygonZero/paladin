use std::fmt::Debug;

use paladin::{
    operation::{Monoid, Operation, Result},
    opkind_derive::OpKind,
};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone, Copy)]
pub struct CharToString;

impl Operation for CharToString {
    type Input = char;
    type Output = String;
    type Kind = Ops;

    fn execute(&self, input: Self::Input) -> Result<Self::Output> {
        Ok(input.to_string())
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy)]
pub struct StringConcat;

impl Monoid for StringConcat {
    type Elem = String;
    type Kind = Ops;
    fn empty(&self) -> Self::Elem {
        String::new()
    }

    fn combine(&self, a: Self::Elem, b: Self::Elem) -> Result<Self::Elem> {
        Ok(a + &b)
    }
}

#[derive(OpKind, Debug, Serialize, Deserialize, Clone, Copy)]
pub enum Ops {
    CharToString(CharToString),
    StringConcat(StringConcat),
}
