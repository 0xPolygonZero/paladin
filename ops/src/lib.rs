//! Sample ops for demo and testing purposes.

use std::{fmt::Debug, ops::Mul};

use anyhow::Result;
use num_traits::One;
use paladin::{
    operation::{Monoid, Operation},
    opkind_derive::OpKind,
    serializer::Serializable,
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
pub struct StringLength;

impl Operation for StringLength {
    type Input = String;
    type Output = usize;
    type Kind = Ops;
    fn execute(&self, input: Self::Input) -> Result<Self::Output> {
        Ok(input.len())
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

#[derive(Serialize, Deserialize, Debug, Clone, Copy)]
pub struct MultiplyBy(pub i32);

impl Operation for MultiplyBy {
    type Input = i32;
    type Output = i32;
    type Kind = Ops;
    fn execute(&self, input: Self::Input) -> Result<Self::Output> {
        Ok(input * self.0)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy)]
pub struct Sum;

impl Monoid for Sum {
    type Elem = i32;
    type Kind = Ops;
    fn empty(&self) -> Self::Elem {
        0
    }

    fn combine(&self, a: Self::Elem, b: Self::Elem) -> Result<Self::Elem> {
        Ok(a + b)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy, Default)]
pub struct GenericMultiplication<T>(std::marker::PhantomData<T>);

impl<T: Mul<Output = T> + One + Serializable + Debug + Clone> Monoid for GenericMultiplication<T>
where
    Ops: std::convert::From<GenericMultiplication<T>>,
{
    type Elem = T;
    type Kind = Ops;

    fn empty(&self) -> Self::Elem {
        T::one()
    }

    fn combine(&self, a: Self::Elem, b: Self::Elem) -> Result<Self::Elem> {
        Ok(a * b)
    }
}

#[derive(OpKind, Debug, Serialize, Deserialize, Clone, Copy)]
pub enum Ops {
    CharToString(CharToString),
    StringLength(StringLength),
    StringConcat(StringConcat),
    MultiplyBy(MultiplyBy),
    Sum(Sum),
    GenericMultiplicationI32(GenericMultiplication<i32>),
    GenericMultiplicationI64(GenericMultiplication<i64>),
}
