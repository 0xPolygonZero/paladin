//! Evaluator implementations for [`Runtime`](crate::runtime::Runtime).

pub mod apply;
pub mod fold;
pub mod map;
pub use apply::*;
pub use fold::*;
pub use map::*;
