use std::{fmt::Display, str::FromStr};

/*
use askama::Template;
use crate::input_args::ErrorHandling;

#[derive(Template)]
#[template(path = "generated_type.rs", escape = "none")]
pub struct CsvTemplate {
    delimiter: &'static str,
    columns: Vec<(usize, String, ColumnType)>,
    error_handling: ErrorHandling,
}
*/

#[derive(PartialEq)]
pub enum ColumnType {
    Unit,
    Bool(bool),
    I8(bool),
    I16(bool),
    I32(bool),
    I64(bool),
    U8(bool),
    U16(bool),
    U32(bool),
    U64(bool),
    F64(bool),
    String(bool),
}

impl Display for ColumnType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl ColumnType {
    fn as_str(&self) -> &'static str {
        match self {
            ColumnType::Unit => "()",
            ColumnType::Bool(false) => "bool",
            ColumnType::Bool(true) => "Option<bool>",
            ColumnType::I8(false) => "i8",
            ColumnType::I8(true) => "Option<i8>",
            ColumnType::I16(false) => "i16",
            ColumnType::I16(true) => "Option<i16>",
            ColumnType::I32(false) => "i32",
            ColumnType::I32(true) => "Option<i32>",
            ColumnType::I64(false) => "i64",
            ColumnType::I64(true) => "Option<i64>",
            ColumnType::U8(false) => "u8",
            ColumnType::U8(true) => "Option<u8>",
            ColumnType::U16(false) => "u16",
            ColumnType::U16(true) => "Option<u16>",
            ColumnType::U32(false) => "u32",
            ColumnType::U32(true) => "Option<u32>",
            ColumnType::U64(false) => "u64",
            ColumnType::U64(true) => "Option<u64>",
            ColumnType::F64(false) => "f64",
            ColumnType::F64(true) => "Option<f64>",
            ColumnType::String(false) => "String",
            ColumnType::String(true) => "Option<String>",
        }
    }

    pub fn is_parseable(&self) -> bool {
        !matches!(self, ColumnType::Unit | ColumnType::String(_))
        /*
        match self {
            ColumnType::Unit => false,
            ColumnType::String(_) => false,
            _ => true,
        }
        */
    }

    pub fn is_optional(&self) -> bool {
        match self {
            ColumnType::Unit => false,
            ColumnType::Bool(b) => *b,
            ColumnType::I8(b) => *b,
            ColumnType::I16(b) => *b,
            ColumnType::I32(b) => *b,
            ColumnType::I64(b) => *b,
            ColumnType::U8(b) => *b,
            ColumnType::U16(b) => *b,
            ColumnType::U32(b) => *b,
            ColumnType::U64(b) => *b,
            ColumnType::F64(b) => *b,
            ColumnType::String(b) => *b,
        }
    }
}

#[derive(Copy, Clone, PartialEq, Debug)]
pub enum IntermediateColumnType {
    Unknown(bool),
    Bool(bool),
    /// Stores the minimum and maximum integer values present
    Integer(i128, i128, bool),
    Float(bool),
    String(bool),
}

impl Default for IntermediateColumnType {
    fn default() -> Self {
        Self::Unknown(false)
    }
}

impl IntermediateColumnType {
    fn set_optional(self) -> Self {
        match self {
            IntermediateColumnType::Unknown(_) => IntermediateColumnType::Bool(true),
            IntermediateColumnType::Bool(_) => IntermediateColumnType::Bool(true),
            IntermediateColumnType::Integer(min, max, _) => {
                IntermediateColumnType::Integer(min, max, true)
            }
            IntermediateColumnType::Float(_) => IntermediateColumnType::Float(true),
            IntermediateColumnType::String(_) => IntermediateColumnType::String(true),
        }
    }

    pub(crate) fn agg(&mut self, input_value: &str) {
        let input_type: IntermediateColumnType = input_value.parse().unwrap();

        match (&self, input_type) {
            // If one is a string, then we must already know it doesn't parse to something more specific
            (IntermediateColumnType::String(true), _) => {}
            (_, IntermediateColumnType::String(true)) => {
                *self = IntermediateColumnType::String(true);
            }
            (IntermediateColumnType::String(false), _)
            | (_, IntermediateColumnType::String(false)) => {
                *self = IntermediateColumnType::String(input_value.is_empty());
            }

            // First iteration with an unknown column, always keep the other
            (IntermediateColumnType::Unknown(false), it) => *self = it,
            (_, IntermediateColumnType::Unknown(false)) => {}

            // When one is unknown, keep the known value, but it is now known to be nullable
            (IntermediateColumnType::Unknown(true), it) => *self = it.set_optional(),
            (s, IntermediateColumnType::Unknown(true)) => *self = s.set_optional(),

            // When types are identical:
            (IntermediateColumnType::Bool(so), IntermediateColumnType::Bool(oo)) => {
                *self = IntermediateColumnType::Bool(*so || oo)
            }
            (IntermediateColumnType::Float(so), IntermediateColumnType::Float(oo)) => {
                *self = IntermediateColumnType::Float(*so || oo)
            }

            (
                IntermediateColumnType::Integer(self_min, self_max, so),
                IntermediateColumnType::Integer(other_min, other_max, oo),
            ) => {
                *self = IntermediateColumnType::Integer(
                    *self_min.min(&other_min),
                    *self_max.max(&other_max),
                    *so || oo,
                )
            }

            // Always take floats over ints
            (
                IntermediateColumnType::Integer(_, _, self_optional),
                IntermediateColumnType::Float(other_optional),
            ) => *self = IntermediateColumnType::Float(*self_optional || other_optional),
            (
                IntermediateColumnType::Float(self_optional),
                IntermediateColumnType::Integer(_, _, other_optional),
            ) => *self = IntermediateColumnType::Float(*self_optional || other_optional),

            // All other cases result in a string
            (IntermediateColumnType::Bool(so), IntermediateColumnType::Integer(_, _, oo))
            | (IntermediateColumnType::Bool(so), IntermediateColumnType::Float(oo))
            | (IntermediateColumnType::Integer(_, _, so), IntermediateColumnType::Bool(oo))
            | (IntermediateColumnType::Float(so), IntermediateColumnType::Bool(oo)) => {
                *self = IntermediateColumnType::String(*so || oo);
            }
        }
    }

    pub(crate) fn finalize(self) -> ColumnType {
        match self {
            IntermediateColumnType::Unknown(_) => ColumnType::Unit,
            IntermediateColumnType::Bool(b) => ColumnType::Bool(b),
            IntermediateColumnType::Float(b) => ColumnType::F64(b),
            IntermediateColumnType::String(b) => ColumnType::String(b),
            IntermediateColumnType::Integer(min, max, b) if min >= 0 => {
                // unsigned values
                if max <= u8::MAX as i128 {
                    ColumnType::U8(b)
                } else if max <= u16::MAX as i128 {
                    ColumnType::U16(b)
                } else if max <= u32::MAX as i128 {
                    ColumnType::U32(b)
                } else if max <= u64::MAX as i128 {
                    ColumnType::U64(b)
                } else {
                    // For now, we'll treat u128s as strings
                    ColumnType::String(b)
                }
            }
            IntermediateColumnType::Integer(min, max, b) => {
                // signed values
                if i8::MIN as i128 <= min && max <= i8::MAX as i128 {
                    ColumnType::I8(b)
                } else if i16::MIN as i128 <= min && max <= i16::MAX as i128 {
                    ColumnType::I16(b)
                } else if i32::MIN as i128 <= min && max <= i32::MAX as i128 {
                    ColumnType::I32(b)
                } else if i64::MIN as i128 <= min && max <= i64::MAX as i128 {
                    ColumnType::I64(b)
                } else {
                    // For now, we'll treat all other ints as strings
                    ColumnType::String(b)
                }
            }
        }
    }
}

impl FromStr for IntermediateColumnType {
    // All values will 'parse' -- default is a String
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.is_empty() {
            Ok(IntermediateColumnType::Unknown(true))
        } else if let Ok(i) = s.parse::<i128>() {
            Ok(IntermediateColumnType::Integer(i, i, false))
        } else if s.parse::<f64>().is_ok() {
            Ok(IntermediateColumnType::Float(false))
        } else if s.to_lowercase().parse::<bool>().is_ok() {
            Ok(IntermediateColumnType::Bool(false))
        } else {
            Ok(IntermediateColumnType::String(false))
        }
    }
}
