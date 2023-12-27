use clap::Parser;
use std::{path::PathBuf, str::FromStr};

use crate::util;

/// Specifies how errors are handled with input data, either from the underlying `csv` parser or
/// due to unexpected (ie, unparseable) input values.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ErrorHandling {
    /// Erroneous rows are silently ignored
    IgnoreRow,
    /// All rows are returned as Result<T,E>, allowing the caller to handle errors.
    Result,
    /// Errors immediately panic
    Panic,
}

impl FromStr for ErrorHandling {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let eh = match &s.to_lowercase()[..] {
            "ignore" => Self::IgnoreRow,
            "result" => Self::Result,
            "panic" => Self::Panic,
            _ => Err(format!("Unknown error handler: {s}"))?,
        };

        Ok(eh)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StringHandling {
    Owned,
    Static,
    Enum(u8),
}

impl FromStr for StringHandling {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let eh = match &s.to_lowercase()[..] {
            "owned" => Self::Owned,
            "static" => Self::Static,
            "enum" => Self::Enum(10),
            _ => Err(format!("Unknown strings handler: {s}"))?,
        };

        Ok(eh)
    }
}

#[allow(dead_code)]
#[derive(Debug, Parser)]
#[command(author, version, bin_name("cargo-generate-type"), about)]
pub struct Commands {
    #[clap(hide = true)]
    _subcommand: String,

    /// The input file we'll use to understand the type
    pub input_file: PathBuf,

    /// The name of the generated Rust struct for this type. If omitted, it will be generated from the input filename.
    #[arg(short, long)]
    pub typename: Option<String>,

    /// The output file into which generated code will be written. If it exists, it will not be overwritten.
    #[arg(short, long)]
    pub output_file: Option<PathBuf>,

    /// How many rows of input should be used to infer column types for the input file.
    #[arg(short, long, aliases=["rows"], default_value="1000")]
    pub num_rows: Option<usize>,

    /// The column delimiter
    #[arg(short, long, default_value = ",")]
    pub delimiter: char,

    /// How generated code should handle errors. Options are 'result', 'ignore', and 'panic'
    #[arg(short, long, aliases=["error"], default_value="result")]
    pub error_handling: ErrorHandling,

    /// Permits the output file to be overwritten if it exists.
    #[arg(short, long, default_value = "false")]
    pub force: bool,

    /// Indicates that no header exists on the input file
    #[arg(long, default_value = "false")]
    pub no_header: bool,

    /// How strings will be stored. Options are 'owned', 'static', and 'enum'.
    #[arg(short, long, aliases=["strings"], default_value="owned")]
    pub string_handling: StringHandling,

    /// How many individual values are recognized for 'static' or 'enum' string_handling; other values are handled as errors.
    #[arg(short, long, default_value = "20")]
    pub max_strings: Option<usize>,
}

impl Commands {
    pub(crate) const DEFAULT_NUM_ROWS: usize = 100;

    /// Determines the name of the output source file to use
    pub(crate) fn get_output_filename(&self) -> PathBuf {
        if let Some(of) = &self.output_file {
            return of.to_owned();
        }

        match &self.typename {
            Some(tn) => util::header_to_identifier(tn).to_lowercase() + ".rs",
            None => {
                util::header_to_identifier(
                    self.input_file
                        .file_stem()
                        .expect("File stem")
                        .to_str()
                        .expect("File stem from OsStr"),
                )
                .to_lowercase()
                    + ".rs"
            }
        }
        .into()
    }

    pub(crate) fn get_typename(&self) -> String {
        if let Some(t) = &self.typename {
            t.to_owned()
        } else {
            // No typename was given, so we'll generate one from the input filename
            // First, get the base filename
            let filename = self.input_file.file_stem().expect("File stem");
            let filename = filename.to_str().expect("File stem from OsStr");

            util::str_to_camel_case_identifier(filename)
            /*
                       let mut result = String::new();
                       let mut cap = true;

                       for c in filename.chars() {
                           match c {
                               '_' | ' ' => cap = true,
                               c if cap => {
                                   cap = false;
                                   c.to_uppercase().for_each(|ch| result.push(ch));
                               }
                               c => result.push(c),
                           }
                       }

                       result
            */
        }
    }
}
