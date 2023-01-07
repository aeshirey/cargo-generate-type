use clap::Parser;
use std::{path::PathBuf, str::FromStr};

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

    /// How many rows of input should be used to infer column types for the input file. Default is 100.
    #[arg(short, long, aliases=["rows"])]
    pub num_rows: Option<usize>,

    /// The column delimiter
    #[arg(short, long, default_value = ",")]
    pub delimiter: char,

    /// How generated code should handle errors. Options are 'result', 'ignore', and 'panic'
    #[arg(short, long, aliases=["error"], default_value="result")]
    pub error_handling: ErrorHandling,

    #[arg(short, long, default_value = "false")]
    pub force: bool,
}

impl Commands {
    pub(crate) const DEFAULT_NUM_ROWS: usize = 100;

    /// Determines the name of the output source file to use
    pub(crate) fn get_output_filename(&self) -> PathBuf {
        if let Some(of) = &self.output_file {
            return of.to_owned();
        }

        match &self.typename {
            Some(tn) => tn.replace(' ', "_").to_lowercase() + ".rs",
            None => {
                self.input_file
                    .file_stem()
                    .expect("File stem")
                    .to_str()
                    .expect("File stem from OsStr")
                    .replace(' ', "_")
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
        }
    }
}
