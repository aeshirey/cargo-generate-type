use crate::{
    column::{ColumnType, IntermediateColumnType},
    err::TypeGenErrors,
    input_args::{Commands, ErrorHandling},
};
use std::{
    fs::File,
    io::{BufWriter, Write},
};

#[derive(Debug)]
struct CsvColumnInfo {
    column_docs: Vec<String>,
    name: String,
    r#type: ColumnType,
}

#[derive(Debug)]
pub struct CsvFileInfo {
    args: crate::Commands,
    struct_docs: Vec<String>,
    columns: Vec<CsvColumnInfo>,
}

impl CsvFileInfo {
    pub fn new(args: crate::Commands) -> Self {
        CsvFileInfo {
            args,
            struct_docs: Vec::new(),
            columns: Vec::new(),
        }
    }

    /// Analyzes the input column types and returns the best guess for each type
    /// along with the name of each columns.
    ///
    /// If a header row is present, the values will be used and cleaned for use as
    /// a Rust identifier. If not, values will be generated from `column_0`,
    /// `column_1`, and so on.
    pub fn analyze_input(mut self) -> Result<Self, TypeGenErrors> {
        //Result<Vec<(String, ColumnType)>, TypeGenErrors> {
        let mut reader = csv::ReaderBuilder::new()
            .has_headers(!self.args.no_header)
            .delimiter(self.args.delimiter as u8)
            .flexible(true)
            .from_path(&self.args.input_file)?;

        let columns = if self.args.no_header {
            // We don't know what columns we have, so we'll read the first column:
            let mut record = Default::default();
            reader.read_record(&mut record)?;

            // Reset the position of the internal buffer:
            reader.seek(csv::Position::new())?;

            // Create placeholder column names
            (0..record.len()).map(|i| format!("column_{i}")).collect()
        } else {
            reader
                .headers()?
                .iter()
                .map(crate::util::header_to_identifier)
                .collect::<Vec<_>>()
        };

        let mut intermediates = (0..columns.len())
            .map(|_| IntermediateColumnType::default())
            .collect::<Vec<_>>();

        let num_rows = match self.args.num_rows {
            Some(0) => usize::MAX,
            Some(n) => n,
            None => Commands::DEFAULT_NUM_ROWS,
        };

        for row in reader.into_records().flatten().take(num_rows) {
            for index in 0..columns.len() {
                intermediates[index].agg(&row[index]);
            }
        }

        self.columns = columns
            .into_iter()
            .zip(intermediates)
            .map(|(name, coltype)| CsvColumnInfo {
                name,
                r#type: coltype.finish(),
                column_docs: Vec::new(),
            })
            .collect();

        Ok(self)
    }

    pub fn load_data_def(mut self) -> Self {
        if let Some(mut docs) = crate::util::read_doc_file(&self.args.input_file) {
            if let Some(struct_docs) = docs.remove("") {
                self.struct_docs = struct_docs;
            }

            for column in self.columns.iter_mut() {
                if let Some(docs) = docs.remove(&column.name) {
                    column.column_docs = docs;
                }
            }
        };

        self
    }

    pub fn generate(&self, buf: &mut BufWriter<File>) -> Result<(), TypeGenErrors> {
        let typename = self.args.get_typename();

        if self.args.error_handling == ErrorHandling::Result {
            writeln!(buf, "pub enum {typename}Error {{")?;
            writeln!(buf, "    CsvError(csv::Error),")?;
            writeln!(buf, "    ColumnNotFound {{")?;
            writeln!(buf, "        linenum: u64,")?;
            writeln!(buf, "        column_name: &'static str,")?;
            writeln!(buf, "    }},")?;
            writeln!(buf, "    InvalidColumnValue {{")?;
            writeln!(buf, "        linenum: u64,")?;
            writeln!(buf, "        column_name: &'static str,")?;
            writeln!(buf, "        value: String,")?;
            writeln!(buf, "    }},")?;
            writeln!(buf, "}}")?;
            writeln!(buf)?;

            writeln!(buf, "impl From<csv::Error> for {typename}Error {{")?;
            writeln!(buf, "    fn from(e: csv::Error) -> Self {{")?;
            writeln!(buf, "        Self::CsvError(e)")?;
            writeln!(buf, "    }}")?;
            writeln!(buf, "}}")?;

            writeln!(buf, "impl From<(u64, &'static str)> for {typename}Error {{")?;
            writeln!(
                buf,
                "    fn from((linenum, column_name): (u64, &'static str)) -> Self {{"
            )?;
            writeln!(buf, "        Self::ColumnNotFound {{")?;
            writeln!(buf, "            linenum,")?;
            writeln!(buf, "            column_name,")?;
            writeln!(buf, "        }}")?;
            writeln!(buf, "    }}")?;
            writeln!(buf, "}}")?;
            writeln!(buf)?;

            writeln!(
                buf,
                "impl<S> From<(u64, &'static str, S)> for {typename}Error"
            )?;
            writeln!(buf, "where")?;
            writeln!(buf, "    S: Into<String>,")?;
            writeln!(buf, "{{")?;
            writeln!(
                buf,
                "    fn from((linenum, column_name, value): (u64, &'static str, S)) -> Self {{"
            )?;
            writeln!(buf, "        Self::InvalidColumnValue {{")?;
            writeln!(buf, "            linenum,")?;
            writeln!(buf, "            column_name,")?;
            writeln!(buf, "            value: value.into(),")?;
            writeln!(buf, "        }}")?;
            writeln!(buf, "    }}")?;
            writeln!(buf, "}}")?;
        }
        writeln!(buf)?;

        writeln!(buf, "#[derive(Clone, Debug)]")?;
        for doc in &self.struct_docs {
            writeln!(buf, "/// {doc}")?;
        }
        writeln!(buf, "pub struct {typename} {{")?;
        for col in &self.columns {
            // TODO: For String types, we might be able to use Cow<'_, str>, referencing the StringRecord value
            // until such time as the caller wants to incur allocation overhead.
            for doc in &col.column_docs {
                writeln!(buf, "    /// {doc}")?;
            }
            writeln!(buf, "    pub {}: {},", col.name, col.r#type)?;
        }
        writeln!(buf, "}}")?;
        writeln!(buf)?;

        writeln!(buf, "impl {typename} {{")?;
        writeln!(
            buf,
            "    pub const COLUMN_NAMES: [&str; {}] = [",
            self.columns.len()
        )?;
        for col in &self.columns {
            writeln!(buf, "        \"{}\",", col.name)?;
        }
        writeln!(buf, "    ];")?;
        writeln!(buf)?;

        writeln!(
            buf,
            "    /// A string representation of the Rust type associated with each type."
        )?;
        writeln!(
            buf,
            "    pub const COLUMN_TYPES: [&str; {}] = [",
            self.columns.len()
        )?;
        for col in &self.columns {
            writeln!(buf, "        \"{}\", // {}", col.r#type, col.name)?;
        }
        writeln!(buf, "    ];")?;
        writeln!(buf)?;

        writeln!(
            buf,
            "    pub fn load_csv<P>(filename: P) -> Result<{typename}Iterator, csv::Error>"
        )?;
        writeln!(buf, "    where")?;
        writeln!(buf, "        P: AsRef<std::path::Path>,")?;
        writeln!(buf, "    {{")?;
        writeln!(buf, "        let reader = csv::ReaderBuilder::new()")?;

        if self.args.no_header {
            writeln!(buf, "            .has_headers(false)")?;
        } else {
            writeln!(buf, "            .has_headers(true)")?;
        }

        if self.args.delimiter == '\t' {
            writeln!(buf, "            .delimiter(b'\\t')")?;
        } else {
            writeln!(buf, "            .delimiter(b'{}')", self.args.delimiter)?;
        };
        writeln!(buf, "            .from_path(filename)?;")?;
        writeln!(buf)?;
        writeln!(buf, "        let records = reader.into_records();")?;
        writeln!(buf, "        let row = csv::StringRecord::default();")?;
        writeln!(buf, "        Ok({typename}Iterator {{ records, row }})")?;
        writeln!(buf, "    }}")?;
        writeln!(buf, "}}")?;
        writeln!(buf)?;

        writeln!(buf, "pub struct {typename}Iterator {{")?;
        writeln!(
            buf,
            "    records: csv::StringRecordsIntoIter<std::fs::File>,"
        )?;
        writeln!(buf, "    row: csv::StringRecord,")?;
        writeln!(buf, "}}")?;
        writeln!(buf)?;

        writeln!(buf, "impl Iterator for {typename}Iterator {{")?;

        match self.args.error_handling {
            ErrorHandling::Result => {
                writeln!(buf, "    type Item = Result<{typename}, {typename}Error>;")?
            }
            _ => writeln!(buf, "    type Item = {typename};")?,
        }
        writeln!(buf)?;

        writeln!(buf, "    fn next(&mut self) -> Option<Self::Item> {{")?;

        let indent = match self.args.error_handling {
            ErrorHandling::IgnoreRow => {
                writeln!(
                    buf,
                    "        // Because we're ignoring errors, loop until a row is valid"
                )?;
                writeln!(buf, "        loop {{")?;
                writeln!(buf, "            self.row = match self.records.next()? {{")?;
                writeln!(buf, "                Ok(r) => r,")?;
                writeln!(buf, "                Err(_) => continue,")?;
                writeln!(buf, "            }};")?;
                "            " // IgnoreRow indents more than other error handling
            }
            ErrorHandling::Result => {
                writeln!(buf, "        self.row = match self.records.next()? {{")?;
                writeln!(buf, "            Ok(r) => r,")?;
                writeln!(buf, "            Err(e) => return Some(Err(e.into())),")?;
                writeln!(buf, "        }};")?;
                "        "
            }
            ErrorHandling::Panic => {
                writeln!(buf, "        self.row = match self.records.next()? {{")?;
                writeln!(buf, "            Ok(r) => r,")?;
                writeln!(buf, "            Err(_) => panic!(\"Failed to get row\"),")?;
                writeln!(buf, "        }};")?;
                "        "
            }
        };
        writeln!(buf)?;

        match self.args.error_handling {
            ErrorHandling::Result | ErrorHandling::Panic => {
                writeln!(
                    buf,
                    "{indent}let linenum = self.row.position().unwrap().line();"
                )?;
                writeln!(buf)?;
            }
            ErrorHandling::IgnoreRow => {}
        }

        // Extract each column
        for (i, col) in self.columns.iter().enumerate() {
            let CsvColumnInfo {
                column_docs: _,
                name,
                r#type,
            } = col;

            let optional = r#type.is_optional();

            if r#type == &ColumnType::Unit {
                writeln!(buf, "{indent}let {name} = ();")?;
                writeln!(buf)?;
                continue;
            }

            writeln!(buf, "{indent}let {name} = match self.row.get({i}) {{")?;
            match self.args.error_handling {
                ErrorHandling::IgnoreRow => {
                    writeln!(buf, "{indent}    None => continue,")?;

                    if optional {
                        writeln!(
                            buf,
                            "{indent}    Some(\"\") => None, // empty optional fields become None"
                        )?;
                        write!(buf, "{indent}    Some(val) => Some(")?;
                    } else {
                        write!(buf, "{indent}    Some(val) => ")?;
                    }

                    match r#type {
                        ColumnType::String(_) => writeln!(buf, "val.to_owned()")?,
                        ColumnType::Bool(_) => {
                            // Bools will do case-insensitive comparisons for 'true'/'false'
                            writeln!(buf, "if val.eq_ignore_ascii_case(\"true\") {{")?;
                            writeln!(buf, "{indent}        true\n")?;
                            writeln!(
                                buf,
                                "{indent}    }} else if val.eq_ignore_ascii_case(\"false\") {{\n"
                            )?;
                            writeln!(buf, "{indent}        false\n")?;
                            writeln!(buf, "{indent}    }} else {{\n")?;
                            writeln!(buf, "{indent}        continue\n")?;
                            write!(buf, "{indent}    }}")?;
                        }
                        _ => {
                            writeln!(buf, "match val.parse() {{")?;
                            writeln!(buf, "{indent}        Ok(v) => v,")?;
                            writeln!(
                            buf,
                            "{indent}        Err(_) => continue, // invalid value - skip this row"
                        )?;
                            write!(buf, "{indent}    }}")?;
                        }
                    };

                    if optional {
                        writeln!(buf, "),")?;
                    } else {
                        writeln!(buf, ",")?;
                    }
                }
                ErrorHandling::Result => {
                    writeln!(
                        buf,
                        "{indent}    None => return Some(Err((linenum, \"{name}\").into())),"
                    )?;

                    if optional {
                        writeln!(
                            buf,
                            "{indent}    Some(\"\") => None, // empty optional fields become None"
                        )?;
                        write!(buf, "{indent}    Some(val) => Some(")?;
                    } else {
                        write!(buf, "{indent}    Some(val) => ")?;
                    }

                    match r#type {
                        ColumnType::String(_) => write!(buf, "val.to_owned()")?,
                        ColumnType::Bool(_) => {
                            // Bools will do case-insensitive comparisons for 'true'/'false'
                            writeln!(buf, "if val.eq_ignore_ascii_case(\"true\") {{")?;
                            writeln!(buf, "{indent}        true")?;
                            writeln!(
                                buf,
                                "{indent}    }} else if val.eq_ignore_ascii_case(\"false\") {{"
                            )?;
                            writeln!(buf, "{indent}        false")?;
                            writeln!(buf, "{indent}    }} else {{")?;
                            writeln!(
                            buf,
                            "{indent}        return Some(Err((linenum, \"{name}\", val).into()));"
                        )?;
                            write!(buf, "{indent}    }}")?
                        }
                        _ => {
                            writeln!(buf, "match val.parse() {{")?;
                            writeln!(buf, "{indent}        Ok(v) => v,\n")?;
                            writeln!(buf, "{indent}        Err(_) => return Some(Err((linenum, \"{name}\", val).into())),")?;
                            write!(buf, "{indent}    }}")?
                        }
                    };

                    if optional {
                        writeln!(buf, "),")?;
                    } else {
                        writeln!(buf, ",")?;
                    }
                }
                ErrorHandling::Panic => {
                    // With 'panic' error handling, failure to .get() a column will behave the same
                    writeln!(buf, "{indent}    None => panic!(\"Failed to get '{name}' at line={{linenum}} column={i}\"),")?;

                    if optional {
                        writeln!(buf, "{indent}    Some(\"\") => None,")?;
                        write!(buf, "{indent}    Some(val) => Some(")?;
                    } else {
                        write!(buf, "{indent}    Some(val) => ")?;
                    }

                    match r#type {
                        ColumnType::String(_) => write!(buf, "val.to_owned()")?,
                        ColumnType::Bool(_) => {
                            writeln!(buf, "if val.eq_ignore_ascii_case(\"true\") {{")?;
                            writeln!(buf, "{indent}        true")?;
                            writeln!(
                                buf,
                                "{indent}    }} else if val.eq_ignore_ascii_case(\"false\") {{"
                            )?;
                            writeln!(buf, "{indent}        false")?;
                            writeln!(buf, "{indent}    }} else {{")?;
                            writeln!(buf, "{indent}        panic!(\"Failed to parse {name}='{{val}}' at line={{linenum}} column={i}\")")?;
                            write!(buf, "{indent}    }}")?
                        }
                        _ => {
                            writeln!(buf, "match val.parse() {{")?;
                            writeln!(buf, "{indent}        Ok(v) => v,")?;
                            writeln!(buf, "{indent}        Err(_) => panic!(\"Failed to parse {name}='{{val}}' at line={{linenum}} column={i}\"),")?;
                            write!(buf, "{indent}    }}")?;
                        }
                    }

                    if optional {
                        writeln!(buf, "),")?;
                    } else {
                        writeln!(buf, ",")?;
                    }
                }
            }

            writeln!(buf, "{indent}}};")?; // end this particular column
            writeln!(buf)?;
        }

        // Create the struct that will be returned
        {
            writeln!(buf, "{indent}let res = {typename} {{")?;
            for col in &self.columns {
                writeln!(buf, "{indent}    {},", col.name)?;
            }
            writeln!(buf, "{indent}}};")?;
            writeln!(buf)?;
        }

        match self.args.error_handling {
            ErrorHandling::IgnoreRow => {
                writeln!(buf, "            return Some(res);")?;
                writeln!(buf, "        }} // end loop")?;
            }
            ErrorHandling::Result => writeln!(buf, "{indent}Some(Ok(res))")?,
            ErrorHandling::Panic => writeln!(buf, "{indent}Some(res)")?,
        }

        writeln!(buf, "    }}")?;
        writeln!(buf, "}}")?;
        writeln!(buf)?;

        writeln!(buf, "fn main() {{")?;
        match self.args.error_handling {
            ErrorHandling::Result => {
                // for the result type, our sample will flatten Result<T,E> out to T
                writeln!(
                    buf,
                    "    for row in {typename}::load_csv({:?})",
                    self.args.input_file
                )?;
                writeln!(buf, "        .expect(\"Couldn't load file\")")?;
                writeln!(buf, "        .flatten()")?;
                writeln!(buf, "    {{")?;
            }
            _ => {
                // other kinds will always get T
                writeln!(
                    buf,
                    "    for row in {typename}::load_csv({:?})",
                    self.args.input_file
                )?;
                writeln!(buf, "        .expect(\"Couldn't load file\") {{")?;
            }
        }
        writeln!(buf, "        println!(\"Got row: {{row:?}}\");")?;
        writeln!(buf, "    }}")?;
        writeln!(buf, "}}")?;

        Ok(())
    }
}
