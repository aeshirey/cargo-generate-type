use crate::{
    column::{ColumnType, IntermediateColumnType},
    err::TypeGenErrors,
    input_args::{Commands, ErrorHandling, StringHandling},
    util,
};
use std::{
    borrow::Cow,
    collections::HashSet,
    fs::File,
    io::{BufWriter, Write},
};

#[derive(Debug)]
struct CsvColumnInfo {
    column_docs: Vec<String>,
    name: String,
    r#type: ColumnType,
    seen_values: HashSet<String>,
    error_handling: ErrorHandling,
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

        let start_position = reader.position().clone();

        for (linenum, row) in reader.records().flatten().take(num_rows).enumerate() {
            if row.len() != columns.len() {
                return Err(TypeGenErrors::Other(
                    format!(
                        "Expected {} columns but found {} on line {linenum}",
                        columns.len(),
                        row.len()
                    )
                    .into(),
                ));
            }

            for index in 0..columns.len() {
                //println!("{} @ {index}", &row[index]);
                intermediates[index].agg(&row[index]);
            }
        }

        reader.seek(start_position).unwrap();

        // If we're not going to yield owned strings, we will need to collect the set of known values
        let mut seen_values = (0..columns.len())
            .map(|_| std::collections::HashSet::new())
            .collect::<Vec<_>>();

        if self.args.string_handling != StringHandling::Owned {
            for row in reader.records().flatten().take(num_rows) {
                for index in 0..columns.len() {
                    // Only need to do anything if this is a string column
                    if matches!(intermediates[index], IntermediateColumnType::String(_)) {
                        seen_values[index].insert(row[index].to_string());

                        // Now that we've inserted a value, check if we exceed a (possibly) specified cardinality limit
                        if let Some(max_strings) = self.args.max_strings {
                            if seen_values[index].len() > max_strings {
                                return Err(TypeGenErrors::Other(
                                    format!("Too many unique strings in column {}", columns[index])
                                        .into(),
                                ));
                            }
                        }
                    }
                }
            }
        }

        self.columns = columns
            .into_iter()
            .zip(intermediates)
            .zip(seen_values)
            .map(|((name, coltype), seen_values)| CsvColumnInfo {
                name,
                r#type: coltype.finish(),
                column_docs: Vec::new(),
                seen_values,
                error_handling: self.args.error_handling,
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

        if matches!(self.args.string_handling, StringHandling::Enum(_)) {
            for col in &self.columns {
                if matches!(col.r#type, ColumnType::String(_)) {
                    col.write_enum(buf)?;
                }
            }
        }

        // If string handling is 'static' or 'enum, we can derive 'Copy' on the type
        match self.args.string_handling {
            StringHandling::Static => writeln!(buf, "#[derive(Copy, Clone, Debug)]")?,
            StringHandling::Enum(_) => writeln!(buf, "#[derive(Copy, Clone, Debug)]")?,
            StringHandling::Owned => writeln!(buf, "#[derive(Clone, Debug)]")?,
        }

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
            writeln!(
                buf,
                "    pub {}: {},",
                util::str_to_snake_case_identifier(&col.name),
                col.as_str(self.args.string_handling)
            )?;
        }
        writeln!(buf, "}}")?;
        writeln!(buf)?;

        writeln!(buf, "impl {typename} {{")?;

        {
            writeln!(
                buf,
                "    /// The `(name, type)` associated with each column."
            )?;
            writeln!(
                buf,
                "    pub const COLUMNS: [(&str, &str); {}] = [",
                self.columns.len()
            )?;
            for col in &self.columns {
                writeln!(
                    buf,
                    "        (\"{}\", \"{}\"),",
                    col.name,
                    col.as_str(self.args.string_handling),
                )?;
            }
            writeln!(buf, "    ];")?;
            writeln!(buf)?;
        }

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
                seen_values,
                error_handling,
            } = col;

            let optional = r#type.is_optional();

            if r#type == &ColumnType::Unit {
                writeln!(buf, "{indent}let {name} = ();")?;
                writeln!(buf)?;
                continue;
            }

            let snake_name = util::str_to_snake_case_identifier(name);
            writeln!(buf, "{indent}let {snake_name} = match self.row.get({i}) {{")?;
            write!(buf, "{indent}    None => ")?;

            // Can't get a value from the CSV reader
            {
                match self.args.error_handling {
                    ErrorHandling::IgnoreRow => {
                        writeln!(buf, "continue,")?;
                    }
                    ErrorHandling::Result => {
                        writeln!(buf, "return Some(Err((linenum, \"{name}\").into())),")?;
                    }
                    ErrorHandling::Panic => {
                        writeln!(buf, "panic!(\"Failed to get '{snake_name}' at line={{linenum}} column={i}\"),")?;
                    }
                }
            }

            // Handle the Some start for all non-strings
            //if !matches!(r#type, ColumnType::String(_)) {
            if optional {
                writeln!(buf, "{indent}    Some(\"\") => None,")?;
                //} else if matches!(r#type, ColumnType::Unit) {
                //    writeln!(buf, "{indent}    Some(\"\") => (),")?;
            }
            //}

            let error_string = match error_handling {
                ErrorHandling::IgnoreRow => "continue".to_string(),
                ErrorHandling::Result => format!("return Some(Err((linenum, \"{snake_name}\", val).into()))"),
                ErrorHandling::Panic =>  format!("panic!(\"Unexpected '{snake_name}' value '{{val}}' at line={{linenum}} column={i}\")"),
            };

            match r#type {
                ColumnType::String(_) => match self.args.string_handling {
                    StringHandling::Owned => {
                        write!(buf, "{indent}    Some(val) => val.to_owned()")?
                    }
                    StringHandling::Static => {
                        //writeln!(buf, "Some(val) => match val {{")?;
                        for seen in seen_values {
                            writeln!(buf, "{indent}    Some(\"{seen}\") => \"{seen}\",")?;
                        }
                        writeln!(buf, "{indent}    Some(val) => {error_string},")?;
                        //writeln!(buf, "{indent}    }}")?;
                    }
                    StringHandling::Enum(_) => {
                        writeln!(buf, "{indent}    Some(val) => match val.parse() {{")?;
                        writeln!(buf, "{indent}        Ok(v) => v,")?;
                        writeln!(buf, "{indent}        Err(_) => {error_string},")?;
                        writeln!(buf, "{indent}    }}")?;
                    }
                },
                ColumnType::Bool(_) => {
                    // Bools will do case-insensitive comparisons for 'true'/'false'
                    write!(
                        buf,
                        "{indent}    Some(val) if val.eq_ignore_ascii_case(\"true\") => "
                    )?;

                    if optional {
                        writeln!(buf, "Some(true),")?;
                    } else {
                        writeln!(buf, "true,")?;
                    }

                    write!(
                        buf,
                        "{indent}    Some(val) if val.eq_ignore_ascii_case(\"false\") => "
                    )?;
                    if optional {
                        writeln!(buf, "Some(false),")?;
                    } else {
                        writeln!(buf, "false,")?;
                    }

                    writeln!(buf, "{indent}    Some(val) => {error_string}")?;
                }
                _ => {
                    writeln!(buf, "{indent}    Some(val) => match val.parse() {{")?;
                    if optional {
                        writeln!(buf, "{indent}        Ok(v) => Some(v),")?;
                    } else {
                        writeln!(buf, "{indent}        Ok(v) => v,")?;
                    }
                    writeln!(buf, "{indent}        Err(_) => {error_string},")?;
                    writeln!(buf, "{indent}    }}")?;
                }
            }

            writeln!(buf, "{indent}}};")?; // end this particular column
            writeln!(buf)?;
        }

        // Create the struct that will be returned
        {
            writeln!(buf, "{indent}let res = {typename} {{")?;
            for col in &self.columns {
                writeln!(
                    buf,
                    "{indent}    {},",
                    util::str_to_snake_case_identifier(&col.name)
                )?;
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

impl CsvColumnInfo {
    pub fn write_enum(&self, buf: &mut BufWriter<File>) -> Result<(), std::io::Error> {
        let enum_name = util::str_to_camel_case_identifier(&self.name);

        // definition
        {
            writeln!(buf, "#[derive(Copy, Clone, Debug, PartialEq, Eq)]")?;
            writeln!(buf, "pub enum {enum_name} {{")?;

            for seen_value in &self.seen_values {
                let seen_value_name = util::str_to_camel_case_identifier(seen_value);

                if seen_value_name != *seen_value {
                    writeln!(buf, "    /// From the input string '{seen_value}'")?;
                }
                writeln!(buf, "    {seen_value_name},")?;
            }

            writeln!(buf, "}}")?;
            writeln!(buf)?;
        }

        // implementation
        {
            writeln!(buf, "impl std::str::FromStr for {enum_name} {{")?;
            writeln!(buf, "    type Err = String;")?;
            writeln!(buf)?;
            writeln!(
                buf,
                "    fn from_str(s: &str) -> Result<Self, Self::Err> {{"
            )?;

            writeln!(buf, "        match s {{")?;
            for seen_value in &self.seen_values {
                writeln!(
                    buf,
                    "            \"{seen_value}\" => Ok(Self::{}),",
                    util::str_to_camel_case_identifier(seen_value)
                )?;
            }
            writeln!(buf, "            _ => Err(s.to_string()),")?;
            writeln!(buf, "        }}")?;
            writeln!(buf, "    }}")?;
            writeln!(buf, "}}")?;
            writeln!(buf)?;
        }

        Ok(())
    }

    pub(crate) fn as_str(&self, string_handling: StringHandling) -> Cow<'static, str> {
        match self.r#type {
            ColumnType::Unit => "()".into(),
            ColumnType::Bool(false) => "bool".into(),
            ColumnType::Bool(true) => "Option<bool>".into(),
            ColumnType::I8(false) => "i8".into(),
            ColumnType::I8(true) => "Option<i8>".into(),
            ColumnType::I16(false) => "i16".into(),
            ColumnType::I16(true) => "Option<i16>".into(),
            ColumnType::I32(false) => "i32".into(),
            ColumnType::I32(true) => "Option<i32>".into(),
            ColumnType::I64(false) => "i64".into(),
            ColumnType::I64(true) => "Option<i64>".into(),
            ColumnType::U8(false) => "u8".into(),
            ColumnType::U8(true) => "Option<u8>".into(),
            ColumnType::U16(false) => "u16".into(),
            ColumnType::U16(true) => "Option<u16>".into(),
            ColumnType::U32(false) => "u32".into(),
            ColumnType::U32(true) => "Option<u32>".into(),
            ColumnType::U64(false) => "u64".into(),
            ColumnType::U64(true) => "Option<u64>".into(),
            ColumnType::F64(false) => "f64".into(),
            ColumnType::F64(true) => "Option<f64>".into(),
            ColumnType::String(is_opt) => match (is_opt, string_handling) {
                (false, StringHandling::Owned) => "String".into(),
                (true, StringHandling::Owned) => "Option<String>".into(),
                (false, StringHandling::Static) => "&'static str".into(),
                (true, StringHandling::Static) => "Option<&'static str>".into(),
                (false, StringHandling::Enum(_)) => {
                    util::str_to_camel_case_identifier(&self.name).into()
                }
                (true, StringHandling::Enum(_)) => {
                    format!("Option<{}>", util::str_to_camel_case_identifier(&self.name)).into()
                }
            },
        }
    }
}
