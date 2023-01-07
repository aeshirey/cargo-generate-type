use crate::{
    column::{ColumnType, IntermediateColumnType},
    err::TypeGenErrors,
    input_args::{Commands, ErrorHandling},
};
use std::{
    fs::File,
    io::{BufWriter, Write},
};

pub fn generate(args: &crate::Commands, buf: &mut BufWriter<File>) -> Result<(), TypeGenErrors> {
    let columns = check_file(args)?;

    let typename = args.get_typename();

    if args.error_handling == ErrorHandling::Result {
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
    writeln!(buf, "pub struct {typename} {{")?;
    for (name, coltype) in columns.iter() {
        // TODO: For String types, we might be able to use Cow<'_, str>, referencing the StringRecord value
        // until such time as the caller wants to incur allocation overhead.
        writeln!(buf, "    pub {name}: {coltype},")?;
    }
    writeln!(buf, "}}")?;
    writeln!(buf)?;

    writeln!(buf, "impl {typename} {{")?;
    writeln!(buf, "    pub const COLUMNS: [&str; {}] = [", columns.len())?;
    for (name, _) in columns.iter() {
        writeln!(buf, "        \"{name}\",")?;
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

    if args.no_header {
        writeln!(buf, "            .has_headers(false)")?;
    } else {
        writeln!(buf, "            .has_headers(true)")?;
    }

    if args.delimiter == '\t' {
        writeln!(buf, "            .delimiter(b'\\t')")?;
    } else {
        writeln!(buf, "            .delimiter(b'{}')", args.delimiter)?;
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

    match args.error_handling {
        ErrorHandling::Result => {
            writeln!(buf, "    type Item = Result<{typename}, {typename}Error>;")?
        }
        _ => writeln!(buf, "    type Item = {typename};")?,
    }
    writeln!(buf)?;

    writeln!(buf, "    fn next(&mut self) -> Option<Self::Item> {{")?;

    let indent = match args.error_handling {
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

    match args.error_handling {
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
    for (i, (name, coltype)) in columns.iter().enumerate() {
        let optional = coltype.is_optional();

        if coltype == &ColumnType::Unit {
            writeln!(buf, "{indent}let {name} = ();")?;
            writeln!(buf)?;
            continue;
        }

        writeln!(buf, "{indent}let {name} = match self.row.get({i}) {{")?;
        match args.error_handling {
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

                match coltype {
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

                match coltype {
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

                match coltype {
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
        for (name, _coltype) in columns.iter() {
            writeln!(buf, "{indent}    {name},")?;
        }
        writeln!(buf, "{indent}}};")?;
        writeln!(buf)?;
    }

    match args.error_handling {
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
    match args.error_handling {
        ErrorHandling::Result => {
            // for the result type, our sample will flatten Result<T,E> out to T
            writeln!(
                buf,
                "    for row in {typename}::load_csv({:?})",
                args.input_file
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
                args.input_file
            )?;
            writeln!(buf, "        .expect(\"Couldn't load file\") {{")?;
        }
    }
    writeln!(buf, "        println!(\"Got row: {{row:?}}\");")?;
    writeln!(buf, "    }}")?;
    writeln!(buf, "}}")?;

    Ok(())
}

fn check_file(args: &crate::Commands) -> Result<Vec<(String, ColumnType)>, TypeGenErrors> {
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(!args.no_header)
        .from_path(&args.input_file)?;

    let columns = if args.no_header {
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
            .map(|s| s.replace(' ', "_"))
            .collect::<Vec<_>>()
    };

    let mut intermediates = (0..columns.len())
        .map(|_| IntermediateColumnType::default())
        .collect::<Vec<_>>();

    let num_rows = match args.num_rows {
        Some(0) => usize::MAX,
        Some(n) => n,
        None => Commands::DEFAULT_NUM_ROWS,
    };

    for row in reader.into_records().flatten().take(num_rows) {
        for index in 0..columns.len() {
            let input_value = &row[index];

            intermediates[index].agg(input_value);
        }
    }

    let results = columns
        .into_iter()
        .zip(intermediates)
        .map(|(name, coltype)| (name, coltype.finalize()))
        .collect();

    Ok(results)
}
