use crate::{
    column::{ColumnType, IntermediateColumnType},
    err::TypeGenErrors,
    input_args::{ErrorHandling, Commands},
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
    for (i, (name, _)) in columns.iter().enumerate() {
        if i == 0 {
            write!(buf, "        \"{name}\"")?;
        } else {
            write!(buf, ",\n        \"{name}\"")?;
        }
    }
    writeln!(buf)?;
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
    writeln!(buf, "            .has_headers(true)")?;
    if args.delimiter == '\t' {
        writeln!(buf, "            .delimiter(b'\\t')")?;
    } else {
        writeln!(buf, "            .delimiter(b'{}')", args.delimiter)?;
    };
    writeln!(buf, "            .from_path(filename)?;")?;
    writeln!(buf)?;
    writeln!(buf, "        let records = reader.into_records();")?;
    writeln!(buf, "        Ok({typename}Iterator {{ records }})")?;
    writeln!(buf, "    }}")?;
    writeln!(buf, "}}")?;
    writeln!(buf)?;

    writeln!(buf, "pub struct {typename}Iterator {{")?;
    writeln!(
        buf,
        "    records: csv::StringRecordsIntoIter<std::fs::File>,"
    )?;
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
            writeln!(buf, "            let row = match self.records.next()? {{")?;
            writeln!(buf, "                Ok(r) => r,")?;
            writeln!(buf, "                Err(_) => continue,")?;
            writeln!(buf, "            }};")?;
            "            " // IgnoreRow indents more than other error handling
        }
        ErrorHandling::Result => {
            writeln!(buf, "        let row = match self.records.next()? {{")?;
            writeln!(buf, "            Ok(r) => r,")?;
            writeln!(buf, "            Err(e) => return Some(Err(e.into())),")?;
            writeln!(buf, "        }};")?;
            "        "
        }
        ErrorHandling::Panic => {
            writeln!(buf, "        let row = match self.records.next()? {{")?;
            writeln!(buf, "            Ok(r) => r,")?;
            writeln!(
                buf,
                "            Err(_) => panic!(\"Failed to get row after line {{}}\"),"
            )?;
            writeln!(buf, "        }};")?;
            //writeln!(buf, "        let row = self.records.next()?.unwrap();")?;
            "        "
        }
    };
    writeln!(buf)?;

    match args.error_handling {
        ErrorHandling::Result | ErrorHandling::Panic => {
            writeln!(buf, "{indent}let linenum = row.position().unwrap().line();")?;
            writeln!(buf)?;
        }
        ErrorHandling::IgnoreRow => {}
    }

    // Extract each column
    for (i, (name, coltype)) in columns.iter().enumerate() {
        let parseable = coltype.is_parseable();
        let optional = coltype.is_optional();

        if coltype == &ColumnType::Unit {
            writeln!(buf, "let {name} = ();")?;
            continue;
        }

        writeln!(buf, "{indent}let {name} = match row.get({i}) {{")?;
        match args.error_handling {
            ErrorHandling::IgnoreRow => {
                writeln!(buf, "{indent}    None => continue,")?;

                if optional {
                    writeln!(
                        buf,
                        "{indent}    Some(\"\") => None, // empty optional fields become None"
                    )?;
                }

                let val = if parseable {
                    let mut s = "match val.parse() {\n".to_string();
                    s.push_str(&format!("{indent}        Ok(v) => v,\n"));
                    s.push_str(&format!(
                        "{indent}        Err(_) => continue, // invalid value - skip this row\n"
                    ));
                    s.push_str(&format!("{indent}    }}"));
                    s
                } else {
                    // if it's not parseable, it's a string
                    "val.to_owned()".to_owned()
                };

                if optional {
                    writeln!(buf, "{indent}    Some(val) => Some({val}),")?;
                } else {
                    writeln!(buf, "{indent}    Some(val) => {val},")?;
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
                }

                let val = if parseable {
                    let mut s = "match val.parse() {\n".to_string();
                    s.push_str(&format!("{indent}        Ok(v) => v,\n"));
                    s.push_str(&format!(
                        "{indent}        Err(_) => return Some(Err((linenum, \"{name}\", val).into())),\n"
                    ));
                    s.push_str(&format!("{indent}    }}"));
                    s
                } else {
                    // if it's not parseable, it's a string
                    "val.to_owned()".to_owned()
                };

                if optional {
                    writeln!(buf, "{indent}    Some(val) => Some({val}),")?;
                } else {
                    writeln!(buf, "{indent}    Some(val) => {val},")?;
                }
            }
            ErrorHandling::Panic => {
                // With 'panic' error handling, failure to .get() a column will behave the same
                writeln!(buf, "{indent}    None => panic!(\"Failed to get '{name}' at line={{linenum}} column={i}\"),")?;

                if optional {
                    writeln!(buf, "{indent}    Some(\"\") => None,")?;
                }

                let val = if parseable {
                    //"match val.parse().unwrap()"
                    let mut s = "match val.parse() {\n".to_string();
                    s.push_str(&format!("{indent}        Ok(v) => v,\n"));
                    s.push_str(&format!(
                        "{indent}        Err(_) => panic!(\"Failed to parse {name}='{{val}}' at line={{linenum}} column={i}\"),\n"
                    ));
                    s.push_str(&format!("{indent}    }}"));
                    s
                } else {
                    // if it's not parseable, it's a string
                    "val.to_owned()".to_owned()
                };

                if optional {
                    writeln!(buf, "{indent}    Some(val) => Some({val}),")?;
                } else {
                    writeln!(buf, "{indent}    Some(val) => {val},")?;
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

    Ok(())
}

fn check_file(args: &crate::Commands) -> Result<Vec<(String, ColumnType)>, TypeGenErrors> {
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(true)
        .from_path(&args.input_file)?;

    let columns = reader
        .headers()?
        .iter()
        .map(|s| s.replace(' ', "_"))
        .collect::<Vec<_>>();

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
