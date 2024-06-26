use std::{
    collections::HashMap,
    io::{BufRead, BufReader},
    path::Path,
};

/// Attempts to produce valid Rust identifiers from a column name.
///
/// If a CSV header contains the column "first name", it is reasonable to
/// convert this to a field named `first_name`. Data may have more and varied
/// invalid characters, such as:
/// "name, first" -> `name_first` (comma and space turned into a single underscore)
/// `name (first)` -> `name_first` (parens removed, final value trimmed)
pub(crate) fn header_to_identifier(header: &str) -> String {
    // start by trimming some reasonably likely unusable characters

    let m: &[_] = &[' ', '(', ')', '[', ']'];
    let header = header.trim_matches(m);
    let mut result = String::with_capacity(header.len());

    // indicates that the previous character was an underscore
    let mut underscore = false;

    for ch in header.chars() {
        match ch {
            c if c.is_numeric() && result.is_empty() => {
                // identifiers can't start with a digit
                result.push('_');
                result.push(c);
                underscore = false;
            }
            c if c.is_alphanumeric() => {
                result.push(c);
                underscore = false;
            }
            _ if underscore => {}
            _ => {
                result.push('_');
                underscore = true;
            }
        }
    }

    if underscore {
        result.pop();
    }

    result
}

/// Reads an '.def' file associated with an input dataset, using the .def as
/// user-provided documentation to be included in generated code.
///
/// `data_filename` is the name of the input data file; the extension is changed
/// to `.def` to look for the definition file. If present, it will be parsed as
/// an ini-like schema. Given an input that looks like this:
///
/// ```ignore
/// sepal_length,sepal_width,petal_length,petal_width,class
/// 5.1,3.5,1.4,0.2,Iris-setosa
/// 4.9,3.0,1.4,0.2,Iris-setosa
/// 4.7,3.2,1.3,0.2,Iris-setosa
/// ```
///
/// You might have a definition file that looks like:
///
/// ```ignore
/// This represents [Fisher's Iris dataset](https://en.wikipedia.org/wiki/Iris_flower_data_set)
///
/// The `class` is one of: Iris-setosa, Iris-virginica, Iris-versicolor.
///
/// [sepal_length]
/// The length of the flower's sepal in centimeters.
///
/// [petal_length]
/// The length of the flower's petal in centimeters.
///
/// (...)
/// ```
pub fn read_doc_file(data_filename: &Path) -> Option<HashMap<String, Vec<String>>> {
    let dir = data_filename.parent()?.to_str()?;
    let stem = data_filename.file_stem()?.to_str()?;
    let filename = format!("{dir}/{stem}.def");

    let fh = std::fs::File::open(filename).ok()?;
    let buf = BufReader::new(fh);

    // The column name and its associated lines of text for documentation
    let mut docs = HashMap::new();

    // The empty string represents the comments on the struct itself
    let mut section = String::new();
    let mut doc_lines: Vec<String> = Vec::new();

    for line in buf.lines().map_while(Result::ok) {
        if line.starts_with('[') && line.ends_with(']') {
            // starting a new section. pop empty lines from the end
            while !doc_lines.is_empty() && doc_lines[doc_lines.len() - 1].is_empty() {
                doc_lines.pop();
            }

            docs.insert(section, doc_lines);

            let line = line.trim_matches(&['[', ']'][..]);
            let normalized = crate::util::header_to_identifier(line);

            section = normalized;
            doc_lines = Vec::new();
        } else {
            doc_lines.push(line);
        }
    }

    docs.insert(section, doc_lines);
    Some(docs)
}

fn is_identifier_char(c: char) -> bool {
    c.is_alphanumeric() || c == '_'
}

pub fn str_to_snake_case_identifier(s: &str) -> String {
    let mut result = String::new();
    let mut underscore = false;

    // Handling cases like "FIRST_NAME"
    if s == s.to_uppercase() {
        //&& s.contains('_') {
        return s.to_lowercase();
    }

    for c in s.chars() {
        match c {
            c if !is_identifier_char(c) => {
                if !underscore && !result.is_empty() {
                    underscore = true;
                    result.push('_');
                }
            }
            c if c.is_uppercase() => {
                if !result.is_empty() {
                    result.push('_');
                }

                let c = c.to_lowercase().to_string();
                result.push_str(&c);
            }
            c => result.push(c),
        }
    }

    result
}

pub fn str_to_camel_case_identifier(s: &str) -> String {
    let mut result = String::new();

    // The first character in the identifier will start out as uppercase
    let mut cap = true;

    for c in s.chars() {
        match c {
            c if !is_identifier_char(c) => cap = true,
            c if cap => {
                cap = false;
                c.to_uppercase().for_each(|ch| result.push(ch));
            }
            c => c.to_lowercase().for_each(|ch| result.push(ch)),
        }
    }

    result
}

#[test]
fn test_header_to_identifier() {
    assert_eq!("first", header_to_identifier("first"));
    assert_eq!("name_first", header_to_identifier("name, first"));
    assert_eq!("name_first", header_to_identifier("name (first)"));
    assert_eq!("smile", header_to_identifier("smile :)"));
    assert_eq!("smiley_face", header_to_identifier("smiley :) face"));
}

#[test]
fn test_str_to_snake_case_identifier() {
    let snake = str_to_snake_case_identifier("Species");
    assert_eq!(snake, "species");

    let snake = str_to_snake_case_identifier("Genus and Species");
    assert_eq!(snake, "genus_and_species");

    let snake = str_to_snake_case_identifier("Sepal length");
    assert_eq!(snake, "sepal_length");

    let snake = str_to_snake_case_identifier("FIRST_NAME");
    assert_eq!(snake, "first_name");
}

#[test]
fn test_str_to_camel_case_identifier() {
    let snake = str_to_camel_case_identifier("Species");
    assert_eq!(snake, "Species");

    let snake = str_to_camel_case_identifier("Genus and Species");
    assert_eq!(snake, "GenusAndSpecies");

    let snake = str_to_camel_case_identifier("Sepal length");
    assert_eq!(snake, "SepalLength");

    let snake = str_to_camel_case_identifier("I. versicolor");
    assert_eq!(snake, "IVersicolor");

    let snake = str_to_camel_case_identifier("CLASS");
    assert_eq!(snake, "Class");
}
