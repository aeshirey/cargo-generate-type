# cargo-generate-type

This binary, used as a [`cargo`](https://doc.rust-lang.org/cargo/) [custom command](https://doc.rust-lang.org/book/ch14-05-extending-cargo.html), analyzes delimited (CSV, TSV) files with headers and generates appropriate Rust code to cleanly parse the data. The generated code wraps the [`csv` crate](https://crates.io/crates/csv) to do the CSV parsing. You can install this utility now by running:

```bash
cargo install cargo-generate-type
```

The output of this tool is meant to make your life much easier when dealing with delimited data -- just point it to your file (or one representative of what you plan to process "in production") and integrate the generated code into your project.

`cargo-generate-type` reads the specified file; decides on the appropriate type for each column and, for integers, restricts the type to the minimum size required to accommodate the range; and generates code that makes reading those data ergonomic:

```ignore
$ head -5 iris.csv
sepal length in cm,sepal width in cm,petal length in cm,petal width in cm,class
5.1,3.5,1.4,0.2,Iris-setosa
4.9,3.0,1.4,0.2,Iris-setosa
4.7,3.2,1.3,0.2,Iris-setosa
4.6,3.1,1.5,0.2,Iris-setosa

$ cargo generate-type iris.csv
Generated "iris.rs"
```

This generates an `Iris` struct:

```rust
#[derive(Clone, Debug)]
pub struct Iris {
    pub sepal_length_in_cm: f64,
    pub sepal_width_in_cm: f64,
    pub petal_length_in_cm: f64,
    pub petal_width_in_cm: f64,
    pub class: String,
}
```

It also generates the `impl Iris` that knows how to load and parse the data and deal with errors in input.

## Use

You must pass in the path to a delimited file. By default, the name of this file becomes the name of the generated struct (albeit with some normalization) and the output source file. For example, "iris.csv" would generate an `Iris` struct contained in iris.rs. "shareholder_report.csv" will generate `ShareholderReport`. You can override this behavior using the `--typename name` argument to specify the struct name and `--output-file outfile` to specify the file to create. If the file already exists, it will _not_ be overwritten unless you use the `--force` flag.

The presence of a header row allows this tool to generate appropriate column names. If your data lack a header row, you may pass in `--no-header`. This will count the number of columns in the first row of input and stub in column names in the format `column_{index}`. You may then rename these columns if you so choose from your IDE.

## Schema detection

When you run this tool, it will process (part of) your input file to try to understand the schema, including the column names and the type for all columns. 

By default, it will process the first 100 rows of input to guess what types are. You may override this by specifying `--rows n` for any non-negative integer value `n`. If you use `--rows 0`, it will use the entire input file.

Type inference for columns roughly follows this process, in order:
* If a column is _always_ blank, it will be treated as unit (`()`).
* If a column ever has a blank value, it will be treated as an `Option<T>` for whatever type `T` is decided on.
* If values are always "true" or "false", the column will be `bool`
* If values are always integral types, the range of values seen will be tracked, and according to the min and max values, the column will be one of: `i8`, `i16`, `i32`, `i64`, `u8`, `u16`, `u32`, `u64`.
* If values are always numeric, the column will be `f64`.
* Otherwise, `String` is used.

Currently, string values will be allocated instead of using a `&'a str` referring to the underlying [`csv::StringRecord`](https://docs.rs/csv/latest/csv/struct.StringRecord.html). This may be replaced soon with a `Cow<'_, str>` to reduce allocations.

## Error Handling

Generated code can handle unexpected input in three different ways, specified with the `--error handling-type` argument:

1. `--error ignore` will silently ignore any errors in input, skipping rows where a column isn't present or is of an unexpected type.
2. `--error panic` will panic for any row with invalid input.
3. `--error result` will return a `Result<T, E>` for every row with `T` being the type representing your data and `E` covering underlying [`csv` errors](https://docs.rs/csv/latest/csv/struct.Error.html) as well as unexpected input types. This is the default behavior if unspecified.

## Note on use

Because this binary is meant to be a Cargo custom command, it is called as `cargo generate-type`. Note that this is the `cargo` program with the `generate-type` subcommand. Cargo passes this subcommand as an argument to the binary, which it ignores. Because this argument is expected, if you try running `cargo-generate-type` itself, you must pass a dummy argument to it:

```bash
./cargo-generate-type generate-type shareholder_report.csv --error result
#                     ^^^^^^^^^^^^^ or any bogus argument is fine
```

## TODO
* Try avoiding string allocations by using `Cow<'_, str>` if possible, storing the most recent `StringRecord` in the generated iterator.
