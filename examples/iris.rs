pub enum IrisError {
    CsvError(csv::Error),
    ColumnNotFound {
        linenum: u64,
        column_name: &'static str,
    },
    InvalidColumnValue {
        linenum: u64,
        column_name: &'static str,
        value: String,
    },
}

impl From<csv::Error> for IrisError {
    fn from(e: csv::Error) -> Self {
        Self::CsvError(e)
    }
}
impl From<(u64, &'static str)> for IrisError {
    fn from((linenum, column_name): (u64, &'static str)) -> Self {
        Self::ColumnNotFound {
            linenum,
            column_name,
        }
    }
}

impl<S> From<(u64, &'static str, S)> for IrisError
where
    S: Into<String>,
{
    fn from((linenum, column_name, value): (u64, &'static str, S)) -> Self {
        Self::InvalidColumnValue {
            linenum,
            column_name,
            value: value.into(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct Iris {
    pub sepal_length_in_cm: (),
    pub sepal_width_in_cm: (),
    pub petal_length_in_cm: (),
    pub petal_width_in_cm: (),
    pub class: (),
    pub unit: (),
}

impl Iris {
    pub const COLUMNS: [&str; 6] = [
        "sepal_length_in_cm",        "sepal_width_in_cm",        "petal_length_in_cm",        "petal_width_in_cm",        "class",        "unit",
    ];

    pub fn load_csv<P>(filename: P) -> Result<IrisIterator, csv::Error>
    where
        P: AsRef<std::path::Path>,
    {
        let reader = csv::ReaderBuilder::new()
            .has_headers(true)
            .delimiter(b',')
            .from_path(filename)?;

        let records = reader.into_records();
        let row = csv::StringRecord::default();
        Ok(IrisIterator { records, row })
    }
}

pub struct IrisIterator {
    records: csv::StringRecordsIntoIter<std::fs::File>,
    row: csv::StringRecord,
}

impl Iterator for IrisIterator {
    type Item = Result<Iris, IrisError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.row = match self.records.next()? {
            Ok(r) => r,
            Err(e) => return Some(Err(e.into())),
        };

        let linenum = self.row.position().unwrap().line();

let sepal_length_in_cm = ();
let sepal_width_in_cm = ();
let petal_length_in_cm = ();
let petal_width_in_cm = ();
let class = ();
let unit = ();
        let res = Iris {
            sepal_length_in_cm,
            sepal_width_in_cm,
            petal_length_in_cm,
            petal_width_in_cm,
            class,
            unit,
        };

        Some(Ok(res))
    }
}

fn main() {
    for row in Iris::load_csv("examples/iris.csv")
        .expect("Couldn't load file")
        .flatten()
    {
        println!("Got row: {row:?}");
    }
}
