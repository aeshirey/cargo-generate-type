pub enum IrisNoHeaderError {
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

impl From<csv::Error> for IrisNoHeaderError {
    fn from(e: csv::Error) -> Self {
        Self::CsvError(e)
    }
}
impl From<(u64, &'static str)> for IrisNoHeaderError {
    fn from((linenum, column_name): (u64, &'static str)) -> Self {
        Self::ColumnNotFound {
            linenum,
            column_name,
        }
    }
}

impl<S> From<(u64, &'static str, S)> for IrisNoHeaderError
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
pub struct IrisNoHeader {
    pub column_0: f64,
    pub column_1: f64,
    pub column_2: f64,
    pub column_3: f64,
    pub column_4: String,
}

impl IrisNoHeader {
    pub const COLUMNS: [&str; 5] = [
        "column_0",
        "column_1",
        "column_2",
        "column_3",
        "column_4",
    ];

    pub fn load_csv<P>(filename: P) -> Result<IrisNoHeaderIterator, csv::Error>
    where
        P: AsRef<std::path::Path>,
    {
        let reader = csv::ReaderBuilder::new()
            .has_headers(false)
            .delimiter(b',')
            .from_path(filename)?;

        let records = reader.into_records();
        let row = csv::StringRecord::default();
        Ok(IrisNoHeaderIterator { records, row })
    }
}

pub struct IrisNoHeaderIterator {
    records: csv::StringRecordsIntoIter<std::fs::File>,
    row: csv::StringRecord,
}

impl Iterator for IrisNoHeaderIterator {
    type Item = Result<IrisNoHeader, IrisNoHeaderError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.row = match self.records.next()? {
            Ok(r) => r,
            Err(e) => return Some(Err(e.into())),
        };

        let linenum = self.row.position().unwrap().line();

        let column_0 = match self.row.get(0) {
            None => return Some(Err((linenum, "column_0").into())),
            Some(val) => match val.parse() {
                Ok(v) => v,

                Err(_) => return Some(Err((linenum, "column_0", val).into())),
            },
        };

        let column_1 = match self.row.get(1) {
            None => return Some(Err((linenum, "column_1").into())),
            Some(val) => match val.parse() {
                Ok(v) => v,

                Err(_) => return Some(Err((linenum, "column_1", val).into())),
            },
        };

        let column_2 = match self.row.get(2) {
            None => return Some(Err((linenum, "column_2").into())),
            Some(val) => match val.parse() {
                Ok(v) => v,

                Err(_) => return Some(Err((linenum, "column_2", val).into())),
            },
        };

        let column_3 = match self.row.get(3) {
            None => return Some(Err((linenum, "column_3").into())),
            Some(val) => match val.parse() {
                Ok(v) => v,

                Err(_) => return Some(Err((linenum, "column_3", val).into())),
            },
        };

        let column_4 = match self.row.get(4) {
            None => return Some(Err((linenum, "column_4").into())),
            Some(val) => val.to_owned(),
        };

        let res = IrisNoHeader {
            column_0,
            column_1,
            column_2,
            column_3,
            column_4,
        };

        Some(Ok(res))
    }
}

fn main() {
    for row in IrisNoHeader::load_csv("examples/iris_no_header.csv")
        .expect("Couldn't load file")
        .flatten()
    {
        println!("Got row: {row:?}");
    }
}
