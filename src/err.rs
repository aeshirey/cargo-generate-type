use std::borrow::Cow;

#[derive(Debug)]
pub enum TypeGenErrors {
    IO(std::io::Error),
    Other(Cow<'static, str>),
    Csv(csv::Error),
}

impl From<String> for TypeGenErrors {
    fn from(s: String) -> Self {
        TypeGenErrors::Other(s.into())
    }
}

impl From<&'static str> for TypeGenErrors {
    fn from(s: &'static str) -> Self {
        TypeGenErrors::Other(s.into())
    }
}

impl From<std::io::Error> for TypeGenErrors {
    fn from(e: std::io::Error) -> Self {
        TypeGenErrors::IO(e)
    }
}

impl From<csv::Error> for TypeGenErrors {
    fn from(e: csv::Error) -> Self {
        TypeGenErrors::Csv(e)
    }
}
