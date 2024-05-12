use std::fmt;

pub type Table = Vec<Row>;
pub type Row = Vec<Record>;

#[derive(Debug)]
pub enum Record {
    NULL,
    INT(usize),
    FLOAT(f64),
    RESERVED,
    BLOB(String),
    STRING(String),
}

impl Record {
    pub fn to_int(&self) -> Option<usize> {
        match &self {
            Record::INT(val) => Some(*val),
            _ => None,
        }
    }

    pub fn to_str(&self) -> Option<&str> {
        match &self {
            Record::STRING(val) | Record::BLOB(val) => Some(&val[..]),
            _ => None,
        }
    }
}

impl PartialEq<&str> for Record {
    fn eq(&self, other: &&str) -> bool {
        match &self {
            Record::STRING(val) | Record::BLOB(val) => val == other,
            _ => false,
        }
    }

    fn ne(&self, other: &&str) -> bool {
        !&self.eq(other)
    }
}

impl fmt::Display for Record {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self {
            Self::INT(v) => write!(f, "{}", v),
            Self::FLOAT(v) => write!(f, "{:.4}", v),
            Self::BLOB(v) | Self::STRING(v) => write!(f, "{}", v),
            _ => Ok(()),
        }
    }
}
