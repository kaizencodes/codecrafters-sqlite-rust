use crate::header;
use anyhow::{bail, Result};
use std::fmt;
use std::str;

pub fn read_page(page: &Vec<u8>, first: bool) -> Result<Table> {
    let mut pointer = 0;
    if first {
        pointer += header::SIZE;
    }
    let page_type: PageType;
    match u8::from_be_bytes([page[pointer]]) {
        0x02 => page_type = PageType::InteriorIndex,
        0x05 => page_type = PageType::InteriorTable,
        0x0a => page_type = PageType::LeafIndex,
        0x0d => page_type = PageType::LeafTable,
        _ => bail!(
            "Incorrect page type, {}",
            u8::from_be_bytes([page[pointer]])
        ),
    };

    let cell_count = u16::from_be_bytes([page[pointer + 3], page[pointer + 4]]);

    // moving to the cell pointer array.
    match page_type {
        PageType::LeafIndex | PageType::LeafTable => pointer += 8,
        PageType::InteriorIndex | PageType::InteriorTable => pointer += 12,
    }

    let mut table = Table::new();
    // cell pointer array
    for i in 0..cell_count {
        let counter = i as usize * 2;
        let cell_pos =
            u16::from_be_bytes([page[pointer + counter], page[pointer + counter + 1]]) as usize;
        let row = read_cell(page, cell_pos)?;
        // find an elegant solution to handle internal tables.
        if row[2] != "sqlite_sequence" {
            table.push(row)
        }
    }

    Ok(table)
}

enum PageType {
    InteriorIndex,
    InteriorTable,
    LeafIndex,
    LeafTable,
}

pub type Table = Vec<Row>;
type Row = Vec<CellType>;

#[derive(Debug)]
pub enum CellType {
    NULL,
    INT(usize),
    FLOAT(f64),
    RESERVED,
    BLOB(String),
    STRING(String),
}

impl CellType {
    pub fn to_int(&self) -> Option<usize> {
        match &self {
            CellType::INT(val) => Some(*val),
            _ => None,
        }
    }
}

impl PartialEq<&str> for CellType {
    fn eq(&self, other: &&str) -> bool {
        match &self {
            CellType::STRING(val) | CellType::BLOB(val) => val == other,
            _ => false,
        }
    }

    fn ne(&self, other: &&str) -> bool {
        !&self.eq(other)
    }
}

impl fmt::Display for CellType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self {
            Self::INT(v) => write!(f, "{}", v),
            Self::FLOAT(v) => write!(f, "{:.4}", v),
            Self::BLOB(v) | Self::STRING(v) => write!(f, "{}", v),
            _ => Ok(()),
        }
    }
}

fn read_cell(page: &Vec<u8>, mut pointer: usize) -> Result<Row> {
    let _payload_size = read_varint(page, &mut pointer);

    let mut row = Row::new();
    let row_id = read_varint(page, &mut pointer);
    row.push(CellType::INT(row_id));

    let previous_pos = pointer;
    let header_size = read_varint(page, &mut pointer);

    let mut remaining_header = header_size - (pointer - previous_pos);
    let mut serial_types = vec![];
    while remaining_header > 0 {
        let previous_pos = pointer;
        let serial_type = read_varint(page, &mut pointer);
        serial_types.push(serial_type);
        remaining_header -= pointer - previous_pos;
    }

    for serial_type in serial_types {
        let record = read_elem(serial_type, page, &mut pointer)?;
        row.push(record);
    }
    Ok(row)
}

fn read_elem(serial_type: usize, page: &Vec<u8>, pointer: &mut usize) -> Result<CellType> {
    match serial_type {
        0 => return Ok(CellType::NULL),
        1 => {
            let record: [u8; 1] = page[*pointer..*pointer + 1].try_into().unwrap();
            let record = u8::from_be_bytes(record);
            *pointer += 1;
            return Ok(CellType::INT(record as usize));
        }
        2 => {
            let record: [u8; 2] = page[*pointer..*pointer + 2].try_into().unwrap();
            let record = u16::from_be_bytes(record);
            *pointer += 2;
            return Ok(CellType::INT(record as usize));
        }
        3 => {
            let record: [u8; 4] = page[*pointer..*pointer + 3].try_into().unwrap();
            let record = u32::from_be_bytes(record);
            *pointer += 3;
            return Ok(CellType::INT(record as usize));
        }
        4 => {
            let record: [u8; 4] = page[*pointer..*pointer + 4].try_into().unwrap();
            let record = u32::from_be_bytes(record);
            *pointer += 4;
            return Ok(CellType::INT(record as usize));
        }
        5 => {
            let record: [u8; 8] = page[*pointer..*pointer + 6].try_into().unwrap();
            let record = u64::from_be_bytes(record);
            *pointer += 6;
            return Ok(CellType::INT(record as usize));
        }
        6 => {
            let record: [u8; 8] = page[*pointer..*pointer + 8].try_into().unwrap();
            let record = u64::from_be_bytes(record);
            *pointer += 8;
            return Ok(CellType::INT(record as usize));
        }
        7 => {
            let record: [u8; 8] = page[*pointer..*pointer + 8].try_into().unwrap();
            let record = f64::from_be_bytes(record);
            *pointer += 8;
            return Ok(CellType::FLOAT(record));
        }
        8 => return Ok(CellType::INT(0 as usize)),
        9 => return Ok(CellType::INT(1 as usize)),
        10 | 11 => return Ok(CellType::RESERVED),
        _ => {
            let data_size: usize;
            if serial_type >= 12 && serial_type % 2 == 0 {
                data_size = (serial_type - 12) / 2;

                let record = &page[*pointer..*pointer + data_size];
                let record = String::from(str::from_utf8(record).unwrap());
                *pointer += data_size;

                return Ok(CellType::BLOB(record));
            } else if serial_type >= 13 && serial_type % 2 == 1 {
                data_size = (serial_type - 13) / 2;

                let record = &page[*pointer..*pointer + data_size];
                let record = String::from(str::from_utf8(record).unwrap());
                *pointer += data_size;

                return Ok(CellType::STRING(record));
            } else {
                bail!("incorrect serial_type {}", serial_type);
            }
        }
    }
}

fn read_varint(page: &Vec<u8>, pointer: &mut usize) -> usize {
    let mask = 0b01111111;
    let current_value = page[*pointer];
    let mut flag = (current_value >> 7) & 1 == 1;
    let mut result = (current_value & mask) as usize;

    while flag {
        *pointer += 1;
        let mut current_value = page[*pointer];
        flag = (current_value >> 7) & 1 == 1;
        if flag {
            current_value &= mask;
        }
        result = (result << 7) | current_value as usize;
    }

    *pointer += 1;

    return result;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_read_varint() {
        let test_cases = [
            (vec![0b00001000], 0, 0b00001000, 1),
            (vec![0b11000001, 0b00000001], 0, 0b10000010000001, 2),
            (
                vec![0b11001000, 0b11101000, 0b1001],
                0,
                0b100100011010000001001,
                3,
            ),
            // stops when the most significant bit is zero
            (
                vec![0b11001000, 0b11101000, 0b1001, 0b11001000],
                0,
                // 1001000 + 1101000 + 0001001
                0b100100011010000001001,
                3,
            ),
            // starts from pointer
            (vec![0b1001, 0b11101000, 0b1001], 1, 0b11010000001001, 3),
        ];

        for (page, pointer, expected, expected_pointer) in &test_cases {
            let mut pointer = *pointer;
            let result = read_varint(page, &mut pointer);

            assert_eq!(result, *expected);
            assert_eq!(pointer, *expected_pointer);
        }
    }
}
