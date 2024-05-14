use crate::header;
use crate::table::{Record, Row, Table};
use anyhow::{bail, Result};
use std::str;

pub struct Page<'t> {
    pub buffer: &'t Vec<u8>,
    pub cursor: usize,
}

impl<'t> Page<'t> {
    fn read_u8(&mut self) -> u8 {
        let result: [u8; 1] = self.read_bits(1).try_into().unwrap();
        self.cursor += 1;
        u8::from_be_bytes(result)
    }

    fn read_u16(&mut self) -> u16 {
        let result: [u8; 2] = self.read_bits(2).try_into().unwrap();
        self.cursor += 2;
        u16::from_be_bytes(result)
    }

    fn read_u24(&mut self) -> u32 {
        let result: [u8; 4] = self.read_bits(3).try_into().unwrap();
        self.cursor += 3;
        u32::from_be_bytes(result)
    }

    fn read_u32(&mut self) -> u32 {
        let result: [u8; 4] = self.read_bits(4).try_into().unwrap();
        self.cursor += 4;
        u32::from_be_bytes(result)
    }

    fn read_u48(&mut self) -> u64 {
        let result: [u8; 8] = self.read_bits(6).try_into().unwrap();
        self.cursor += 6;
        u64::from_be_bytes(result)
    }

    fn read_u64(&mut self) -> u64 {
        let result: [u8; 8] = self.read_bits(8).try_into().unwrap();
        self.cursor += 8;
        u64::from_be_bytes(result)
    }

    fn read_f64(&mut self) -> f64 {
        let result: [u8; 8] = self.read_bits(8).try_into().unwrap();
        self.cursor += 8;
        f64::from_be_bytes(result)
    }

    fn read_utf8(&mut self, length: usize) -> String {
        let record = self.read_bits(length);
        let result = String::from(str::from_utf8(record).unwrap());
        self.cursor += length;
        return result;
    }

    fn read_bits(&self, length: usize) -> &[u8] {
        return &self.buffer[self.cursor..self.cursor + length];
    }
}

pub fn read_page(page: &mut Page, first: bool) -> Result<Table> {
    if first {
        page.cursor += header::SIZE;
    }
    let page_type: PageType;
    match page.read_u8() {
        0x02 => page_type = PageType::InteriorIndex,
        0x05 => page_type = PageType::InteriorTable,
        0x0a => page_type = PageType::LeafIndex,
        0x0d => page_type = PageType::LeafTable,
        _ => bail!("Incorrect page type, {}", page.read_u8()),
    };

    // skip next 2 values.
    page.cursor += 2;
    let cell_count = page.read_u16();

    // moving to the cell pointer array.
    match page_type {
        PageType::LeafIndex | PageType::LeafTable => page.cursor += 3,
        PageType::InteriorIndex | PageType::InteriorTable => page.cursor += 7,
    }

    let mut table = Table::new();
    let cell_pointer_array_start = page.cursor;

    // cell pointer array
    for i in 0..cell_count {
        let next_cell_pointer = i as usize * 2;
        page.cursor += next_cell_pointer;
        let cell_location = page.read_u16() as usize;
        page.cursor = cell_location;

        let row = read_cell(page)?;
        // find an elegant solution to handle internal tables.
        if row[2] != "sqlite_sequence" {
            table.push(row)
        }
        page.cursor = cell_pointer_array_start;
    }

    Ok(table)
}

enum PageType {
    InteriorIndex,
    InteriorTable,
    LeafIndex,
    LeafTable,
}

fn read_cell(page: &mut Page) -> Result<Row> {
    let _payload_size = read_varint(page);

    let mut row = Row::new();
    let row_id = read_varint(page);
    row.push(Record::INT(row_id));

    let previous_pos = page.cursor;
    let header_size = read_varint(page);

    let mut remaining_header = header_size - (page.cursor - previous_pos);
    let mut serial_types = vec![];
    while remaining_header > 0 {
        let previous_pos = page.cursor;

        let serial_type = read_varint(page);

        serial_types.push(serial_type);
        remaining_header -= page.cursor - previous_pos;
    }

    for serial_type in serial_types {
        let record = read_record(serial_type, page)?;
        match record {
            Record::NULL => continue,
            _ => row.push(record),
        }
    }
    Ok(row)
}

fn read_record(serial_type: usize, page: &mut Page) -> Result<Record> {
    let result: Record;
    match serial_type {
        0 => result = Record::NULL,
        1 => result = Record::INT(page.read_u8() as usize),
        2 => result = Record::INT(page.read_u16() as usize),
        3 => result = Record::INT(page.read_u24() as usize),
        4 => result = Record::INT(page.read_u32() as usize),
        5 => result = Record::INT(page.read_u48() as usize),
        6 => result = Record::INT(page.read_u64() as usize),
        7 => result = Record::FLOAT(page.read_f64()),
        8 => result = Record::INT(0 as usize),
        9 => result = Record::INT(1 as usize),
        10 | 11 => result = Record::RESERVED,
        _ => {
            let data_size: usize;
            if serial_type >= 12 && serial_type % 2 == 0 {
                data_size = (serial_type - 12) / 2;

                let record = page.read_utf8(data_size);
                result = Record::BLOB(record);
            } else if serial_type >= 13 && serial_type % 2 == 1 {
                data_size = (serial_type - 13) / 2;

                let record = page.read_utf8(data_size);
                result = Record::STRING(record);
            } else {
                bail!("incorrect serial_type {}", serial_type);
            }
        }
    }
    return Ok(result);
}

fn read_varint(page: &mut Page) -> usize {
    let mask = 0b01111111;
    let current_value = page.read_u8();
    let mut flag = (current_value >> 7) & 1 == 1;
    let mut result = (current_value & mask) as usize;

    while flag {
        let mut current_value = page.read_u8();
        flag = (current_value >> 7) & 1 == 1;
        if flag {
            current_value &= mask;
        }
        result = (result << 7) | current_value as usize;
    }

    return result;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_read_varint() {
        let test_cases = [
            (
                &mut Page {
                    buffer: &vec![0b00001000],
                    cursor: 0,
                },
                0b00001000,
                1,
            ),
            (
                &mut Page {
                    buffer: &vec![0b11000001, 0b00000001],
                    cursor: 0,
                },
                0b10000010000001,
                2,
            ),
            (
                &mut Page {
                    buffer: &vec![0b11001000, 0b11101000, 0b1001],
                    cursor: 0,
                },
                0b100100011010000001001,
                3,
            ),
            // stops when the most significant bit is zero
            (
                &mut Page {
                    buffer: &vec![0b11001000, 0b11101000, 0b1001, 0b11001000],
                    cursor: 0,
                },
                // 1001000 + 1101000 + 0001001
                0b100100011010000001001,
                3,
            ),
            // starts from pointer
            (
                &mut Page {
                    buffer: &vec![0b1001, 0b11101000, 0b1001],
                    cursor: 1,
                },
                0b11010000001001,
                3,
            ),
        ];

        for (page, expected, expected_pointer) in test_cases {
            let result = read_varint(page);

            assert_eq!(result, expected);
            assert_eq!(page.cursor, expected_pointer);
        }
    }

    #[test]
    fn test_read_varint_two() {
        let mut page = Page {
            buffer: &vec![0b00001000],
            cursor: 0,
        };
        let expected = 0b00001000;
        let result = read_varint(&mut page);
        assert_eq!(result, expected);
        assert_eq!(page.cursor, 1);
    }
}
