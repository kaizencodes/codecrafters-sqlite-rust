use anyhow::{bail, Result};
use std::fs::File;
use std::io::prelude::*;

use std::str;

pub const HEADER_SIZE: usize = 100;

pub fn db_info(file: &mut File) -> Result<()> {
    let mut header = [0; HEADER_SIZE];
    file.read_exact(&mut header)?;

    let db_header = DbHeader::build(&header);

    let mut page_buffer = vec![0; db_header.page_size as usize];
    file.rewind()?;
    file.read_exact(&mut page_buffer)?;

    let table = read_page(&page_buffer, true)?;
    println!("database page size: {}", db_header.page_size);
    println!("number of tables: {}", table.len());
    Ok(())
}

pub fn tables(file: &mut File) -> Result<()> {
    let mut header = [0; HEADER_SIZE];
    file.read_exact(&mut header)?;

    let db_header = DbHeader::build(&header);

    let mut page_buffer = vec![0; db_header.page_size as usize];
    file.rewind()?;
    file.read_exact(&mut page_buffer)?;

    let table = read_page(&page_buffer, true)?;
    for row in table {
        print!("{} ", row[2]);
    }
    Ok(())
}

fn read_page(page: &Vec<u8>, first: bool) -> Result<Table> {
    let mut pointer = 0;
    if first {
        pointer += HEADER_SIZE;
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
    // println!("number of cells: {}", cell_count);

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

type Table = Vec<Row>;
type Row = Vec<String>;

fn read_cell(page: &Vec<u8>, mut pointer: usize) -> Result<Row> {
    // let cell = &page[pointer..];
    // for elem in cell {
    // println!("cell data: {:#b}", elem);
    // }
    let _payload_size = read_varint(page, &mut pointer);
    // println!("cell:");
    // println!("payload_size: {payload_size}");
    let mut row = Row::new();
    let row_id = read_varint(page, &mut pointer);
    row.push(row_id.to_string());

    // println!("row_id: {row_id}");

    let previous_pos = pointer;
    let header_size = read_varint(page, &mut pointer);

    // println!("header_size: {header_size}");
    // the header size contains itself.
    let mut remaining_header = header_size - (pointer - previous_pos);
    let mut data_types = vec![];
    while remaining_header > 0 {
        let previous_pos = pointer;
        let data_type = read_varint(page, &mut pointer);
        data_types.push(data_type);
        remaining_header -= pointer - previous_pos;
    }

    // println!("data_types: {:?}", data_types);
    for data_type in data_types {
        let data_size: usize;
        if data_type >= 12 && data_type % 2 == 0 {
            data_size = (data_type - 12) / 2;
        } else if data_type >= 13 && data_type % 2 == 1 {
            data_size = (data_type - 13) / 2;
        } else {
            data_size = data_type
        }
        let record = &page[pointer..pointer + data_size];
        // println!("name: {:?}", str::from_utf8(record));
        // this is ugly.
        let record = String::from(str::from_utf8(record)?);
        row.push(record);
        pointer += data_size;
    }
    Ok(row)
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

struct DbHeader {
    page_size: u16,
    // encoding: Encoding,
}

// #[derive(Debug)]
// enum Encoding {
//     UTF8,
//     UTF16LE,
//     UTF16BE,
// }

impl DbHeader {
    fn build(header: &[u8; HEADER_SIZE]) -> Self {
        // Based on https://www.sqlite.org/fileformat.html#storage_of_the_sql_database_schema
        // 1.3 The Database Header
        let page_size = u16::from_be_bytes([header[16], header[17]]);
        // let encoding: Encoding;
        // encoding = match u32::from_be_bytes([header[56], header[57], header[58], header[59]]) {
        //     1 => Encoding::UTF8,
        //     2 => Encoding::UTF16LE,
        //     3 => Encoding::UTF16BE,
        //     _ => Encoding::UTF8,
        // };

        return DbHeader {
            page_size,
            // encoding,
        };
    }
}
