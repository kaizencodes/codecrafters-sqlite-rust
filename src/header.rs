use crate::page::{read_page, Page};
use crate::table::Table;
use anyhow::Result;
use std::fs::File;
use std::io::{Read, Seek};

pub const SIZE: usize = 100;

pub struct DbHeader {
    pub page_size: u16,
    pub encoding: Encoding,
}

#[derive(Debug)]
pub enum Encoding {
    UTF8,
    UTF16LE,
    UTF16BE,
}

impl DbHeader {
    pub fn build(header: &[u8; SIZE]) -> Self {
        // Based on https://www.sqlite.org/fileformat.html#storage_of_the_sql_database_schema
        // 1.3 The Database Header
        let page_size = u16::from_be_bytes([header[16], header[17]]);
        let encoding: Encoding;
        encoding = match u32::from_be_bytes([header[56], header[57], header[58], header[59]]) {
            1 => Encoding::UTF8,
            2 => Encoding::UTF16LE,
            3 => Encoding::UTF16BE,
            _ => Encoding::UTF8,
        };

        return DbHeader {
            page_size,
            encoding,
        };
    }
}

pub fn read_schema(file: &mut File) -> Result<(Table, DbHeader)> {
    let mut buffer = [0; SIZE];
    file.read_exact(&mut buffer)?;

    let db_header = DbHeader::build(&buffer);

    let mut page_buffer = vec![0; db_header.page_size as usize];
    file.rewind()?;
    file.read_exact(&mut page_buffer)?;
    let mut page = Page {
        buffer: &page_buffer,
        cursor: 0,
    };
    let table = read_page(&mut page, true)?;
    Ok((table, db_header))
}
