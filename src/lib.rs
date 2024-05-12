use crate::header::DbHeader;
use crate::reader::*;
use crate::sql_parser::parse;
use anyhow::{bail, Result};
use std::fs::File;
use std::io::prelude::*;
use std::str;

mod header;
mod reader;
mod sql_parser;

pub fn db_info(file: &mut File) -> Result<()> {
    let (table, db_header) = read_metadata(file)?;

    println!("database page size: {}", db_header.page_size);
    println!("number of tables: {}", table.len());
    Ok(())
}

pub fn tables(file: &mut File) -> Result<()> {
    let (table, _) = read_metadata(file)?;

    for row in table {
        print!("{} ", row[2]);
    }
    Ok(())
}

pub fn statement(file: &mut File, query: &str) -> Result<()> {
    let table_name: &str;

    let (table, db_header) = read_metadata(file)?;
    let mut page_buffer = vec![0; db_header.page_size as usize];

    let page_id: usize;
    let statement = sql_parser::parse::select_statement(query)?;
    table_name = statement.table;

    let schema: sql_parser::CreateTableStatement;
    match table.iter().find(|row| row[2] == table_name) {
        Some(row) => {
            match row[4].to_int() {
                Some(num) => page_id = num,
                None => bail!("no such table {}", table_name),
            }
            match sql_parser::parse::create_table_statement(&row[5].to_str().unwrap()) {
                Ok(statement) => schema = statement,
                Err(e) => bail!("table schema could not be loaded for {}, {}", table_name, e),
            }
        }
        None => bail!("no such table {}", table_name),
    }

    let page_offset = (page_id - 1) as u64 * db_header.page_size as u64;
    file.seek(std::io::SeekFrom::Start(page_offset))?;
    file.read_exact(&mut page_buffer)?;

    let positions: Vec<usize> = statement
        .args
        .iter()
        .enumerate()
        .filter_map(|(_, arg)| {
            for (index, column) in schema.args.iter().enumerate() {
                if arg == column {
                    return Some(index);
                }
            }
            None
        })
        .collect();

    let mut page = Page {
        buffer: &page_buffer,
        cursor: 0,
    };
    let table2 = read_page(&mut page, false)?;
    let mapping = table2
        .iter()
        .map(|row| {
            positions
                .iter()
                .map(|pos| row[*pos].to_str().unwrap())
                .collect::<Vec<&str>>()
                .join("|")
        })
        .collect::<Vec<String>>()
        .join("\n");
    println!("{}", mapping);

    Ok(())
}

fn read_metadata(file: &mut File) -> Result<(Table, DbHeader)> {
    let mut buffer = [0; header::SIZE];
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
