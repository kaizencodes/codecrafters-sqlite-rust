use crate::eval::*;
use crate::header::read_schema;
use anyhow::Result;
use std::fs::File;
use std::str;

mod eval;
mod header;
mod page;
mod sql_parser;
mod table;

pub fn db_info(file: &mut File) -> Result<()> {
    let (table, db_header) = read_schema(file)?;

    println!("database page size: {}", db_header.page_size);
    println!("number of tables: {}", table.len());
    Ok(())
}

pub fn tables(file: &mut File) -> Result<()> {
    let (table, _) = read_schema(file)?;

    for row in table {
        print!("{} ", row[2]);
    }
    Ok(())
}

pub fn statement(file: &mut File, query: &str) -> Result<()> {
    eval(file, query)?;

    Ok(())
}
