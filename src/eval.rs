use crate::header::read_schema;
use crate::page::*;
use crate::sql_parser;
use crate::table::Table;
use anyhow::{bail, Result};
use std::fs::File;
use std::io::prelude::*;
use std::str;

pub fn eval(file: &mut File, query: &str) -> Result<()> {
    let (db_schema, db_header) = read_schema(file)?;
    let mut page_buffer = vec![0; db_header.page_size as usize];

    let statement = sql_parser::parse::select_statement(query)?;
    let page_id: usize;

    let table_name = statement.table;

    let schema: sql_parser::CreateTableStatement;
    match db_schema.iter().find(|row| row[2] == table_name) {
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

    let mut page = Page {
        buffer: &page_buffer,
        cursor: 0,
    };
    let table = read_page(&mut page, false)?;

    display(table, schema, statement);

    Ok(())
}

fn display(
    table: Table,
    schema: sql_parser::CreateTableStatement,
    statement: sql_parser::SelectStatement,
) {
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

    let header = positions
        .iter()
        .map(|pos| schema.args[*pos].column)
        .collect::<Vec<&str>>()
        .join("|");

    println!("{}", header);
    let mapping = table
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
}
