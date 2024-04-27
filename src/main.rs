use anyhow::{bail, Result};
use std::fs::File;
use std::io::prelude::*;

fn main() -> Result<()> {
    // Parse arguments
    let args = std::env::args().collect::<Vec<_>>();
    match args.len() {
        0 | 1 => bail!("Missing <database path> and <command>"),
        2 => bail!("Missing <command>"),
        _ => {}
    }

    // Parse command and act accordingly
    let command = &args[2];
    match command.as_str() {
        ".dbinfo" => {
            let mut file = File::open(&args[1])?;
            let mut header = [0; 100];
            file.read_exact(&mut header)?;

            // The page size is stored at the 16th byte offset, using 2 bytes in big-endian order
            let page_size = u16::from_be_bytes([header[16], header[17]]);
            println!("database page size: {}", page_size);
            // make it dynamic
            let mut schema = [0; 4096 - 100];
            file.read_exact(&mut schema)?;

            // println!("header: {:X?}", header);
            // println!("schema: {:X?}", schema);

            let table_count = u16::from_be_bytes([schema[3], schema[4]]);
            println!("number of tables: {}", table_count);

            // let offset = u16::from_be_bytes([schema[8], schema[9]]);

            // // println!("offset: {}", offset);
            // println!("offset data: {:X?}", &schema[(offset as usize)..]);
            // for elem in &schema[(offset as usize)..] {
            //     println!("offset data: {:#b}", elem);
            // }
            // println!("offset data: {:b}", &schema[offset as usize]);
            // println!("offset data: {:b}", &schema[(offset as usize) + 1]);
        }
        _ => bail!("Missing or invalid command passed: {}", command),
    }

    Ok(())
}
