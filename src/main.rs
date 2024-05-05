use anyhow::{bail, Result};
use regex::Regex;
use sqlite_starter_rust::{db_info, statement, tables};
use std::fs::File;

fn main() -> Result<()> {
    let args = std::env::args().collect::<Vec<_>>();
    match args.len() {
        0 | 1 => bail!("Missing <database path> and <command>"),
        2 => bail!("Missing <command>"),
        _ => {}
    }
    let mut file = File::open(&args[1])?;
    let command = &args[2];
    let re = Regex::new(r"^SELECT")?;
    match command.as_str() {
        ".dbinfo" => {
            db_info(&mut file)?;
        }
        ".tables" => {
            tables(&mut file)?;
        }
        _ if re.is_match(command) => {
            statement(&mut file, command)?;
        }
        _ => bail!("Missing or invalid command passed: {}", command),
    }

    Ok(())
}
