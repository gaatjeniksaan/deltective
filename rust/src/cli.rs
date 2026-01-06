use anyhow::{Context, Result};
use clap::{Arg, Command};

const VERSION: &str = env!("CARGO_PKG_VERSION");

pub fn run() -> Result<()> {
    let matches = Command::new("deltective")
        .version(VERSION)
        .about("A detective for your Delta tables - inspect, analyze, and optimize")
        .arg(
            Arg::new("table_path")
                .help("Path to the Delta table directory")
                .required(true)
                .index(1),
        )
        .get_matches();

    let table_path = matches
        .get_one::<String>("table_path")
        .context("Table path is required")?;

    // Validate local paths (not Azure storage URLs)
    if !table_path.starts_with("abfss://") && !table_path.starts_with("az://") {
        if !std::path::Path::new(table_path).exists() {
            eprintln!("Error: Path does not exist: {}", table_path);
            std::process::exit(1);
        }
    }

    // Launch interactive TUI
    tui_app::run_tui(table_path)?;

    Ok(())
}

