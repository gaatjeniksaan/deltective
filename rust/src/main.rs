mod cli;
mod inspector;
mod insights;
mod tui_app;

use anyhow::Result;

fn main() -> Result<()> {
    cli::run()
}

