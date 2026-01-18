pub mod overview;
pub mod history;
pub mod insights;
pub mod configuration;
pub mod timeline;

use crate::inspector::{DeltaTableInspector, TableStatistics};
use crate::insights::DeltaTableAnalyzer;
use anyhow::Result;
use crossterm::event::{self, Event, KeyCode, KeyEventKind};
use ratatui::{
    backend::CrosstermBackend,
    layout::{Constraint, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Paragraph, Tabs},
    Frame, Terminal,
};
use std::io;

pub fn run_tui(table_path: &str) -> Result<()> {
    // Setup terminal
    let mut terminal = Terminal::new(CrosstermBackend::new(io::stdout()))?;
    crossterm::terminal::enable_raw_mode()?;
    crossterm::execute!(
        io::stdout(),
        crossterm::terminal::EnterAlternateScreen,
        crossterm::event::EnableMouseCapture
    )?;

    // Initialize inspector
    let rt = tokio::runtime::Runtime::new()?;
    let inspector = rt.block_on(DeltaTableInspector::new(table_path))?;
    let stats = rt.block_on(inspector.get_statistics())?;
    let history = rt.block_on(inspector.get_history(false))?;

    let mut app = App {
        table_path: table_path.to_string(),
        inspector,
        stats: stats.clone(),
        history: history.clone(),
        current_tab: 0,
        should_quit: false,
        scroll_positions: [0; 5],
        history_page: 0,
        history_reversed: false,
    };

    // Main event loop
    loop {
        terminal.draw(|f| app.ui(f))?;

        if let Event::Key(key) = event::read()? {
            if key.kind == KeyEventKind::Press {
                match key.code {
                    KeyCode::Char('q') => break,
                    KeyCode::Tab => {
                        app.current_tab = (app.current_tab + 1) % 5;
                        app.scroll_positions[app.current_tab] = 0;
                    }
                    KeyCode::Right => {
                        app.current_tab = (app.current_tab + 1) % 5;
                        app.scroll_positions[app.current_tab] = 0;
                    }
                    KeyCode::Left => {
                        app.current_tab = if app.current_tab == 0 {
                            4
                        } else {
                            app.current_tab - 1
                        };
                        // Reset scroll when switching tabs
                        app.scroll_positions[app.current_tab] = 0;
                    }
                    KeyCode::Up | KeyCode::Char('k') => {
                        let pos = &mut app.scroll_positions[app.current_tab];
                        *pos = pos.saturating_sub(1);
                    }
                    KeyCode::Down | KeyCode::Char('j') => {
                        let pos = &mut app.scroll_positions[app.current_tab];
                        *pos = pos.saturating_add(1);
                    }
                    KeyCode::PageUp => {
                        let pos = &mut app.scroll_positions[app.current_tab];
                        *pos = pos.saturating_sub(10);
                    }
                    KeyCode::PageDown => {
                        let pos = &mut app.scroll_positions[app.current_tab];
                        *pos = pos.saturating_add(10);
                    }
                    KeyCode::Home => {
                        app.scroll_positions[app.current_tab] = 0;
                    }
                    _ => {
                        // Handle tab-specific keys
                        app.handle_key(key.code);
                    }
                }
            }
        }

        if app.should_quit {
            break;
        }
    }

    // Restore terminal
    crossterm::execute!(
        io::stdout(),
        crossterm::event::DisableMouseCapture,
        crossterm::terminal::LeaveAlternateScreen
    )?;
    crossterm::terminal::disable_raw_mode()?;

    Ok(())
}

struct App {
    table_path: String,
    inspector: DeltaTableInspector,
    stats: TableStatistics,
    history: Vec<deltalake::kernel::CommitInfo>,
    current_tab: usize,
    should_quit: bool,
    // Scroll position for each tab (vertical offset)
    scroll_positions: [u16; 5],
    // History tab pagination
    history_page: usize,
    history_reversed: bool,
}

const HISTORY_PAGE_SIZE: usize = 10;

impl App {
    fn ui(&mut self, f: &mut Frame) {
        let chunks = Layout::default()
            .constraints([Constraint::Length(3), Constraint::Min(0)])
            .split(f.size());

        // Tabs
        let tabs = Tabs::new(vec!["Overview", "History", "Insights", "Configuration", "Timeline"])
            .block(Block::default().borders(Borders::ALL).title("Deltective"))
            .select(self.current_tab)
            .style(Style::default().fg(Color::White))
            .highlight_style(
                Style::default()
                    .add_modifier(Modifier::BOLD)
                    .bg(Color::Blue),
            );

        f.render_widget(tabs, chunks[0]);

        // Tab content
        let content_chunk = chunks[1];
        let scroll = self.scroll_positions[self.current_tab];
        match self.current_tab {
            0 => overview::render(f, content_chunk, &self.stats, scroll),
            1 => history::render(
                f,
                content_chunk,
                &self.history,
                scroll,
                self.history_page,
                self.total_history_pages(),
                self.history_reversed,
            ),
            2 => insights::render(f, content_chunk, &self.stats, scroll),
            3 => configuration::render(f, content_chunk, &self.table_path, &self.inspector, scroll),
            4 => timeline::render(f, content_chunk, &self.table_path, &self.inspector, scroll),
            _ => {}
        }
    }

    fn handle_key(&mut self, key: KeyCode) {
        match self.current_tab {
            1 => {
                // History tab specific keys
                let total_pages = (self.history.len() + HISTORY_PAGE_SIZE - 1) / HISTORY_PAGE_SIZE;
                match key {
                    KeyCode::Char('n') => {
                        // Next page
                        if self.history_page + 1 < total_pages {
                            self.history_page += 1;
                            self.scroll_positions[1] = 0; // Reset scroll on page change
                        }
                    }
                    KeyCode::Char('p') => {
                        // Previous page
                        if self.history_page > 0 {
                            self.history_page -= 1;
                            self.scroll_positions[1] = 0;
                        }
                    }
                    KeyCode::Char('r') => {
                        // Reverse sort
                        self.history_reversed = !self.history_reversed;
                        self.history.reverse();
                        self.history_page = 0;
                        self.scroll_positions[1] = 0;
                    }
                    _ => {}
                }
            }
            _ => {}
        }
    }

    fn total_history_pages(&self) -> usize {
        (self.history.len() + HISTORY_PAGE_SIZE - 1) / HISTORY_PAGE_SIZE
    }
}

// Helper function to format bytes
pub fn format_bytes(bytes: i64) -> String {
    let mut bytes = bytes as f64;
    let units = ["B", "KB", "MB", "GB", "TB"];
    for unit in &units {
        if bytes < 1024.0 {
            return format!("{:.2} {}", bytes, unit);
        }
        bytes /= 1024.0;
    }
    format!("{:.2} PB", bytes)
}

