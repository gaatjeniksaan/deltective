use crate::inspector::TableStatistics;
use crate::tui_app::format_bytes;
use ratatui::{
    layout::Rect,
    style::{Color, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Paragraph},
    Frame,
};

pub fn render(f: &mut Frame, area: Rect, stats: &TableStatistics) {
    let mut lines = Vec::new();

    // Table Overview
    lines.push(Line::from(vec![
        Span::styled("═══ TABLE OVERVIEW ═══", Style::default().fg(Color::Cyan).add_modifier(ratatui::style::Modifier::BOLD)),
    ]));
    lines.push(Line::from(""));
    lines.push(Line::from(vec![
        Span::styled("Table Path: ", Style::default().fg(Color::Cyan)),
        Span::raw(&stats.table_path),
    ]));
    lines.push(Line::from(vec![
        Span::styled("Current Version: ", Style::default().fg(Color::Cyan)),
        Span::raw(format!("{}", stats.version)),
        Span::styled(format!(" (of {} total)", stats.total_versions), Style::default().fg(Color::DarkGray)),
    ]));
    lines.push(Line::from(vec![
        Span::styled("Oldest Available Version: ", Style::default().fg(Color::Cyan)),
        Span::raw(format!("{}", stats.oldest_version)),
    ]));
    lines.push(Line::from(vec![
        Span::styled("Number of Files: ", Style::default().fg(Color::Cyan)),
        Span::raw(format!("{}", stats.num_files)),
    ]));
    lines.push(Line::from(vec![
        Span::styled("Total Size: ", Style::default().fg(Color::Cyan)),
        Span::raw(format_bytes(stats.total_size_bytes)),
    ]));

    if let Some(num_rows) = stats.num_rows {
        lines.push(Line::from(vec![
            Span::styled("Number of Rows: ", Style::default().fg(Color::Cyan)),
            Span::raw(format!("{}", num_rows)),
        ]));
    }

    if !stats.partition_columns.is_empty() {
        lines.push(Line::from(vec![
            Span::styled("Partition Columns: ", Style::default().fg(Color::Cyan)),
            Span::raw(stats.partition_columns.join(", ")),
        ]));
    }

    if let Some(created_time) = stats.created_time {
        lines.push(Line::from(vec![
            Span::styled("Created: ", Style::default().fg(Color::Cyan)),
            Span::raw(created_time.format("%Y-%m-%d %H:%M:%S").to_string()),
        ]));
    }

    if let Some(name) = &stats.metadata.name {
        lines.push(Line::from(vec![
            Span::styled("Table Name: ", Style::default().fg(Color::Cyan)),
            Span::raw(name),
        ]));
    }

    if let Some(description) = &stats.metadata.description {
        lines.push(Line::from(vec![
            Span::styled("Description: ", Style::default().fg(Color::Cyan)),
            Span::raw(description),
        ]));
    }

    // Delta Protocol & History
    lines.push(Line::from(""));
    lines.push(Line::from(vec![
        Span::styled("═══ DELTA PROTOCOL & HISTORY ═══", Style::default().fg(Color::Magenta).add_modifier(ratatui::style::Modifier::BOLD)),
    ]));
    lines.push(Line::from(""));
    lines.push(Line::from(vec![
        Span::styled("Min Reader Version: ", Style::default().fg(Color::Cyan)),
        Span::raw(format!("{}", stats.min_reader_version)),
    ]));
    lines.push(Line::from(vec![
        Span::styled("Min Writer Version: ", Style::default().fg(Color::Cyan)),
        Span::raw(format!("{}", stats.min_writer_version)),
    ]));

    if !stats.reader_features.is_empty() {
        lines.push(Line::from(""));
        lines.push(Line::from(vec![
            Span::styled("Reader Features: ", Style::default().fg(Color::Cyan)),
        ]));
        for feature in &stats.reader_features {
            lines.push(Line::from(vec![
                Span::raw("  • "),
                Span::raw(feature),
            ]));
        }
    }

    if !stats.writer_features.is_empty() {
        lines.push(Line::from(""));
        lines.push(Line::from(vec![
            Span::styled("Writer Features: ", Style::default().fg(Color::Cyan)),
        ]));
        for feature in &stats.writer_features {
            lines.push(Line::from(vec![
                Span::raw("  • "),
                Span::raw(feature),
            ]));
        }
    }

    if let Some(last_op) = &stats.last_operation {
        lines.push(Line::from(""));
        lines.push(Line::from(vec![
            Span::styled("Last Operation: ", Style::default().fg(Color::Cyan)),
            Span::raw(&last_op.operation),
        ]));
        lines.push(Line::from(vec![
            Span::styled("  Time: ", Style::default().fg(Color::DarkGray)),
            Span::raw(last_op.timestamp.format("%Y-%m-%d %H:%M:%S").to_string()),
        ]));
    }

    lines.push(Line::from(""));
    lines.push(Line::from(vec![
        Span::styled("Last Vacuum: ", Style::default().fg(Color::Cyan)),
        Span::raw(
            stats.last_vacuum
                .map(|dt| dt.format("%Y-%m-%d %H:%M:%S").to_string())
                .unwrap_or_else(|| "Never".to_string())
        ),
    ]));

    // Schema
    lines.push(Line::from(""));
    lines.push(Line::from(vec![
        Span::styled("═══ SCHEMA ═══", Style::default().fg(Color::Green).add_modifier(ratatui::style::Modifier::BOLD)),
    ]));
    lines.push(Line::from(""));

    for (col_name, col_type) in &stats.schema {
        if stats.partition_columns.contains(col_name) {
            lines.push(Line::from(vec![
                Span::styled(format!("  {}", col_name), Style::default().fg(Color::Yellow)),
                Span::styled(" (partition)", Style::default().fg(Color::DarkGray)),
                Span::raw(": "),
                Span::styled(col_type, Style::default().fg(Color::Green)),
            ]));
        } else {
            lines.push(Line::from(vec![
                Span::styled(format!("  {}", col_name), Style::default().fg(Color::Cyan)),
                Span::raw(": "),
                Span::styled(col_type, Style::default().fg(Color::Green)),
            ]));
        }
    }

    let paragraph = Paragraph::new(lines)
        .block(Block::default().borders(Borders::ALL))
        .scroll((0, 0));

    f.render_widget(paragraph, area);
}

