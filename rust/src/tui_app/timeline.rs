use crate::inspector::DeltaTableInspector;
use chrono::{DateTime, Utc};
use ratatui::{
    layout::Rect,
    style::{Color, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Paragraph},
    Frame,
};

pub fn render(f: &mut Frame, area: Rect, table_path: &str, inspector: &DeltaTableInspector, scroll: u16) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let timeline_result = rt.block_on(inspector.get_timeline_analysis());

    let mut lines = Vec::new();

    lines.push(Line::from(vec![
        Span::styled("═══ TABLE TIMELINE & ACTIVITY ═══", Style::default().fg(Color::Cyan).add_modifier(ratatui::style::Modifier::BOLD)),
    ]));
    lines.push(Line::from(""));

    match timeline_result {
        Ok(timeline) => {
            // Activity Summary
            lines.push(Line::from(vec![
                Span::styled("📊 Activity Summary", Style::default().fg(Color::Magenta).add_modifier(ratatui::style::Modifier::BOLD)),
            ]));
            lines.push(Line::from(""));
            lines.push(Line::from(vec![
                Span::styled("  Total Operations: ", Style::default().fg(Color::Cyan)),
                Span::styled(format!("{}", timeline.total_operations), Style::default().fg(Color::Green)),
            ]));
            lines.push(Line::from(vec![
                Span::styled("  Version Creation Rate: ", Style::default().fg(Color::Cyan)),
                Span::styled(format!("{:.2} versions/day", timeline.version_creation_rate), Style::default().fg(Color::Green)),
            ]));

            // First and Latest Operations
            if let Some(first_op) = &timeline.first_operation {
                let first_time = DateTime::from_timestamp(first_op.timestamp.unwrap_or(0) / 1000, 0)
                    .unwrap_or_default();
                let op_name = first_op.operation.as_deref().unwrap_or("Unknown");
                lines.push(Line::from(vec![
                    Span::styled("  First Operation: ", Style::default().fg(Color::Cyan)),
                    Span::styled(first_time.format("%Y-%m-%d %H:%M:%S").to_string(), Style::default().fg(Color::Green)),
                    Span::styled(format!(" ({})", op_name), Style::default().fg(Color::DarkGray)),
                ]));
            }

            if let Some(latest_op) = &timeline.latest_operation {
                let latest_time = DateTime::from_timestamp(latest_op.timestamp.unwrap_or(0) / 1000, 0)
                    .unwrap_or_default();
                let op_name = latest_op.operation.as_deref().unwrap_or("Unknown");
                lines.push(Line::from(vec![
                    Span::styled("  Latest Operation: ", Style::default().fg(Color::Cyan)),
                    Span::styled(latest_time.format("%Y-%m-%d %H:%M:%S").to_string(), Style::default().fg(Color::Green)),
                    Span::styled(format!(" ({})", op_name), Style::default().fg(Color::DarkGray)),
                ]));
            }

            // Operations by Type
            lines.push(Line::from(""));
            lines.push(Line::from(vec![
                Span::styled("📈 Operations by Type", Style::default().fg(Color::Magenta).add_modifier(ratatui::style::Modifier::BOLD)),
            ]));
            lines.push(Line::from(""));

            if !timeline.operations_by_type.is_empty() {
                let mut sorted_ops: Vec<_> = timeline.operations_by_type.iter().collect();
                sorted_ops.sort_by_key(|(_, count)| **count);
                sorted_ops.reverse();

                let max_count = sorted_ops.first().map(|(_, c)| **c).unwrap_or(1) as f64;

                for (op_type, count) in sorted_ops.iter().take(10) {
                    let bar_width = ((**count as f64 / max_count) * 30.0) as usize;
                    let bar = "█".repeat(bar_width);
                    let pct = if timeline.total_operations > 0 {
                        (**count as f64 / timeline.total_operations as f64) * 100.0
                    } else {
                        0.0
                    };
                    lines.push(Line::from(vec![
                        Span::styled(format!("  {:15}", op_type), Style::default().fg(Color::Cyan)),
                        Span::styled(bar, Style::default().fg(Color::Green)),
                        Span::raw(format!(" {:4} ({:.1}%)", count, pct)),
                    ]));
                }
            } else {
                lines.push(Line::from(vec![
                    Span::styled("  No operation data available", Style::default().fg(Color::DarkGray)),
                ]));
            }

            // Write Patterns Analysis
            lines.push(Line::from(""));
            lines.push(Line::from(vec![
                Span::styled("🔍 Write Pattern Analysis", Style::default().fg(Color::Magenta).add_modifier(ratatui::style::Modifier::BOLD)),
            ]));
            lines.push(Line::from(""));

            if timeline.write_patterns.is_empty() {
                lines.push(Line::from(vec![
                    Span::styled("  ✓", Style::default().fg(Color::Green)),
                    Span::raw(" No unusual write patterns detected"),
                ]));
            } else {
                for pattern in &timeline.write_patterns {
                    lines.push(Line::from(vec![
                        Span::raw("  • "),
                        Span::styled(pattern, Style::default().fg(Color::Yellow)),
                    ]));
                }
            }

            // Timeline Insights
            lines.push(Line::from(""));
            lines.push(Line::from(vec![
                Span::styled("💡 Timeline Insights", Style::default().fg(Color::Magenta).add_modifier(ratatui::style::Modifier::BOLD)),
            ]));
            lines.push(Line::from(""));

            if timeline.version_creation_rate > 100.0 {
                lines.push(Line::from(vec![
                    Span::styled("  ⚠️", Style::default().fg(Color::Yellow)),
                    Span::styled("  Very high version creation rate", Style::default().fg(Color::Yellow)),
                ]));
                lines.push(Line::from(vec![
                    Span::styled("     ", Style::default().fg(Color::DarkGray)),
                    Span::raw("Consider running OPTIMIZE more frequently to manage file growth"),
                ]));
            } else if timeline.version_creation_rate > 10.0 {
                lines.push(Line::from(vec![
                    Span::styled("  ℹ️", Style::default().fg(Color::Cyan)),
                    Span::styled("  Moderate version creation rate", Style::default().fg(Color::Cyan)),
                ]));
                lines.push(Line::from(vec![
                    Span::styled("     ", Style::default().fg(Color::DarkGray)),
                    Span::raw("Regular OPTIMIZE operations recommended"),
                ]));
            } else {
                lines.push(Line::from(vec![
                    Span::styled("  ✓", Style::default().fg(Color::Green)),
                    Span::styled("  Normal version creation rate", Style::default().fg(Color::Green)),
                ]));
            }

            if timeline.total_operations > 100 {
                lines.push(Line::from(vec![
                    Span::styled("  ℹ️", Style::default().fg(Color::Cyan)),
                    Span::styled(format!("  Table has extensive history ({} operations)", timeline.total_operations), Style::default().fg(Color::Cyan)),
                ]));
                lines.push(Line::from(vec![
                    Span::styled("     ", Style::default().fg(Color::DarkGray)),
                    Span::raw("Consider periodic VACUUM to manage storage costs"),
                ]));
            }
        }
        Err(_) => {
            lines.push(Line::from(vec![
                Span::styled("Loading timeline data...", Style::default().fg(Color::DarkGray)),
            ]));
        }
    }

    let paragraph = Paragraph::new(lines)
        .block(Block::default().borders(Borders::ALL).title("Timeline [↑↓ scroll]"))
        .scroll((scroll, 0));

    f.render_widget(paragraph, area);
}

