use chrono::DateTime;
use deltalake::kernel::CommitInfo;
use ratatui::{
    layout::Rect,
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Paragraph},
    Frame,
};

const PAGE_SIZE: usize = 10;

pub fn render(
    f: &mut Frame,
    area: Rect,
    history: &[CommitInfo],
    scroll: u16,
    current_page: usize,
    total_pages: usize,
    reversed: bool,
) {
    let mut lines = Vec::new();

    // Header with sort order indicator
    let sort_indicator = if reversed { "oldest first" } else { "newest first" };
    lines.push(Line::from(vec![
        Span::styled("═══ OPERATION HISTORY ═══", Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD)),
        Span::styled(format!(" ({})", sort_indicator), Style::default().fg(Color::DarkGray)),
    ]));
    lines.push(Line::from(""));

    // Calculate page bounds
    let start_idx = current_page * PAGE_SIZE;
    let end_idx = std::cmp::min(start_idx + PAGE_SIZE, history.len());

    if history.is_empty() {
        lines.push(Line::from(vec![
            Span::styled("No history entries found.", Style::default().fg(Color::DarkGray)),
        ]));
    } else {
        // Show entries for current page
        for entry in history.iter().skip(start_idx).take(PAGE_SIZE) {
            let version = entry.read_version.unwrap_or(0);
            let operation = entry.operation.as_deref().unwrap_or("Unknown");
            let timestamp = DateTime::from_timestamp(entry.timestamp.unwrap_or(0) / 1000, 0)
                .unwrap_or_default()
                .format("%Y-%m-%d %H:%M:%S")
                .to_string();

            lines.push(Line::from(vec![
                Span::styled(format!("Version {}", version), Style::default().fg(Color::Yellow)),
                Span::raw(" - "),
                Span::styled(operation.to_string(), Style::default().fg(Color::Cyan)),
                Span::raw(" - "),
                Span::styled(timestamp, Style::default().fg(Color::Green)),
            ]));

            // Add operation parameters
            if let Some(params) = &entry.operation_parameters {
                if !params.is_empty() {
                    let param_strs: Vec<String> = params
                        .iter()
                        .filter_map(|(k, v): (&String, &serde_json::Value)| {
                            match k.as_str() {
                                "mode" => Some(format!("mode={}", v)),
                                "partitionBy" => Some("partitioned".to_string()),
                                "predicate" => Some(format!("where: {}", v)),
                                _ => None,
                            }
                        })
                        .collect();
                    if !param_strs.is_empty() {
                        lines.push(Line::from(vec![
                            Span::styled("  ", Style::default().fg(Color::DarkGray)),
                            Span::raw(param_strs.join(", ")),
                        ]));
                    }
                }
            }

            lines.push(Line::from(""));
        }

        // Pagination info
        lines.push(Line::from(""));
        lines.push(Line::from(vec![
            Span::styled("───────────────────────────────────────", Style::default().fg(Color::DarkGray)),
        ]));
        lines.push(Line::from(vec![
            Span::styled(
                format!("Showing {}-{} of {} entries", start_idx + 1, end_idx, history.len()),
                Style::default().fg(Color::DarkGray),
            ),
        ]));
    }

    // Build title with navigation hints
    let title = format!(
        "History [Page {}/{} | n:next p:prev r:reverse | ↑↓:scroll]",
        current_page + 1,
        total_pages.max(1)
    );

    let paragraph = Paragraph::new(lines)
        .block(Block::default().borders(Borders::ALL).title(title))
        .scroll((scroll, 0));

    f.render_widget(paragraph, area);
}
