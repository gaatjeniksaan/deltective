use chrono::DateTime;
use deltalake::kernel::CommitInfo;
use ratatui::{
    layout::Rect,
    style::{Color, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Paragraph},
    Frame,
};

pub fn render(f: &mut Frame, area: Rect, history: &[CommitInfo]) {
    let mut lines = Vec::new();

    lines.push(Line::from(vec![
        Span::styled("═══ OPERATION HISTORY ═══", Style::default().fg(Color::Cyan).add_modifier(ratatui::style::Modifier::BOLD)),
    ]));
    lines.push(Line::from(""));

    // Show first 10 entries
    for entry in history.iter().take(10) {
        let version = entry.read_version;
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

        // Add metrics (operation_metrics doesn't exist in deltalake 0.18, skip metrics display)
        if let Some(metrics) = &None::<HashMap<String, serde_json::Value>> {
            let mut metric_strs = Vec::new();
            if let Some(num_added_files) = metrics.get("num_added_files").and_then(|v: &serde_json::Value| v.as_i64()) {
                metric_strs.push(format!("+{} files", num_added_files));
            }
            if let Some(num_removed_files) = metrics.get("num_removed_files").and_then(|v: &serde_json::Value| v.as_i64()) {
                metric_strs.push(format!("-{} files", num_removed_files));
            }
            if let Some(num_added_rows) = metrics.get("num_added_rows").and_then(|v: &serde_json::Value| v.as_i64()) {
                metric_strs.push(format!("+{} rows", num_added_rows));
            }
            if let Some(num_deleted_rows) = metrics.get("num_deleted_rows").and_then(|v: &serde_json::Value| v.as_i64()) {
                metric_strs.push(format!("-{} rows", num_deleted_rows));
            }
            if !metric_strs.is_empty() {
                lines.push(Line::from(vec![
                    Span::styled("  ", Style::default().fg(Color::DarkGray)),
                    Span::raw(metric_strs.join(", ")),
                ]));
            }
        }

        lines.push(Line::from(""));
    }

    let paragraph = Paragraph::new(lines)
        .block(Block::default().borders(Borders::ALL).title("History"))
        .scroll((0, 0));

    f.render_widget(paragraph, area);
}

