use crate::inspector::DeltaTableInspector;
use crate::tui_app::format_bytes;
use anyhow::Result;
use ratatui::{
    layout::Rect,
    style::{Color, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Paragraph},
    Frame,
};

pub fn render(f: &mut Frame, area: Rect, table_path: &str, inspector: &DeltaTableInspector) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let config_result = rt.block_on(inspector.get_configuration());

    let mut lines = Vec::new();

    lines.push(Line::from(vec![
        Span::styled("═══ TABLE CONFIGURATION ═══", Style::default().fg(Color::Cyan).add_modifier(ratatui::style::Modifier::BOLD)),
    ]));
    lines.push(Line::from(""));

    match config_result {
        Ok(config) => {
            // Table Properties
            lines.push(Line::from(vec![
                Span::styled("📋 Table Properties", Style::default().fg(Color::Magenta).add_modifier(ratatui::style::Modifier::BOLD)),
            ]));
            lines.push(Line::from(""));
            if config.table_properties.is_empty() {
                lines.push(Line::from(vec![
                    Span::styled("  ", Style::default().fg(Color::DarkGray)),
                    Span::raw("No custom properties configured"),
                ]));
            } else {
                let mut props: Vec<_> = config.table_properties.iter().collect();
                props.sort_by_key(|(k, _)| *k);
                for (key, value) in props {
                    lines.push(Line::from(vec![
                        Span::styled(format!("  {}: ", key), Style::default().fg(Color::Cyan)),
                        Span::styled(value.clone(), Style::default().fg(Color::Green)),
                    ]));
                }
            }

            // Table Metadata
            lines.push(Line::from(""));
            lines.push(Line::from(vec![
                Span::styled("🏷️  Table Metadata", Style::default().fg(Color::Magenta).add_modifier(ratatui::style::Modifier::BOLD)),
            ]));
            lines.push(Line::from(""));
            if let Some(id) = &config.table_id {
                lines.push(Line::from(vec![
                    Span::styled("  Table ID: ", Style::default().fg(Color::Cyan)),
                    Span::styled(id.clone(), Style::default().fg(Color::Green)),
                ]));
            }
            if let Some(name) = &config.table_name {
                lines.push(Line::from(vec![
                    Span::styled("  Table Name: ", Style::default().fg(Color::Cyan)),
                    Span::styled(name.clone(), Style::default().fg(Color::Green)),
                ]));
            }
            if let Some(desc) = &config.description {
                lines.push(Line::from(vec![
                    Span::styled("  Description: ", Style::default().fg(Color::Cyan)),
                    Span::styled(desc.clone(), Style::default().fg(Color::Green)),
                ]));
            }
            if !config.partition_columns.is_empty() {
                lines.push(Line::from(vec![
                    Span::styled("  Partition Columns: ", Style::default().fg(Color::Cyan)),
                    Span::styled(config.partition_columns.join(", "), Style::default().fg(Color::Green)),
                ]));
            }

            // Protocol Information
            lines.push(Line::from(""));
            lines.push(Line::from(vec![
                Span::styled("⚙️  Protocol Versions", Style::default().fg(Color::Magenta).add_modifier(ratatui::style::Modifier::BOLD)),
            ]));
            lines.push(Line::from(""));
            lines.push(Line::from(vec![
                Span::styled("  Min Reader Version: ", Style::default().fg(Color::Cyan)),
                Span::styled(format!("{}", config.protocol.min_reader_version), Style::default().fg(Color::Green)),
            ]));
            lines.push(Line::from(vec![
                Span::styled("  Min Writer Version: ", Style::default().fg(Color::Cyan)),
                Span::styled(format!("{}", config.protocol.min_writer_version), Style::default().fg(Color::Green)),
            ]));

            if !config.protocol.reader_features.is_empty() {
                lines.push(Line::from(""));
                lines.push(Line::from(vec![
                    Span::styled("  Reader Features: ", Style::default().fg(Color::Cyan)),
                ]));
                for feature in &config.protocol.reader_features {
                    lines.push(Line::from(vec![
                        Span::raw("    • "),
                        Span::raw(feature.clone()),
                    ]));
                }
            }

            if !config.protocol.writer_features.is_empty() {
                lines.push(Line::from(""));
                lines.push(Line::from(vec![
                    Span::styled("  Writer Features: ", Style::default().fg(Color::Cyan)),
                ]));
                for feature in &config.protocol.writer_features {
                    lines.push(Line::from(vec![
                        Span::raw("    • "),
                        Span::raw(feature.clone()),
                    ]));
                }
            }

            // Advanced Features
            lines.push(Line::from(""));
            lines.push(Line::from(vec![
                Span::styled("🚀 Advanced Features", Style::default().fg(Color::Magenta).add_modifier(ratatui::style::Modifier::BOLD)),
            ]));
            lines.push(Line::from(""));

            let features = &config.advanced_features;
            if features.deletion_vectors {
                lines.push(Line::from(vec![
                    Span::styled("  ✓", Style::default().fg(Color::Green)),
                    Span::styled(" Deletion Vectors: ", Style::default().fg(Color::Cyan)),
                    Span::styled("Enabled", Style::default().fg(Color::Green)),
                ]));
            } else {
                lines.push(Line::from(vec![
                    Span::styled("  ✗ Deletion Vectors: Disabled", Style::default().fg(Color::DarkGray)),
                ]));
            }

            if features.column_mapping.enabled {
                lines.push(Line::from(vec![
                    Span::styled("  ✓", Style::default().fg(Color::Green)),
                    Span::styled(" Column Mapping: ", Style::default().fg(Color::Cyan)),
                    Span::styled(features.column_mapping.mode.clone(), Style::default().fg(Color::Green)),
                ]));
            } else {
                lines.push(Line::from(vec![
                    Span::styled("  ✗ Column Mapping: Disabled", Style::default().fg(Color::DarkGray)),
                ]));
            }

            if features.liquid_clustering {
                lines.push(Line::from(vec![
                    Span::styled("  ✓", Style::default().fg(Color::Green)),
                    Span::styled(" Liquid Clustering: ", Style::default().fg(Color::Cyan)),
                    Span::styled("Enabled", Style::default().fg(Color::Green)),
                ]));
            } else {
                lines.push(Line::from(vec![
                    Span::styled("  ✗ Liquid Clustering: Disabled", Style::default().fg(Color::DarkGray)),
                ]));
            }

            if features.auto_optimize.enabled {
                let mut opts = Vec::new();
                if features.auto_optimize.auto_compact {
                    opts.push("auto compact");
                }
                if features.auto_optimize.optimize_write {
                    opts.push("optimize write");
                }
                lines.push(Line::from(vec![
                    Span::styled("  ✓", Style::default().fg(Color::Green)),
                    Span::styled(" Auto Optimize: ", Style::default().fg(Color::Cyan)),
                    Span::styled(opts.join(", "), Style::default().fg(Color::Green)),
                ]));
            } else {
                lines.push(Line::from(vec![
                    Span::styled("  ✗ Auto Optimize: Disabled", Style::default().fg(Color::DarkGray)),
                ]));
            }

            lines.push(Line::from(""));
            lines.push(Line::from(vec![
                Span::styled("  Vacuum Retention: ", Style::default().fg(Color::Cyan)),
                Span::styled(format!("{} hours", features.vacuum_retention_hours), Style::default().fg(Color::Green)),
            ]));
        }
        Err(_) => {
            lines.push(Line::from(vec![
                Span::styled("Loading configuration...", Style::default().fg(Color::DarkGray)),
            ]));
        }
    }

    let paragraph = Paragraph::new(lines)
        .block(Block::default().borders(Borders::ALL).title("Configuration"))
        .scroll((0, 0));

    f.render_widget(paragraph, area);
}

