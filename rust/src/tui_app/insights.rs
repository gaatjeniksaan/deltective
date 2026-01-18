use crate::inspector::TableStatistics;
use crate::insights::{DeltaTableAnalyzer, Insight};
use ratatui::{
    layout::Rect,
    style::{Color, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Paragraph},
    Frame,
};

pub fn render(f: &mut Frame, area: Rect, stats: &TableStatistics, scroll: u16) {
    let analyzer = DeltaTableAnalyzer::new(stats.clone());
    let insights = analyzer.analyze();

    let mut lines = Vec::new();

    lines.push(Line::from(vec![
        Span::styled("═══ TABLE HEALTH & RECOMMENDATIONS ═══", Style::default().fg(Color::Cyan).add_modifier(ratatui::style::Modifier::BOLD)),
    ]));
    lines.push(Line::from(""));

    // Group by severity
    let critical: Vec<&Insight> = insights.iter().filter(|i| i.severity == "critical").collect();
    let warnings: Vec<&Insight> = insights.iter().filter(|i| i.severity == "warning").collect();
    let info: Vec<&Insight> = insights.iter().filter(|i| i.severity == "info").collect();
    let good: Vec<&Insight> = insights.iter().filter(|i| i.severity == "good").collect();

    // Display critical issues first
    if !critical.is_empty() {
        lines.push(Line::from(vec![
            Span::styled("🔴 CRITICAL ISSUES", Style::default().fg(Color::Red).add_modifier(ratatui::style::Modifier::BOLD)),
        ]));
        lines.push(Line::from(""));
        for insight in &critical {
            lines.extend(format_insight(insight));
            lines.push(Line::from(""));
        }
    }

    // Display warnings
    if !warnings.is_empty() {
        lines.push(Line::from(vec![
            Span::styled("⚠️  WARNINGS", Style::default().fg(Color::Yellow).add_modifier(ratatui::style::Modifier::BOLD)),
        ]));
        lines.push(Line::from(""));
        for insight in &warnings {
            lines.extend(format_insight(insight));
            lines.push(Line::from(""));
        }
    }

    // Display info/recommendations
    if !info.is_empty() {
        lines.push(Line::from(vec![
            Span::styled("ℹ️  RECOMMENDATIONS", Style::default().fg(Color::Green).add_modifier(ratatui::style::Modifier::BOLD)),
        ]));
        lines.push(Line::from(""));
        for insight in &info {
            lines.extend(format_insight(insight));
            lines.push(Line::from(""));
        }
    }

    // Display positive feedback
    if !good.is_empty() {
        lines.push(Line::from(vec![
            Span::styled("✅ GOOD CONFIGURATION", Style::default().fg(Color::Green).add_modifier(ratatui::style::Modifier::BOLD)),
        ]));
        lines.push(Line::from(""));
        for insight in &good {
            lines.extend(format_insight(insight));
            lines.push(Line::from(""));
        }
    }

    // Summary
    lines.push(Line::from(""));
    lines.push(Line::from(vec![
        Span::styled("═══ SUMMARY ═══", Style::default().fg(Color::Cyan).add_modifier(ratatui::style::Modifier::BOLD)),
    ]));
    lines.push(Line::from(vec![
        Span::styled("  Critical: ", Style::default().fg(Color::Red)),
        Span::raw(format!("{}", critical.len())),
    ]));
    lines.push(Line::from(vec![
        Span::styled("  Warnings: ", Style::default().fg(Color::Yellow)),
        Span::raw(format!("{}", warnings.len())),
    ]));
    lines.push(Line::from(vec![
        Span::styled("  Info: ", Style::default().fg(Color::Green)),
        Span::raw(format!("{}", info.len())),
    ]));

    let paragraph = Paragraph::new(lines)
        .block(Block::default().borders(Borders::ALL).title("Insights [↑↓ scroll]"))
        .scroll((scroll, 0));

    f.render_widget(paragraph, area);
}

fn format_insight(insight: &Insight) -> Vec<Line> {
    let mut lines = Vec::new();

    let (icon, title_color) = match insight.severity.as_str() {
        "critical" => ("🚨", Color::Red),
        "warning" => ("⚠️", Color::Yellow),
        "info" => ("💡", Color::Green),
        _ => ("✓", Color::Green),
    };

    lines.push(Line::from(vec![
        Span::styled(format!("{} {}", icon, insight.title), Style::default().fg(title_color).add_modifier(ratatui::style::Modifier::BOLD)),
    ]));
    lines.push(Line::from(vec![
        Span::styled(format!("Category: {}", insight.category), Style::default().fg(Color::DarkGray)),
    ]));
    lines.push(Line::from(""));
    lines.push(Line::from(insight.description.clone()));
    lines.push(Line::from(""));
    lines.push(Line::from(vec![
        Span::styled("→ Recommendation: ", Style::default().fg(Color::Cyan)),
        Span::raw(insight.recommendation.clone()),
    ]));

    lines
}

