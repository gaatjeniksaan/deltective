# Deltective Usability Review

This document identifies usability issues and recommended improvements for the Rust-based deltective TUI application.

## Critical Issues

### 1. History Tab Pagination is Non-Functional
**Location:** `rust/src/tui_app.rs:136-146`

The README and code structure indicate pagination keys (`n` for next page, `p` for previous, `r` for reverse sort), but the handlers are empty:

```rust
KeyCode::Char('n') => {
    // Next page - would need to track page state
}
KeyCode::Char('p') => {
    // Previous page
}
KeyCode::Char('r') => {
    // Reverse sort
}
```

**Impact:** Users expect pagination to work based on documentation but it does nothing. Only the first 10 history entries are ever visible.

**Fix:** Implement pagination state tracking and update the history view accordingly.

---

### 2. History Tab Metrics Display is Dead Code
**Location:** `rust/src/tui_app/history.rs:60`

The metrics section uses a hardcoded `None` value:
```rust
if let Some(metrics) = &None::<HashMap<String, serde_json::Value>> {
```

This block will never execute, so operation metrics (files added/removed, rows changed) are never displayed.

**Impact:** Users don't see important operation metrics that help them understand what happened in each version.

**Fix:** Either implement metrics extraction from the deltalake API or remove this dead code to avoid confusion.

---

### 3. No Scrolling Support in Any Tab
**Location:** All tab render functions (overview.rs:165, history.rs:87, insights.rs:96, configuration.rs:196, timeline.rs:168)

All tabs use hardcoded scroll position:
```rust
.scroll((0, 0));
```

**Impact:** Content that extends beyond the terminal viewport is invisible. Users cannot scroll down to see:
- Schema columns on tables with many fields
- Full history entries
- All insights/recommendations
- Complete configuration details

**Fix:** Add scroll state to the `App` struct and handle `Up`/`Down`/`PageUp`/`PageDown` keys per tab.

---

### 4. Configuration and Timeline Tabs Block UI on Every Frame
**Location:** `rust/src/tui_app/configuration.rs:13`, `rust/src/tui_app/timeline.rs:12`

Both tabs create a new tokio runtime and fetch data synchronously during render:
```rust
let rt = tokio::runtime::Runtime::new().unwrap();
let config_result = rt.block_on(inspector.get_configuration());
```

**Impact:** The UI freezes/stutters every time these tabs render. For remote tables or large tables, this causes noticeable lag.

**Fix:** Fetch configuration and timeline data once during initialization and store in `App` state, similar to how stats and history are handled.

---

## Moderate Issues

### 5. No Loading Indicator During Table Open
**Location:** `rust/src/tui_app.rs:31-35`

When opening a table, there's no visual feedback:
```rust
let inspector = rt.block_on(DeltaTableInspector::new(table_path))?;
let stats = rt.block_on(inspector.get_statistics())?;
```

**Impact:** Users see a blank/black screen while loading, especially for large or remote tables. They may think the application has frozen.

**Fix:** Show a loading spinner or "Loading table..." message before the TUI fully initializes.

---

### 6. Mouse Capture Enabled But Unused
**Location:** `rust/src/tui_app.rs:28`

Mouse capture is enabled but no mouse events are handled:
```rust
crossterm::event::EnableMouseCapture
```

**Impact:** Misleading behavior - mouse is captured but clicks/scrolling do nothing.

**Fix:** Either implement mouse-based tab switching and scrolling, or remove mouse capture.

---

### 7. Generic Error Messages
**Location:** `rust/src/cli.rs:24-27`, `rust/src/inspector.rs:66-70`

Error messages lack specificity:
```rust
eprintln!("Error: Path does not exist: {}", table_path);
// ...
.context("Failed to open Delta table")?
```

**Impact:** Users don't know *why* a table failed to open (permissions, not a Delta table, missing _delta_log, network error, etc.)

**Fix:** Provide specific error messages:
- "Path exists but is not a Delta table (no _delta_log directory)"
- "Permission denied reading table"
- "Network error connecting to Azure storage"

---

### 8. CLI Lacks Useful Options
**Location:** `rust/src/cli.rs:7-16`

The CLI only supports one positional argument. Missing common options:

**Impact:** Power users can't:
- Get JSON output for scripting (`--json`)
- Inspect a specific version (`--version N`)
- Run non-interactive commands (`--stats`, `--insights`)
- Set verbosity level

**Fix:** Add clap subcommands or flags:
```
deltective /path/to/table --json --stats
deltective /path/to/table --version 42
deltective /path/to/table insights --format json
```

---

### 9. No Keybinding Help in TUI
**Location:** All TUI rendering code

The interface doesn't show available keybindings.

**Impact:** Users must read external documentation or guess what keys are available. Discoverability is poor.

**Fix:** Add a help bar at the bottom of the screen:
```
Tab/←→: Switch tabs | ↑↓: Scroll | n/p: Next/Prev page | r: Reverse | q: Quit | ?: Help
```

---

### 10. Write Pattern Analysis Disabled
**Location:** `rust/src/inspector.rs:461`

Small frequent writes detection is disabled with dead code:
```rust
if false { // Disabled since operation_metrics unavailable
    patterns.push("Small frequent writes detected (avg < 1000 rows)".to_string());
}
```

**Impact:** Users don't get warnings about small frequent write patterns.

**Fix:** Remove dead code or find alternative detection method using available API.

---

### 11. Azure Authentication Incomplete
**Location:** `rust/src/inspector.rs:79-90`

Azure storage support is stubbed out:
```rust
if table_path.starts_with("abfss://") || table_path.starts_with("az://") {
    // Azure storage support would be implemented here
    // For now, return None and let deltalake handle it
    Ok(None)
}
```

**Impact:** Azure tables may not work reliably; users don't know what authentication methods are supported.

**Fix:** Implement Azure credential support or clearly document current limitations.

---

### 12. Schema Column Order Not Preserved
**Location:** `rust/src/inspector.rs:211-224`

Schema uses `HashMap<String, String>` which doesn't preserve column order:
```rust
let mut result = HashMap::new();
for field in arrow_schema.fields() {
    result.insert(field.name().clone(), type_str);
}
```

**Impact:** Schema columns appear in arbitrary order, making it hard to understand table structure.

**Fix:** Use `Vec<(String, String)>` or `IndexMap` to preserve original schema order.

---

## Minor Issues

### 13. Inconsistent Section Styling
Some tabs use emojis (Configuration: 📋 🏷️ ⚙️ 🚀, Timeline: 📊 📈 🔍 💡), while Overview and History use only text borders (═══).

**Fix:** Standardize styling across all tabs.

---

### 14. No Way to Copy Values
Users may want to copy table ID, paths, or generated SQL commands but the TUI doesn't support copying.

**Fix:** Add a "copy to clipboard" feature for important values, or show values in a format easy to manually copy.

---

### 15. Timestamps Always in UTC
**Location:** All timestamp formatting uses `.format("%Y-%m-%d %H:%M:%S")` with UTC times.

**Impact:** Users in different timezones must mentally convert times.

**Fix:** Add option to display times in local timezone.

---

## Summary

| Priority | Count | Issues |
|----------|-------|--------|
| Critical | 4 | Broken pagination, dead metrics code, no scrolling, blocking UI renders |
| Moderate | 8 | No loading indicator, unused mouse capture, generic errors, missing CLI options, no keybinding help, disabled features, incomplete Azure, schema order |
| Minor | 3 | Inconsistent styling, no copy, UTC timestamps |

## Recommended Priority

1. **First:** Fix scrolling and pagination - these block users from seeing all content
2. **Second:** Fix blocking renders in Configuration/Timeline tabs - causes poor UX
3. **Third:** Add loading indicator and improve error messages
4. **Fourth:** Add keybinding help bar for discoverability
5. **Fifth:** Address remaining moderate and minor issues
