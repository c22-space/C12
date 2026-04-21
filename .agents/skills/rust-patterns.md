# Skill: Rust Patterns
Error handling, type safety, and Tauri command conventions for C12.

## When to use
When writing or modifying any Rust code in `src-tauri/src/` or `worker/src/`.

## Instructions

### Error handling
- Always propagate errors with `?` — never swallow with `let _ =` unless the operation is genuinely fire-and-forget (log it if so)
- Tauri commands return `Result<T, AppError>`, never `Result<T, String>`
- `AppError` is defined in `src/error.rs` and implements `serde::Serialize` so Tauri can serialise it to the frontend
- Use `match` or `if let` with meaningful fallbacks when a missing row is a valid state (e.g. `QueryReturnedNoRows → Ok(None)`)

```rust
// Correct
#[tauri::command]
pub fn get_period(db: State<Database>, id: i64) -> Result<Option<ReportingPeriod>, AppError> {
    let conn = db.0.lock().unwrap();
    match conn.query_row("SELECT ...", params![id], |row| Ok(...)) {
        Ok(p) => Ok(Some(p)),
        Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
        Err(e) => Err(AppError::Database(e)),
    }
}
```

### Tauri commands
- Every command takes `db: State<Database>` as the first arg if it touches the DB
- Args structs use `#[serde(rename_all = "camelCase")]` — Tauri v2 sends camelCase on the wire
- Return types use explicit structs, never `serde_json::Value`

```rust
#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateSourceInput {
    pub period_id: i64,
    pub scope: i32,
    pub activity_value: f64,
}
```

### Numeric types
| Use case | Type |
|---|---|
| Emission values (tCO₂e, GWP, activity) | `f64` |
| IDs, timestamps (Unix epoch) | `i64` |
| Years, scope numbers | `i32` |
| Percentages stored as 0–100 | `f64` |

### Minimal surface area
- No helper functions unless called from ≥2 call sites
- No `mod utils`, `mod helpers`, or catch-all modules
- No `#[allow(dead_code)]` — delete unused code instead
- Dead code that might be needed later belongs in a branch, not commented-out in main

### What not to do
- No `.unwrap()` in production paths — always `?` or handle the error
- No `serde_json::Value` in command return types
- No re-exports just for backwards compatibility — change the callers
