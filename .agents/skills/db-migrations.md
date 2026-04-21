# Skill: DB Migrations
SQLite migration and connection conventions for C12's rusqlite backend.

## When to use
When adding tables, columns, or indexes, or when writing queries in `src-tauri/src/`.

## Instructions

### Migrations
- Migration files live in `src-tauri/migrations/` as numbered SQL files: `0001_init.sql`, `0002_add_column.sql`, etc.
- **Never modify an existing migration file** — always append a new numbered file
- Migrations run in numeric order at startup via `database.migrate()` in `lib.rs`
- Each migration file must be idempotent where possible (`CREATE TABLE IF NOT EXISTS`, `CREATE INDEX IF NOT EXISTS`)

### Connection sharing
- The single `Database(Mutex<Connection>)` is managed by Tauri's state system
- Acquire the lock with `let conn = db.0.lock().unwrap();` and release it as soon as possible
- Never hold the lock across an `await` — this is sync code
- Custom math functions (`SQRT`, `POWER`) are registered once at connection open time in `db.rs` — don't re-register them

### Query patterns
```rust
// Multi-row
let mut stmt = conn.prepare("SELECT id, name FROM organizations WHERE active = 1")?;
let rows = stmt.query_map([], |row| Ok(Org { id: row.get(0)?, name: row.get(1)? }))?
    .collect::<rusqlite::Result<Vec<_>>>()?;

// Single row
let name: String = conn.query_row(
    "SELECT name FROM organizations WHERE id = ?1",
    params![id],
    |row| row.get(0),
)?;

// Insert + get ID
conn.execute("INSERT INTO ...", params![...])?;
let id = conn.last_insert_rowid();
```

### What not to do
- No string interpolation in SQL — always use `params![...]` or `?1`/`?2` placeholders
- No `SELECT *` — always name columns explicitly for forward-compatibility
- No raw `Connection::open` inside commands — use the managed `Database` state
