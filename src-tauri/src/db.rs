use rusqlite::{
    functions::FunctionFlags,
    Connection, Result, params,
};
use std::path::Path;
use std::sync::Mutex;

pub struct Database(pub Mutex<Connection>);

impl Database {
    pub fn open(path: &Path) -> Result<Self> {
        let conn = Connection::open(path)?;
        conn.execute_batch(
            "PRAGMA journal_mode=WAL; PRAGMA foreign_keys=ON; PRAGMA busy_timeout=5000;",
        )?;
        register_math_functions(&conn)?;
        Ok(Self(Mutex::new(conn)))
    }

    pub fn migrate(&self) -> Result<()> {
        let conn = self.0.lock().unwrap();

        // Create migration tracking table if not exists
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS _migrations (
                version INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                applied_at INTEGER NOT NULL DEFAULT (unixepoch())
            );"
        )?;

        let applied: Vec<i64> = {
            let mut stmt = conn.prepare("SELECT version FROM _migrations ORDER BY version")?;
            let rows = stmt.query_map([], |r| r.get(0))?;
            rows.collect::<Result<Vec<_>>>()?
        };

        let migrations: &[(&str, &str)] = &[
            ("001_init", include_str!("../migrations/001_init.sql")),
            ("002_seed", include_str!("../migrations/002_seed.sql")),
        ];

        for (idx, (name, sql)) in migrations.iter().enumerate() {
            let version = (idx + 1) as i64;
            if applied.contains(&version) {
                continue;
            }
            conn.execute_batch(sql)?;
            conn.execute(
                "INSERT INTO _migrations (version, name) VALUES (?1, ?2)",
                params![version, name],
            )?;
        }

        Ok(())
    }
}

#[cfg(test)]
pub(crate) fn test_conn() -> Connection {
    let conn = Connection::open_in_memory().unwrap();
    conn.execute_batch(include_str!("../migrations/001_init.sql")).unwrap();
    register_math_functions(&conn).unwrap();
    conn
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn migrate_creates_core_tables() {
        let dir = tempdir().unwrap();
        let db = Database::open(&dir.path().join("c12.db")).unwrap();
        db.migrate().unwrap();
        let conn = db.0.lock().unwrap();
        for table in &["organizations", "entities", "reporting_periods",
                       "emission_sources", "gwp_values", "emission_factors"] {
            let exists: i64 = conn.query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?1",
                params![table],
                |r| r.get(0),
            ).unwrap();
            assert_eq!(exists, 1, "table '{table}' should exist after migration");
        }
    }

    #[test]
    fn migrate_is_idempotent() {
        let dir = tempdir().unwrap();
        let db = Database::open(&dir.path().join("c12.db")).unwrap();
        db.migrate().unwrap();
        db.migrate().unwrap(); // second run must not error or duplicate data
        let conn = db.0.lock().unwrap();
        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM _migrations",
            [], |r| r.get(0),
        ).unwrap();
        assert_eq!(count, 2, "exactly 2 migration records, not duplicated");
    }

    #[test]
    fn migrate_seeds_gwp_values() {
        let dir = tempdir().unwrap();
        let db = Database::open(&dir.path().join("c12.db")).unwrap();
        db.migrate().unwrap();
        let conn = db.0.lock().unwrap();
        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM gwp_values WHERE ar_version = 'AR6'",
            [], |r| r.get(0),
        ).unwrap();
        assert!(count > 0, "AR6 GWP values should be seeded");
        let co2_gwp: f64 = conn.query_row(
            "SELECT gwp_100 FROM gwp_values WHERE gas = 'CO2' AND ar_version = 'AR6'",
            [], |r| r.get(0),
        ).unwrap();
        assert_eq!(co2_gwp, 1.0, "CO2 GWP is always 1.0");
    }
}

fn register_math_functions(conn: &Connection) -> Result<()> {
    let flags = FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC;
    conn.create_scalar_function("SQRT", 1, flags, |ctx| {
        let x: f64 = ctx.get(0)?;
        Ok(x.sqrt())
    })?;
    conn.create_scalar_function("POWER", 2, flags, |ctx| {
        let base: f64 = ctx.get(0)?;
        let exp: f64 = ctx.get(1)?;
        Ok(base.powf(exp))
    })?;
    Ok(())
}
