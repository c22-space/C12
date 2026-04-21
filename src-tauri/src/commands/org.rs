use crate::db::Database;
use crate::error::Result;
use rusqlite::params;
use serde::{Deserialize, Serialize};
use tauri::State;

#[derive(Debug, Serialize, Deserialize)]
pub struct Organization {
    pub id: i64,
    pub name: String,
    pub boundary_method: String,
    pub base_year: Option<i64>,
    pub reporting_currency: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Entity {
    pub id: i64,
    pub org_id: i64,
    pub name: String,
    pub r#type: String,
    pub ownership_pct: Option<f64>,
    pub is_financially_controlled: bool,
    pub is_operationally_controlled: bool,
    pub country_code: Option<String>,
    pub sector_gri: Option<i64>,
    pub is_active: bool,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ReportingPeriod {
    pub id: i64,
    pub org_id: i64,
    pub year: i64,
    pub start_date: String,
    pub end_date: String,
    pub status: String,
    pub gwp_ar_version: String,
}

// ── Organization commands ────────────────────────────────────────────────────

#[tauri::command]
pub fn create_org(
    db: State<Database>,
    name: String,
    boundary_method: String,
    base_year: Option<i64>,
    reporting_currency: Option<String>,
) -> Result<Organization> {
    let conn = db.0.lock().unwrap();
    conn.execute(
        "INSERT INTO organizations (name, boundary_method, base_year, reporting_currency)
         VALUES (?1, ?2, ?3, ?4)",
        params![
            name,
            boundary_method,
            base_year,
            reporting_currency.unwrap_or_else(|| "USD".into()),
        ],
    )?;
    let id = conn.last_insert_rowid();
    get_org_inner(&conn, id)
}

#[tauri::command]
pub fn list_orgs(db: State<Database>) -> Result<Vec<Organization>> {
    let conn = db.0.lock().unwrap();
    let mut stmt = conn.prepare(
        "SELECT id, name, boundary_method, base_year, reporting_currency FROM organizations ORDER BY name"
    )?;
    let orgs = stmt.query_map([], |row| {
        Ok(Organization {
            id: row.get(0)?,
            name: row.get(1)?,
            boundary_method: row.get(2)?,
            base_year: row.get(3)?,
            reporting_currency: row.get(4)?,
        })
    })?.collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(orgs)
}

#[tauri::command]
pub fn update_org(
    db: State<Database>,
    id: i64,
    name: String,
    boundary_method: String,
    base_year: Option<i64>,
) -> Result<()> {
    let conn = db.0.lock().unwrap();
    conn.execute(
        "UPDATE organizations SET name = ?1, boundary_method = ?2, base_year = ?3, updated_at = unixepoch()
         WHERE id = ?4",
        params![name, boundary_method, base_year, id],
    )?;
    Ok(())
}

fn get_org_inner(conn: &rusqlite::Connection, id: i64) -> Result<Organization> {
    Ok(conn.query_row(
        "SELECT id, name, boundary_method, base_year, reporting_currency FROM organizations WHERE id = ?1",
        params![id],
        |row| Ok(Organization {
            id: row.get(0)?,
            name: row.get(1)?,
            boundary_method: row.get(2)?,
            base_year: row.get(3)?,
            reporting_currency: row.get(4)?,
        }),
    )?)
}

// ── Entity commands ──────────────────────────────────────────────────────────

#[tauri::command]
pub fn create_entity(
    db: State<Database>,
    org_id: i64,
    name: String,
    r#type: String,
    ownership_pct: Option<f64>,
    is_financially_controlled: bool,
    is_operationally_controlled: bool,
    country_code: Option<String>,
    sector_gri: Option<i64>,
) -> Result<Entity> {
    let conn = db.0.lock().unwrap();
    conn.execute(
        "INSERT INTO entities (org_id, name, type, ownership_pct, is_financially_controlled,
          is_operationally_controlled, country_code, sector_gri)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        params![
            org_id, name, r#type, ownership_pct,
            is_financially_controlled as i64, is_operationally_controlled as i64,
            country_code, sector_gri,
        ],
    )?;
    let id = conn.last_insert_rowid();
    get_entity_inner(&conn, id)
}

#[tauri::command]
pub fn list_entities(db: State<Database>, org_id: i64) -> Result<Vec<Entity>> {
    let conn = db.0.lock().unwrap();
    let mut stmt = conn.prepare(
        "SELECT id, org_id, name, type, ownership_pct, is_financially_controlled,
                is_operationally_controlled, country_code, sector_gri, is_active
         FROM entities WHERE org_id = ?1 AND is_active = 1 ORDER BY name"
    )?;
    let entities = stmt.query_map(params![org_id], |row| {
        Ok(Entity {
            id: row.get(0)?,
            org_id: row.get(1)?,
            name: row.get(2)?,
            r#type: row.get(3)?,
            ownership_pct: row.get(4)?,
            is_financially_controlled: row.get::<_, i64>(5)? != 0,
            is_operationally_controlled: row.get::<_, i64>(6)? != 0,
            country_code: row.get(7)?,
            sector_gri: row.get(8)?,
            is_active: row.get::<_, i64>(9)? != 0,
        })
    })?.collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(entities)
}

fn get_entity_inner(conn: &rusqlite::Connection, id: i64) -> Result<Entity> {
    Ok(conn.query_row(
        "SELECT id, org_id, name, type, ownership_pct, is_financially_controlled,
                is_operationally_controlled, country_code, sector_gri, is_active
         FROM entities WHERE id = ?1",
        params![id],
        |row| Ok(Entity {
            id: row.get(0)?,
            org_id: row.get(1)?,
            name: row.get(2)?,
            r#type: row.get(3)?,
            ownership_pct: row.get(4)?,
            is_financially_controlled: row.get::<_, i64>(5)? != 0,
            is_operationally_controlled: row.get::<_, i64>(6)? != 0,
            country_code: row.get(7)?,
            sector_gri: row.get(8)?,
            is_active: row.get::<_, i64>(9)? != 0,
        }),
    )?)
}

// ── Reporting Period commands ─────────────────────────────────────────────────

#[tauri::command]
pub fn create_period(
    db: State<Database>,
    org_id: i64,
    year: i64,
    start_date: String,
    end_date: String,
    gwp_ar_version: Option<String>,
) -> Result<ReportingPeriod> {
    let conn = db.0.lock().unwrap();
    conn.execute(
        "INSERT INTO reporting_periods (org_id, year, start_date, end_date, gwp_ar_version)
         VALUES (?1, ?2, ?3, ?4, ?5)",
        params![
            org_id, year, start_date, end_date,
            gwp_ar_version.unwrap_or_else(|| "AR6".into()),
        ],
    )?;
    let id = conn.last_insert_rowid();
    Ok(conn.query_row(
        "SELECT id, org_id, year, start_date, end_date, status, gwp_ar_version
         FROM reporting_periods WHERE id = ?1",
        params![id],
        |row| Ok(ReportingPeriod {
            id: row.get(0)?, org_id: row.get(1)?, year: row.get(2)?,
            start_date: row.get(3)?, end_date: row.get(4)?,
            status: row.get(5)?, gwp_ar_version: row.get(6)?,
        }),
    )?)
}

#[tauri::command]
pub fn list_periods(db: State<Database>, org_id: i64) -> Result<Vec<ReportingPeriod>> {
    let conn = db.0.lock().unwrap();
    let mut stmt = conn.prepare(
        "SELECT id, org_id, year, start_date, end_date, status, gwp_ar_version
         FROM reporting_periods WHERE org_id = ?1 ORDER BY year DESC"
    )?;
    let periods = stmt.query_map(params![org_id], |row| {
        Ok(ReportingPeriod {
            id: row.get(0)?, org_id: row.get(1)?, year: row.get(2)?,
            start_date: row.get(3)?, end_date: row.get(4)?,
            status: row.get(5)?, gwp_ar_version: row.get(6)?,
        })
    })?.collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(periods)
}
