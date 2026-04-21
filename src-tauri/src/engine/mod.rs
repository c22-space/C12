pub mod scope1;
pub mod scope2;
pub mod scope3;
pub mod intensity;

use rusqlite::{Connection, Result, params};
use serde::{Deserialize, Serialize};

/// Full GHG inventory result for a reporting period.
/// Aggregates all scopes per GRI 305 and ISO 14064-1.
#[derive(Debug, Serialize, Deserialize)]
pub struct PeriodInventory {
    pub period_id: i64,
    pub year: i64,
    pub gwp_ar_version: String,
    pub scope1: scope1::Scope1Total,
    pub scope2: scope2::Scope2Result,
    pub scope3: scope3::Scope3Total,
    /// Combined total: Scope 1 + Scope 2 (location-based) + Scope 3
    pub total_tco2e: f64,
    /// Scope 1 + 2 only (most commonly reported)
    pub scope1_scope2_tco2e: f64,
}

/// Run the full calculation for a reporting period.
/// Recalculates all sources, then aggregates.
pub fn calculate_period(conn: &Connection, period_id: i64) -> Result<PeriodInventory> {
    let (year, gwp_ar): (i64, String) = conn.query_row(
        "SELECT year, gwp_ar_version FROM reporting_periods WHERE id = ?1",
        params![period_id],
        |r| Ok((r.get(0)?, r.get(1)?)),
    )?;

    // Recalculate all sources for this period
    let source_ids: Vec<i64> = {
        let mut stmt = conn.prepare(
            "SELECT id FROM emission_sources WHERE period_id = ?1 AND is_excluded = 0"
        )?;
        let rows = stmt.query_map(params![period_id], |r| r.get(0))?;
        rows.collect::<Result<Vec<_>>>()?
    };

    for id in &source_ids {
        let scope: i64 = conn.query_row(
            "SELECT scope FROM emission_sources WHERE id = ?1",
            params![id],
            |r| r.get(0),
        )?;
        match scope {
            1 => { scope1::calculate_source(conn, *id)?; }
            2 => { scope2::calculate_source(conn, *id)?; }
            3 => { scope3::calculate_source(conn, *id)?; }
            _ => {}
        }
    }

    let scope1 = scope1::aggregate_period(conn, period_id)?;
    let scope2 = scope2::aggregate_period(conn, period_id)?;
    let scope3 = scope3::aggregate_period(conn, period_id)?;

    let scope1_scope2_tco2e = scope1.gross_tco2e + scope2.location_based_tco2e;
    let total_tco2e = scope1_scope2_tco2e + scope3.gross_tco2e;

    Ok(PeriodInventory {
        period_id,
        year,
        gwp_ar_version: gwp_ar,
        scope1,
        scope2,
        scope3,
        total_tco2e,
        scope1_scope2_tco2e,
    })
}
