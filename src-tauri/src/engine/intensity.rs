// GRI 305-4: GHG Emissions Intensity
// Intensity ratio = Total emissions / Activity metric (e.g. revenue, production units)
// GRI 305-4 requires: which scopes included, denominator metric, Scope 3 intensity separate

use rusqlite::{Connection, Result, params};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct IntensityResult {
    pub includes_scope1: bool,
    pub includes_scope2: bool,
    pub includes_scope3: bool,
    pub total_emissions_tco2e: f64,
    pub metric_name: String,
    pub metric_value: f64,
    pub metric_unit: String,
    /// tCO₂e per unit of metric
    pub intensity_ratio: f64,
    /// GRI 305-4: Scope 3 intensity must be reported separately if calculated
    pub scope3_intensity_ratio: Option<f64>,
}

/// Calculate intensity ratio and store it.
pub fn calculate_and_store(
    conn: &Connection,
    period_id: i64,
    includes_scope1: bool,
    includes_scope2: bool,
    includes_scope3: bool,
    metric_name: &str,
    metric_value: f64,
    metric_unit: &str,
    scope1_tco2e: f64,
    scope2_location_tco2e: f64,
    scope3_tco2e: f64,
) -> Result<IntensityResult> {
    let mut total = 0.0f64;
    if includes_scope1 { total += scope1_tco2e; }
    if includes_scope2 { total += scope2_location_tco2e; }
    if includes_scope3 { total += scope3_tco2e; }

    let intensity_ratio = if metric_value > 0.0 { total / metric_value } else { 0.0 };
    let scope3_intensity_ratio = if includes_scope3 && metric_value > 0.0 {
        Some(scope3_tco2e / metric_value)
    } else {
        None
    };

    // Upsert into intensity_ratios table
    conn.execute(
        "INSERT OR REPLACE INTO intensity_ratios
         (period_id, includes_scope1, includes_scope2, includes_scope3,
          total_emissions_tco2e, metric_name, metric_value, metric_unit,
          intensity_ratio, scope3_intensity_ratio)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
        params![
            period_id,
            includes_scope1 as i64,
            includes_scope2 as i64,
            includes_scope3 as i64,
            total,
            metric_name,
            metric_value,
            metric_unit,
            intensity_ratio,
            scope3_intensity_ratio,
        ],
    )?;

    Ok(IntensityResult {
        includes_scope1,
        includes_scope2,
        includes_scope3,
        total_emissions_tco2e: total,
        metric_name: metric_name.to_string(),
        metric_value,
        metric_unit: metric_unit.to_string(),
        intensity_ratio,
        scope3_intensity_ratio,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::test_conn;
    use rusqlite::params;

    fn setup(conn: &Connection) -> i64 {
        conn.execute(
            "INSERT INTO organizations (name, boundary_method) VALUES ('Acme', 'operational_control')",
            [],
        ).unwrap();
        let org_id = conn.last_insert_rowid();
        conn.execute(
            "INSERT INTO reporting_periods (org_id, year, start_date, end_date)
             VALUES (?1, 2024, '2024-01-01', '2024-12-31')",
            params![org_id],
        ).unwrap();
        conn.last_insert_rowid()
    }

    fn calc(conn: &Connection, period_id: i64,
            s1: bool, s2: bool, s3: bool,
            scope1: f64, scope2: f64, scope3: f64,
            metric: f64) -> IntensityResult {
        calculate_and_store(conn, period_id, s1, s2, s3,
            "Revenue", metric, "USD", scope1, scope2, scope3).unwrap()
    }

    #[test]
    fn intensity_ratio_is_total_over_metric() {
        // GRI 305-4: intensity = tCO2e / metric unit
        // 10 tCO2e scope1 + 5 tCO2e scope2 = 15 tCO2e / 1000 USD = 0.015
        let conn = test_conn();
        let period_id = setup(&conn);
        let r = calc(&conn, period_id, true, true, false, 10.0, 5.0, 0.0, 1000.0);
        assert!((r.intensity_ratio - 0.015).abs() < 1e-9);
        assert!((r.total_emissions_tco2e - 15.0).abs() < 1e-9);
    }

    #[test]
    fn excluded_scopes_not_in_total() {
        let conn = test_conn();
        let period_id = setup(&conn);
        let r = calc(&conn, period_id, true, false, false, 10.0, 999.0, 999.0, 1000.0);
        assert!((r.total_emissions_tco2e - 10.0).abs() < 1e-9);
    }

    #[test]
    fn scope3_intensity_reported_separately() {
        // GRI 305-4: scope 3 intensity must be disclosed separately
        let conn = test_conn();
        let period_id = setup(&conn);
        let r = calc(&conn, period_id, true, true, true, 10.0, 5.0, 3.0, 1000.0);
        let s3_ratio = r.scope3_intensity_ratio.expect("scope3 ratio must be present");
        assert!((s3_ratio - 0.003).abs() < 1e-9);
    }

    #[test]
    fn scope3_intensity_absent_when_scope3_excluded() {
        let conn = test_conn();
        let period_id = setup(&conn);
        let r = calc(&conn, period_id, true, true, false, 10.0, 5.0, 3.0, 1000.0);
        assert!(r.scope3_intensity_ratio.is_none());
    }

    #[test]
    fn zero_metric_returns_zero_ratio() {
        let conn = test_conn();
        let period_id = setup(&conn);
        let r = calc(&conn, period_id, true, true, false, 10.0, 5.0, 0.0, 0.0);
        assert_eq!(r.intensity_ratio, 0.0);
    }
}
