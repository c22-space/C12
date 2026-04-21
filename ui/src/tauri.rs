use js_sys::Promise;
use serde::Serialize;
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::JsFuture;

use crate::types::*;

#[wasm_bindgen]
extern "C" {
    #[wasm_bindgen(js_namespace = ["window", "__TAURI__", "core"], js_name = "invoke")]
    fn tauri_invoke_raw(cmd: &str, args: JsValue) -> Promise;

    #[wasm_bindgen(js_namespace = ["window", "__TAURI__", "shell"], js_name = "open")]
    fn shell_open_raw(url: &str, open_with: JsValue) -> Promise;
}

async fn invoke<A, R>(cmd: &str, args: &A) -> Result<R, String>
where
    A: Serialize,
    R: for<'de> serde::Deserialize<'de>,
{
    let args_js = serde_wasm_bindgen::to_value(args).map_err(|e| e.to_string())?;
    let promise = tauri_invoke_raw(cmd, args_js);
    let result = JsFuture::from(promise)
        .await
        .map_err(|e| e.as_string().unwrap_or_else(|| "Tauri error".to_string()))?;
    serde_wasm_bindgen::from_value(result).map_err(|e| e.to_string())
}

pub async fn shell_open(url: &str) {
    let _ = JsFuture::from(shell_open_raw(url, JsValue::UNDEFINED)).await;
}

// ── Org commands ─────────────────────────────────────────────────────────────

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct CreateOrgArgs {
    name: String,
    boundary_method: String,
    base_year: Option<i32>,
    reporting_currency: String,
}

pub async fn create_org(
    name: &str,
    boundary_method: &str,
    base_year: Option<i32>,
    reporting_currency: &str,
) -> Result<Organization, String> {
    invoke(
        "create_org",
        &CreateOrgArgs {
            name: name.into(),
            boundary_method: boundary_method.into(),
            base_year,
            reporting_currency: reporting_currency.into(),
        },
    )
    .await
}

pub async fn list_orgs() -> Result<Vec<Organization>, String> {
    invoke("list_orgs", &()).await
}


#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct UpdateOrgArgs {
    id: i64,
    name: String,
    boundary_method: String,
    base_year: Option<i32>,
}

pub async fn update_org(
    id: i64,
    name: &str,
    boundary_method: &str,
    base_year: Option<i32>,
) -> Result<(), String> {
    invoke(
        "update_org",
        &UpdateOrgArgs {
            id,
            name: name.into(),
            boundary_method: boundary_method.into(),
            base_year,
        },
    )
    .await
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct CreateEntityArgs {
    org_id: i64,
    name: String,
    #[serde(rename = "type")]
    entity_type: String,
    ownership_pct: Option<f64>,
    is_financially_controlled: bool,
    is_operationally_controlled: bool,
    country_code: Option<String>,
    sector_gri: Option<i32>,
}

pub async fn create_entity(
    org_id: i64,
    name: &str,
    entity_type: &str,
    ownership_pct: Option<f64>,
    is_financially_controlled: bool,
    is_operationally_controlled: bool,
    country_code: Option<String>,
) -> Result<Entity, String> {
    invoke(
        "create_entity",
        &CreateEntityArgs {
            org_id,
            name: name.into(),
            entity_type: entity_type.into(),
            ownership_pct,
            is_financially_controlled,
            is_operationally_controlled,
            country_code,
            sector_gri: None,
        },
    )
    .await
}

pub async fn list_entities(org_id: i64) -> Result<Vec<Entity>, String> {
    invoke("list_entities", &serde_json::json!({ "orgId": org_id })).await
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct CreatePeriodArgs {
    org_id: i64,
    year: i32,
    start_date: String,
    end_date: String,
    gwp_ar_version: String,
}

pub async fn create_period(
    org_id: i64,
    year: i32,
    gwp_ar_version: &str,
) -> Result<ReportingPeriod, String> {
    invoke(
        "create_period",
        &CreatePeriodArgs {
            org_id,
            year,
            start_date: format!("{year}-01-01"),
            end_date: format!("{year}-12-31"),
            gwp_ar_version: gwp_ar_version.into(),
        },
    )
    .await
}

pub async fn list_periods(org_id: i64) -> Result<Vec<ReportingPeriod>, String> {
    invoke("list_periods", &serde_json::json!({ "orgId": org_id })).await
}

// ── Source commands ───────────────────────────────────────────────────────────

#[derive(Serialize)]
pub struct CreateSourceInput {
    pub entity_id: i64,
    pub period_id: i64,
    pub scope: i32,
    pub scope2_method: Option<String>,
    pub scope3_category: Option<i32>,
    pub category_name: String,
    pub ghg_type: String,
    pub activity_value: f64,
    pub activity_unit: String,
    pub activity_source: Option<String>,
    pub emission_factor_value: f64,
    pub emission_factor_unit: String,
    pub emission_factor_source: String,
    pub emission_factor_citation: Option<String>,
    pub gwp_value: f64,
    pub biogenic_co2_tco2e: Option<f64>,
    pub uncertainty_pct: Option<f64>,
    pub notes: Option<String>,
}

pub async fn create_source(input: CreateSourceInput) -> Result<EmissionSource, String> {
    invoke("create_source", &serde_json::json!({ "input": input })).await
}

pub async fn delete_source(id: i64, reason: &str) -> Result<(), String> {
    invoke(
        "delete_source",
        &serde_json::json!({ "id": id, "reason": reason }),
    )
    .await
}

pub async fn list_sources(
    period_id: i64,
    scope: Option<i32>,
) -> Result<Vec<EmissionSource>, String> {
    invoke(
        "list_sources",
        &serde_json::json!({ "periodId": period_id, "scope": scope }),
    )
    .await
}

// ── Calculation commands ──────────────────────────────────────────────────────

pub async fn calculate_period(period_id: i64) -> Result<PeriodInventory, String> {
    invoke(
        "calculate_period",
        &serde_json::json!({ "periodId": period_id }),
    )
    .await
}

pub async fn get_audit_log(table_name: &str, record_id: i64) -> Result<Vec<AuditEntry>, String> {
    invoke(
        "get_audit_log",
        &serde_json::json!({ "tableName": table_name, "recordId": record_id }),
    )
    .await
}

// ── Intensity commands (GRI 305-4) ────────────────────────────────────────────

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct SaveIntensityArgs {
    period_id: i64,
    metric_name: String,
    metric_value: f64,
    metric_unit: String,
    includes_scope1: bool,
    includes_scope2: bool,
    includes_scope3: bool,
}

pub async fn save_intensity_metric(
    period_id: i64,
    metric_name: &str,
    metric_value: f64,
    metric_unit: &str,
    includes_scope1: bool,
    includes_scope2: bool,
    includes_scope3: bool,
) -> Result<IntensityResult, String> {
    invoke(
        "save_intensity_metric",
        &SaveIntensityArgs {
            period_id,
            metric_name: metric_name.into(),
            metric_value,
            metric_unit: metric_unit.into(),
            includes_scope1,
            includes_scope2,
            includes_scope3,
        },
    )
    .await
}

pub async fn list_intensity_results(period_id: i64) -> Result<Vec<IntensityResult>, String> {
    invoke(
        "list_intensity_results",
        &serde_json::json!({ "periodId": period_id }),
    )
    .await
}

pub async fn delete_intensity_result(
    period_id: i64,
    metric_name: &str,
) -> Result<(), String> {
    invoke(
        "delete_intensity_result",
        &serde_json::json!({ "periodId": period_id, "metricName": metric_name }),
    )
    .await
}

// ── Reduction commands (GRI 305-5) ────────────────────────────────────────────

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct CreateReductionArgs {
    period_id: i64,
    baseline_year: i32,
    baseline_tco2e: f64,
    current_tco2e: f64,
    methodology: String,
}

pub async fn list_reductions(period_id: i64) -> Result<Vec<Reduction>, String> {
    invoke(
        "list_reductions",
        &serde_json::json!({ "periodId": period_id }),
    )
    .await
}

pub async fn create_reduction(
    period_id: i64,
    baseline_year: i32,
    baseline_tco2e: f64,
    current_tco2e: f64,
    methodology: &str,
) -> Result<Reduction, String> {
    invoke(
        "create_reduction",
        &CreateReductionArgs {
            period_id,
            baseline_year,
            baseline_tco2e,
            current_tco2e,
            methodology: methodology.into(),
        },
    )
    .await
}

pub async fn delete_reduction(id: i64) -> Result<(), String> {
    invoke("delete_reduction", &serde_json::json!({ "id": id })).await
}

// ── ODS commands (GRI 305-6) ──────────────────────────────────────────────────

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct CreateOdsArgs {
    period_id: i64,
    substance: String,
    production_metric_tons: f64,
    imports_metric_tons: f64,
    exports_metric_tons: f64,
    cfc11_equivalent: f64,
}

pub async fn list_ods_emissions(period_id: i64) -> Result<Vec<OdsEntry>, String> {
    invoke(
        "list_ods_emissions",
        &serde_json::json!({ "periodId": period_id }),
    )
    .await
}

pub async fn create_ods_emission(
    period_id: i64,
    substance: &str,
    production_metric_tons: f64,
    imports_metric_tons: f64,
    exports_metric_tons: f64,
    cfc11_equivalent: f64,
) -> Result<OdsEntry, String> {
    invoke(
        "create_ods_emission",
        &CreateOdsArgs {
            period_id,
            substance: substance.into(),
            production_metric_tons,
            imports_metric_tons,
            exports_metric_tons,
            cfc11_equivalent,
        },
    )
    .await
}

pub async fn delete_ods_emission(id: i64) -> Result<(), String> {
    invoke("delete_ods_emission", &serde_json::json!({ "id": id })).await
}

// ── Air emission commands (GRI 305-7) ─────────────────────────────────────────

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct CreateAirArgs {
    period_id: i64,
    emission_type: String,
    substance: Option<String>,
    value_metric_tons: f64,
    measurement_method: String,
}

pub async fn list_air_emissions(period_id: i64) -> Result<Vec<AirEntry>, String> {
    invoke(
        "list_air_emissions",
        &serde_json::json!({ "periodId": period_id }),
    )
    .await
}

pub async fn create_air_emission(
    period_id: i64,
    emission_type: &str,
    substance: Option<String>,
    value_metric_tons: f64,
    measurement_method: &str,
) -> Result<AirEntry, String> {
    invoke(
        "create_air_emission",
        &CreateAirArgs {
            period_id,
            emission_type: emission_type.into(),
            substance,
            value_metric_tons,
            measurement_method: measurement_method.into(),
        },
    )
    .await
}

pub async fn delete_air_emission(id: i64) -> Result<(), String> {
    invoke("delete_air_emission", &serde_json::json!({ "id": id })).await
}

// ── Report commands ───────────────────────────────────────────────────────────

pub async fn generate_gri305_report(period_id: i64) -> Result<ReportData, String> {
    invoke(
        "generate_gri305_report",
        &serde_json::json!({ "periodId": period_id }),
    )
    .await
}

pub async fn export_sources_csv(period_id: i64, path: &str) -> Result<(), String> {
    invoke(
        "export_sources_csv",
        &serde_json::json!({ "periodId": period_id, "path": path }),
    )
    .await
}

// ── UNGC commands ─────────────────────────────────────────────────────────────

pub async fn init_cop(org_id: i64, reporting_year: i32) -> Result<Cop, String> {
    invoke(
        "init_cop",
        &serde_json::json!({ "orgId": org_id, "reportingYear": reporting_year }),
    )
    .await
}

pub async fn auto_populate_cop(cop_id: i64, period_id: i64) -> Result<i64, String> {
    invoke(
        "auto_populate_cop",
        &serde_json::json!({ "copId": cop_id, "periodId": period_id }),
    )
    .await
}

pub async fn get_cop_questions(cop_id: i64) -> Result<Vec<CopQuestion>, String> {
    invoke(
        "get_cop_questions",
        &serde_json::json!({ "copId": cop_id }),
    )
    .await
}

pub async fn save_cop_response(
    cop_id: i64,
    question_id: &str,
    response: &str,
) -> Result<(), String> {
    invoke(
        "save_cop_response",
        &serde_json::json!({ "copId": cop_id, "questionId": question_id, "response": response }),
    )
    .await
}

pub async fn sign_ceo_statement(cop_id: i64, ceo_name: &str) -> Result<(), String> {
    invoke(
        "sign_ceo_statement",
        &serde_json::json!({ "copId": cop_id, "ceoName": ceo_name }),
    )
    .await
}

pub async fn compute_compliance_level(cop_id: i64) -> Result<String, String> {
    invoke(
        "compute_compliance_level",
        &serde_json::json!({ "copId": cop_id }),
    )
    .await
}
