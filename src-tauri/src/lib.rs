mod commands;
mod db;
mod engine;
mod error;

use tauri::Manager;

pub fn run() {
    tauri::Builder::default()
        .plugin(tauri_plugin_updater::Builder::new().build())
        .plugin(tauri_plugin_shell::init())
        .plugin(tauri_plugin_dialog::init())
        .plugin(tauri_plugin_fs::init())
        .setup(|app| {
            let data_dir = app
                .path()
                .app_data_dir()
                .expect("failed to get app data dir");
            std::fs::create_dir_all(&data_dir)?;

            let db_path = data_dir.join("c12.db");
            let database = db::Database::open(&db_path)
                .expect("failed to open database");
            database.migrate()
                .expect("database migration failed");

            app.manage(database);
            Ok(())
        })
        .invoke_handler(tauri::generate_handler![
            // Org & configuration
            commands::org::create_org,
            commands::org::list_orgs,
            commands::org::update_org,
            commands::org::create_entity,
            commands::org::list_entities,
            commands::org::create_period,
            commands::org::list_periods,
            // Emission sources
            commands::sources::create_source,
            commands::sources::update_source,
            commands::sources::delete_source,
            commands::sources::list_sources,
            commands::sources::list_emission_factors,
            // Calculations
            commands::calculate::calculate_period,
            commands::calculate::calculate_intensity,
            commands::calculate::save_intensity_metric,
            commands::calculate::list_intensity_results,
            commands::calculate::delete_intensity_result,
            commands::calculate::list_gwp_values,
            commands::calculate::get_audit_log,
            // UNGC COP
            commands::ungc::init_cop,
            commands::ungc::auto_populate_cop,
            commands::ungc::get_cop_questions,
            commands::ungc::save_cop_response,
            commands::ungc::sign_ceo_statement,
            commands::ungc::compute_compliance_level,
            // Reports
            commands::reports::generate_gri305_report,
            commands::reports::export_sources_csv,
            commands::reports::list_reductions,
            commands::reports::create_reduction,
            commands::reports::delete_reduction,
            commands::reports::list_ods_emissions,
            commands::reports::create_ods_emission,
            commands::reports::delete_ods_emission,
            commands::reports::list_air_emissions,
            commands::reports::create_air_emission,
            commands::reports::delete_air_emission,
        ])
        .run(tauri::generate_context!())
        .expect("error while running c12");
}
