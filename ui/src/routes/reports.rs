use leptos::prelude::*;
use leptos::task::spawn_local;

use crate::store::AppStore;
use crate::tauri;
use crate::types::{AirEntry, IntensityResult, OdsEntry, PeriodInventory, Reduction, ReportData};

fn fmt(n: Option<f64>) -> String {
    n.map(|v| format!("{v:.3}")).unwrap_or_else(|| "—".into())
}
fn fmt_pct(n: Option<f64>) -> String {
    n.map(|v| format!("{v:.1}%")).unwrap_or_else(|| "—".into())
}

#[component]
pub fn Reports() -> impl IntoView {
    let store = use_context::<AppStore>().expect("AppStore not provided");
    let report = RwSignal::new(None::<ReportData>);
    let inventory = RwSignal::new(None::<PeriodInventory>);
    let intensity_results = RwSignal::new(Vec::<IntensityResult>::new());
    let reductions = RwSignal::new(Vec::<Reduction>::new());
    let ods_entries = RwSignal::new(Vec::<OdsEntry>::new());
    let air_entries = RwSignal::new(Vec::<AirEntry>::new());
    let loading = RwSignal::new(false);
    let error = RwSignal::new(String::new());
    let active_tab = RwSignal::new("305".to_string());

    Effect::new(move |_| {
        let period = store.active_period.get();
        if let Some(period) = period {
            loading.set(true);
            error.set(String::new());
            spawn_local(async move {
                match tauri::generate_gri305_report(period.id).await {
                    Ok(v) => { report.set(Some(v)); }
                    Err(e) => { error.set(e); }
                }
                match tauri::calculate_period(period.id).await {
                    Ok(v) => { inventory.set(Some(v)); }
                    Err(_) => {}
                }
                if let Ok(v) = tauri::list_intensity_results(period.id).await { intensity_results.set(v); }
                if let Ok(v) = tauri::list_reductions(period.id).await { reductions.set(v); }
                if let Ok(v) = tauri::list_ods_emissions(period.id).await { ods_entries.set(v); }
                if let Ok(v) = tauri::list_air_emissions(period.id).await { air_entries.set(v); }
                loading.set(false);
            });
        }
    });

    let export_csv = move |_| {
        let period = store.active_period.get();
        if let Some(period) = period {
            spawn_local(async move {
                let _ = tauri::export_sources_csv(
                    period.id,
                    &format!("c6-scope-data-{}.csv", period.year),
                )
                .await;
            });
        }
    };

    view! {
        <div class="p-8">
            <div class="mb-6 flex items-center justify-between">
                <div>
                    <h1 class="text-xl font-bold text-gray-100">"Reports"</h1>
                    {move || store.active_period.get().map(|p| {
                        let org_name = store.active_org.get().map(|o| o.name).unwrap_or_default();
                        view! {
                            <p class="text-xs text-gray-500">
                                {org_name}" · "{p.year}" · "{p.gwp_ar_version}" GWP"
                            </p>
                        }
                    })}
                </div>
                <button on:click=export_csv
                    class="rounded-lg border border-gray-700 px-4 py-2 text-sm text-gray-400 hover:border-gray-600 hover:text-gray-200">
                    "Export CSV"
                </button>
            </div>

            // Tabs
            <div class="mb-6 flex gap-1 rounded-xl border border-gray-800 bg-gray-900 p-1">
                <button
                    on:click=move |_| active_tab.set("305".into())
                    class=move || if active_tab.get() == "305" {
                        "flex-1 rounded-lg py-2 text-sm font-medium transition-colors bg-green-600 text-white"
                    } else {
                        "flex-1 rounded-lg py-2 text-sm font-medium transition-colors text-gray-500 hover:text-gray-300"
                    }
                >"GRI 305 Disclosures"</button>
                <button
                    on:click=move |_| active_tab.set("inventory".into())
                    class=move || if active_tab.get() == "inventory" {
                        "flex-1 rounded-lg py-2 text-sm font-medium transition-colors bg-green-600 text-white"
                    } else {
                        "flex-1 rounded-lg py-2 text-sm font-medium transition-colors text-gray-500 hover:text-gray-300"
                    }
                >"Inventory Summary"</button>
            </div>

            {move || {
                if loading.get() {
                    return view! { <p class="text-sm text-gray-500">"Generating report…"</p> }.into_any();
                }
                if !error.get().is_empty() {
                    return view! {
                        <div class="rounded-xl border border-red-800 bg-red-950/20 p-4 text-sm text-red-400">{error.get()}</div>
                    }.into_any();
                }
                let Some(inv) = inventory.get() else {
                    return view! {
                        <div class="rounded-xl border border-gray-800 bg-gray-900 p-8 text-center">
                            <p class="text-sm text-gray-500">"No data yet. Add emission sources to generate reports."</p>
                        </div>
                    }.into_any();
                };

                if active_tab.get() == "305" {
                    let ir = intensity_results.get();
                    let reds = reductions.get();
                    let ods = ods_entries.get();
                    let air = air_entries.get();
                    let gwp = store.active_period.get().map(|p| p.gwp_ar_version).unwrap_or_default();
                    let boundary = store.active_org.get().map(|o| o.boundary_method).unwrap_or_default();

                    view! {
                        <div class="space-y-4">
                            // GRI 305-1
                            <div class="rounded-xl border border-gray-800 bg-gray-900 p-5">
                                <div class="mb-4 flex items-center gap-3">
                                    <span class="rounded border border-green-800/60 bg-green-950/30 px-2 py-0.5 font-mono text-xs text-green-400">"GRI 305-1"</span>
                                    <span class="text-sm font-semibold text-gray-200">"Direct (Scope 1) GHG Emissions"</span>
                                </div>
                                <div class="grid gap-4 sm:grid-cols-3">
                                    <div>
                                        <p class="mb-0.5 text-xs font-medium text-gray-500">"Gross Scope 1 (tCO₂e)"</p>
                                        <p class="text-xl font-bold text-gray-100">{fmt(Some(inv.scope1.gross_tco2e))}</p>
                                    </div>
                                    <div>
                                        <p class="mb-0.5 text-xs font-medium text-gray-500">"Biogenic CO₂ (tCO₂)"</p>
                                        <p class="text-xl font-bold text-gray-100">{fmt(Some(inv.scope1.biogenic_co2_tco2e))}</p>
                                    </div>
                                    <div>
                                        <p class="mb-0.5 text-xs font-medium text-gray-500">"Combined uncertainty"</p>
                                        <p class="text-xl font-bold text-gray-100">"±"{fmt_pct(Some(inv.scope1.combined_uncertainty_pct))}</p>
                                    </div>
                                </div>
                                {(!inv.scope1.by_gas.is_empty()).then(|| {
                                    let gases: Vec<_> = inv.scope1.by_gas.iter().map(|(g, v)| (g.clone(), *v)).collect();
                                    view! {
                                        <div class="mt-3">
                                            <p class="mb-2 text-xs font-medium text-gray-500">"By GHG type"</p>
                                            <div class="flex flex-wrap gap-2">
                                                {gases.into_iter().map(|(gas, val)| view! {
                                                    <span class="rounded-full border border-gray-700 bg-gray-800 px-3 py-1 text-xs text-gray-300">
                                                        {gas}": "{fmt(Some(val))}" tCO₂e"
                                                    </span>
                                                }).collect_view()}
                                            </div>
                                        </div>
                                    }
                                })}
                                <p class="mt-3 text-xs text-gray-600">"GWP: "{gwp}" · Boundary: "{boundary}</p>
                            </div>

                            // GRI 305-2
                            <div class="rounded-xl border border-gray-800 bg-gray-900 p-5">
                                <div class="mb-4 flex items-center gap-3">
                                    <span class="rounded border border-green-800/60 bg-green-950/30 px-2 py-0.5 font-mono text-xs text-green-400">"GRI 305-2"</span>
                                    <span class="text-sm font-semibold text-gray-200">"Energy Indirect (Scope 2) GHG Emissions"</span>
                                </div>
                                <div class="grid gap-4 sm:grid-cols-3">
                                    <div>
                                        <p class="mb-0.5 text-xs font-medium text-gray-500">"Location-based (tCO₂e)"</p>
                                        <p class="text-xl font-bold text-gray-100">{fmt(Some(inv.scope2.location_based_tco2e))}</p>
                                    </div>
                                    <div>
                                        <p class="mb-0.5 text-xs font-medium text-gray-500">"Market-based (tCO₂e)"</p>
                                        <p class="text-xl font-bold text-gray-100">{fmt(Some(inv.scope2.market_based_tco2e))}</p>
                                    </div>
                                    <div>
                                        <p class="mb-0.5 text-xs font-medium text-gray-500">"Contractual coverage"</p>
                                        <p class="text-xl font-bold text-gray-100">{fmt_pct(Some(inv.scope2.contractual_coverage_pct))}</p>
                                    </div>
                                </div>
                                {(inv.scope2.location_based_tco2e == 0.0 || inv.scope2.market_based_tco2e == 0.0).then(|| view! {
                                    <p class="mt-3 text-xs text-yellow-500">
                                        "Warning: GRI 305-2 requires BOTH location-based and market-based figures."
                                    </p>
                                })}
                            </div>

                            // GRI 305-3
                            <div class="rounded-xl border border-gray-800 bg-gray-900 p-5">
                                <div class="mb-4 flex items-center gap-3">
                                    <span class="rounded border border-green-800/60 bg-green-950/30 px-2 py-0.5 font-mono text-xs text-green-400">"GRI 305-3"</span>
                                    <span class="text-sm font-semibold text-gray-200">"Other Indirect (Scope 3) GHG Emissions"</span>
                                </div>
                                <div class="grid gap-4 sm:grid-cols-3">
                                    <div>
                                        <p class="mb-0.5 text-xs font-medium text-gray-500">"Gross Scope 3 (tCO₂e)"</p>
                                        <p class="text-xl font-bold text-gray-100">{fmt(Some(inv.scope3.gross_tco2e))}</p>
                                    </div>
                                    <div>
                                        <p class="mb-0.5 text-xs font-medium text-gray-500">"Upstream (tCO₂e)"</p>
                                        <p class="text-xl font-bold text-gray-100">{fmt(Some(inv.scope3.upstream_tco2e))}</p>
                                    </div>
                                    <div>
                                        <p class="mb-0.5 text-xs font-medium text-gray-500">"Downstream (tCO₂e)"</p>
                                        <p class="text-xl font-bold text-gray-100">{fmt(Some(inv.scope3.downstream_tco2e))}</p>
                                    </div>
                                </div>
                                <div class="mt-3 space-y-1">
                                    {inv.scope3.categories.iter()
                                        .filter(|c| c.total_tco2e > 0.0 || c.is_excluded)
                                        .map(|cat| {
                                            let num = cat.category;
                                            let name = cat.category_name.clone();
                                            let excl = cat.is_excluded;
                                            let total = cat.total_tco2e;
                                            view! {
                                                <div class="flex items-center justify-between rounded-lg border border-gray-800 px-3 py-1.5">
                                                    <div class="flex items-center gap-2">
                                                        <span class="w-5 text-right text-xs text-gray-600">{num}</span>
                                                        <span class="text-xs text-gray-300">{name}</span>
                                                        {excl.then(|| view! {
                                                            <span class="rounded-full border border-gray-700 px-2 py-0.5 text-[10px] text-gray-600">"excluded"</span>
                                                        })}
                                                    </div>
                                                    <span class="text-xs font-semibold text-gray-300">{fmt(Some(total))}" tCO₂e"</span>
                                                </div>
                                            }
                                        }).collect_view()}
                                </div>
                            </div>

                            // GRI 305-4
                            <div class="rounded-xl border border-gray-800 bg-gray-900 p-5">
                                <div class="mb-4 flex items-center gap-3">
                                    <span class="rounded border border-green-800/60 bg-green-950/30 px-2 py-0.5 font-mono text-xs text-green-400">"GRI 305-4"</span>
                                    <span class="text-sm font-semibold text-gray-200">"GHG Emissions Intensity"</span>
                                </div>
                                {if !ir.is_empty() {
                                    view! {
                                        <div class="space-y-3">
                                            {ir.into_iter().map(|r| {
                                                let scopes = [
                                                    r.includes_scope1.then_some("1"),
                                                    r.includes_scope2.then_some("2"),
                                                    r.includes_scope3.then_some("3"),
                                                ].into_iter().flatten().collect::<Vec<_>>().join("+");
                                                let unit = r.metric_unit.clone();
                                                view! {
                                                    <div class="rounded-lg border border-gray-800 px-4 py-3">
                                                        <div class="flex items-center justify-between">
                                                            <p class="text-sm font-semibold text-gray-200">{r.metric_name}</p>
                                                            <p class="text-lg font-bold text-gray-100">
                                                                {format!("{:.4}", r.intensity_ratio)}
                                                                <span class="text-xs font-normal text-gray-500">" tCO₂e / "{unit.clone()}</span>
                                                            </p>
                                                        </div>
                                                        <p class="mt-1 text-xs text-gray-500">
                                                            {format!("{:.2}", r.total_emissions_tco2e)}
                                                            " tCO₂e ÷ "
                                                            {r.metric_value}" "{unit}
                                                            " · Scopes: "{scopes}
                                                        </p>
                                                    </div>
                                                }
                                            }).collect_view()}
                                        </div>
                                    }.into_any()
                                } else {
                                    view! { <p class="text-xs text-gray-500">"No intensity metrics defined. Add one in Settings → Supplemental."</p> }.into_any()
                                }}
                            </div>

                            // GRI 305-5
                            <div class="rounded-xl border border-gray-800 bg-gray-900 p-5">
                                <div class="mb-4 flex items-center gap-3">
                                    <span class="rounded border border-green-800/60 bg-green-950/30 px-2 py-0.5 font-mono text-xs text-green-400">"GRI 305-5"</span>
                                    <span class="text-sm font-semibold text-gray-200">"Reduction of GHG Emissions"</span>
                                </div>
                                {if !reds.is_empty() {
                                    view! {
                                        <div class="space-y-2">
                                            {reds.into_iter().map(|r| view! {
                                                <div class="rounded-lg border border-gray-800 px-4 py-3">
                                                    <div class="flex items-center justify-between">
                                                        <p class="text-sm text-gray-300">{r.methodology}</p>
                                                        <p class="text-sm font-semibold text-green-400">
                                                            {format!("{:.2} tCO₂e ({:.1}%)", r.reduction_tco2e, r.reduction_pct)}
                                                        </p>
                                                    </div>
                                                    <p class="mt-1 text-xs text-gray-600">
                                                        "Baseline "{r.baseline_year}": "
                                                        {format!("{:.2}", r.baseline_tco2e)}" tCO₂e → "
                                                        {format!("{:.2}", r.current_tco2e)}" tCO₂e"
                                                    </p>
                                                </div>
                                            }).collect_view()}
                                        </div>
                                    }.into_any()
                                } else {
                                    view! { <p class="text-xs text-gray-500">"No reductions recorded. Add them in Settings → Supplemental."</p> }.into_any()
                                }}
                            </div>

                            // GRI 305-6
                            <div class="rounded-xl border border-gray-800 bg-gray-900 p-5">
                                <div class="mb-4 flex items-center gap-3">
                                    <span class="rounded border border-green-800/60 bg-green-950/30 px-2 py-0.5 font-mono text-xs text-green-400">"GRI 305-6"</span>
                                    <span class="text-sm font-semibold text-gray-200">"Emissions of Ozone-Depleting Substances"</span>
                                </div>
                                {if !ods.is_empty() {
                                    view! {
                                        <div class="overflow-hidden rounded-lg border border-gray-800">
                                            <table class="w-full text-xs">
                                                <thead class="border-b border-gray-800 bg-gray-800/40">
                                                    <tr class="text-left text-gray-500">
                                                        <th class="px-3 py-2">"Substance"</th>
                                                        <th class="px-3 py-2">"Production (t)"</th>
                                                        <th class="px-3 py-2">"Imports (t)"</th>
                                                        <th class="px-3 py-2">"Exports (t)"</th>
                                                        <th class="px-3 py-2">"CFC-11 eq (t)"</th>
                                                    </tr>
                                                </thead>
                                                <tbody class="divide-y divide-gray-800">
                                                    {ods.into_iter().map(|e| view! {
                                                        <tr class="text-gray-300">
                                                            <td class="px-3 py-2 font-medium">{e.substance}</td>
                                                            <td class="px-3 py-2">{e.production_metric_tons}</td>
                                                            <td class="px-3 py-2">{e.imports_metric_tons}</td>
                                                            <td class="px-3 py-2">{e.exports_metric_tons}</td>
                                                            <td class="px-3 py-2 font-semibold">{e.cfc11_equivalent}</td>
                                                        </tr>
                                                    }).collect_view()}
                                                </tbody>
                                            </table>
                                        </div>
                                    }.into_any()
                                } else {
                                    view! { <p class="text-xs text-gray-500">"No ODS entries. Add them in Settings → Supplemental."</p> }.into_any()
                                }}
                            </div>

                            // GRI 305-7
                            <div class="rounded-xl border border-gray-800 bg-gray-900 p-5">
                                <div class="mb-4 flex items-center gap-3">
                                    <span class="rounded border border-green-800/60 bg-green-950/30 px-2 py-0.5 font-mono text-xs text-green-400">"GRI 305-7"</span>
                                    <span class="text-sm font-semibold text-gray-200">"Nitrogen Oxides, Sulfur Oxides and Other Air Emissions"</span>
                                </div>
                                {if !air.is_empty() {
                                    view! {
                                        <div class="overflow-hidden rounded-lg border border-gray-800">
                                            <table class="w-full text-xs">
                                                <thead class="border-b border-gray-800 bg-gray-800/40">
                                                    <tr class="text-left text-gray-500">
                                                        <th class="px-3 py-2">"Type"</th>
                                                        <th class="px-3 py-2">"Substance"</th>
                                                        <th class="px-3 py-2">"Value (metric t)"</th>
                                                        <th class="px-3 py-2">"Method"</th>
                                                    </tr>
                                                </thead>
                                                <tbody class="divide-y divide-gray-800">
                                                    {air.into_iter().map(|e| view! {
                                                        <tr class="text-gray-300">
                                                            <td class="px-3 py-2 font-medium">{e.emission_type}</td>
                                                            <td class="px-3 py-2 text-gray-500">{e.substance.unwrap_or_else(|| "—".into())}</td>
                                                            <td class="px-3 py-2 font-semibold">{e.value_metric_tons}</td>
                                                            <td class="px-3 py-2 text-gray-500">{e.measurement_method.replace('_', " ")}</td>
                                                        </tr>
                                                    }).collect_view()}
                                                </tbody>
                                            </table>
                                        </div>
                                    }.into_any()
                                } else {
                                    view! { <p class="text-xs text-gray-500">"No air emissions recorded. Add them in Settings → Supplemental."</p> }.into_any()
                                }}
                            </div>
                        </div>
                    }.into_any()
                } else {
                    // Inventory Summary
                    let period = store.active_period.get();
                    let org = store.active_org.get();
                    view! {
                        <div class="space-y-4">
                            <div class="rounded-xl border border-green-900/50 bg-green-950/20 p-5">
                                <p class="text-xs font-semibold uppercase tracking-wider text-green-600">"Total GHG Inventory"</p>
                                <p class="mt-1 text-3xl font-black text-green-400">
                                    {fmt(Some(inv.total_tco2e))}
                                    <span class="text-base font-normal text-green-700">" tCO₂e"</span>
                                </p>
                                <p class="mt-1 text-xs text-gray-500">
                                    "Scope 1+2+3 · "
                                    {period.as_ref().map(|p| p.gwp_ar_version.clone()).unwrap_or_else(|| "—".into())}
                                    " GWP · "
                                    {period.as_ref().map(|p| p.year.to_string()).unwrap_or_else(|| "—".into())}
                                </p>
                            </div>
                            <div class="grid gap-4 sm:grid-cols-3">
                                <div class="rounded-xl border border-gray-800 bg-gray-900 p-4">
                                    <p class="text-xs font-medium text-gray-500">"Scope 1"</p>
                                    <p class="text-2xl font-bold text-gray-100">{fmt(Some(inv.scope1.gross_tco2e))}</p>
                                    <p class="text-xs text-gray-500">"tCO₂e · "{inv.scope1.sources.len()}" sources"</p>
                                </div>
                                <div class="rounded-xl border border-gray-800 bg-gray-900 p-4">
                                    <p class="text-xs font-medium text-gray-500">"Scope 2 (location)"</p>
                                    <p class="text-2xl font-bold text-gray-100">{fmt(Some(inv.scope2.location_based_tco2e))}</p>
                                    <p class="text-xs text-gray-500">"tCO₂e · market: "{fmt(Some(inv.scope2.market_based_tco2e))}</p>
                                </div>
                                <div class="rounded-xl border border-gray-800 bg-gray-900 p-4">
                                    <p class="text-xs font-medium text-gray-500">"Scope 3"</p>
                                    <p class="text-2xl font-bold text-gray-100">{fmt(Some(inv.scope3.gross_tco2e))}</p>
                                    <p class="text-xs text-gray-500">"tCO₂e · upstream + downstream"</p>
                                </div>
                            </div>
                            <div class="rounded-xl border border-gray-800 bg-gray-900 p-5">
                                <h2 class="mb-3 text-xs font-semibold uppercase tracking-wider text-gray-500">
                                    "ISO 14064-1 Inventory Elements"
                                </h2>
                                <div class="space-y-2">
                                    {vec![
                                        ("Organizational boundary", org.as_ref().map(|o| o.boundary_method.replace('_', " ")).unwrap_or_else(|| "—".into())),
                                        ("Reporting period", period.as_ref().map(|p| format!("{} to {}", p.start_date, p.end_date)).unwrap_or_else(|| "—".into())),
                                        ("GWP version (IPCC)", period.as_ref().map(|p| p.gwp_ar_version.clone()).unwrap_or_else(|| "—".into())),
                                        ("Base year", org.as_ref().and_then(|o| o.base_year).map(|y| y.to_string()).unwrap_or_else(|| "—".into())),
                                        ("Reporting status", period.as_ref().map(|p| p.status.clone()).unwrap_or_else(|| "—".into())),
                                    ].into_iter().map(|(label, value)| view! {
                                        <div class="flex items-center justify-between border-b border-gray-800 pb-2 last:border-0">
                                            <span class="text-xs text-gray-500">{label}</span>
                                            <span class="text-xs text-gray-300">{value}</span>
                                        </div>
                                    }).collect_view()}
                                </div>
                            </div>
                        </div>
                    }.into_any()
                }
            }}
        </div>
    }
}
