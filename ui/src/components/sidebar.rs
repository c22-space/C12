use leptos::prelude::*;
use leptos::task::spawn_local;

use crate::store::AppStore;
use crate::tauri;

const DOCS_URL: &str =
    "https://github.com/c22-space/C6/blob/main/docs/guide.md";

struct NavItem {
    path: &'static str,
    label: &'static str,
    icon: &'static str,
}

const NAV: &[NavItem] = &[
    NavItem { path: "/dashboard",      label: "Dashboard", icon: "◈" },
    NavItem { path: "/sources/scope1", label: "Scope 1",   icon: "①" },
    NavItem { path: "/sources/scope2", label: "Scope 2",   icon: "②" },
    NavItem { path: "/sources/scope3", label: "Scope 3",   icon: "③" },
    NavItem { path: "/reports",        label: "Reports",   icon: "↗" },
    NavItem { path: "/ungc",           label: "UNGC COP",  icon: "✦" },
    NavItem { path: "/settings",       label: "Settings",  icon: "⚙" },
];

#[component]
pub fn Sidebar() -> impl IntoView {
    let store = use_context::<AppStore>().expect("AppStore not provided");
    let route = store.current_route;

    view! {
        <aside class="flex w-56 flex-col border-r border-gray-800 bg-gray-950 no-select">
            // Logo
            <div class="flex h-14 items-center gap-2 border-b border-gray-800 px-4">
                <span class="text-lg font-bold text-green-500">"C6"</span>
                <span class="text-xs text-gray-600">"Carbon Accounting"</span>
            </div>

            // Org / period selector
            <div class="border-b border-gray-800 px-3 py-3">
                {move || match store.active_org.get() {
                    Some(org) => view! {
                        <p class="truncate text-xs font-semibold text-gray-300">{org.name}</p>
                        {move || store.active_period.get().map(|p| view! {
                            <p class="text-xs text-gray-500">{p.year}" · "{p.gwp_ar_version}</p>
                        })}
                    }.into_any(),
                    None => view! {
                        <p class="text-xs text-gray-600">"No organisation"</p>
                    }.into_any(),
                }}
            </div>

            // Navigation
            <nav class="flex-1 space-y-0.5 overflow-y-auto p-2">
                {NAV.iter().map(|item| {
                    let path = item.path;
                    let label = item.label;
                    let icon = item.icon;
                    view! {
                        <button
                            class=move || {
                                let active = route.get() == path;
                                if active { "sidebar-item active w-full text-left" }
                                else { "sidebar-item w-full text-left" }
                            }
                            on:click=move |_| store.navigate(path)
                        >
                            <span class="w-4 text-center font-mono text-xs">{icon}</span>
                            {label}
                        </button>
                    }
                }).collect_view()}
            </nav>

            // Docs link
            <div class="border-t border-gray-800 px-2 py-2">
                <button
                    class="sidebar-item w-full text-left text-gray-500 hover:text-gray-300"
                    on:click=move |_| spawn_local(async { tauri::shell_open(DOCS_URL).await })
                >
                    <span class="w-4 text-center font-mono text-xs">"?"</span>
                    "User guide"
                </button>
            </div>

            // Footer branding
            <div class="border-t border-gray-800 px-4 py-3">
                <p class="text-[10px] text-gray-700">
                    "Built by "
                    <a href="https://c22.space" target="_blank" rel="noopener"
                        class="text-gray-600 hover:text-gray-400 transition-colors">
                        "c22"
                    </a>
                    " · "
                    <a href="https://c22.space/hire" target="_blank" rel="noopener"
                        class="text-gray-600 hover:text-gray-400 transition-colors">
                        "Hire us →"
                    </a>
                </p>
            </div>
        </aside>
    }
}
