# C12 — Agent Instructions

## Project
C12 is a carbon accounting desktop app targeting GRI 305, ISO 14064-1:2018, and UNGC COP compliance.
Users record emission sources, calculate Scope 1/2/3 inventories, and generate compliance reports.

## Stack
| Directory    | Purpose |
|---|---|
| `ui/`        | Leptos 0.7 (Rust/WASM, CSR), Trunk, Tailwind CDN |
| `src-tauri/` | Tauri v2 backend, rusqlite (bundled SQLite) |
| `worker/`    | Cloudflare Worker (Rust, workers-rs 0.4), D1, R2 |

Build: `cargo tauri dev` (dev) · `cargo tauri build` (release)  
Frontend served by Trunk at `:5173` during dev.

## Skills
Read the relevant skill file before working in that area:

| Task area | Skill |
|---|---|
| Leptos components, signals, reactivity | `.agents/skills/leptos-ui.md` |
| SQLite migrations, connections | `.agents/skills/db-migrations.md` |
| Rust error handling, Tauri commands, type rules | `.agents/skills/rust-patterns.md` |
| GRI 305, ISO 14064, UNGC COP domain knowledge | `.agents/skills/sustainability-standards.md` |

## Extreme Programming Principles
- **Simple design** — the simplest code that works. No speculative abstractions.
- **YAGNI** — don't build what isn't needed right now.
- **Refactor continuously** — every change leaves the code cleaner than it found it.
- **Small, complete steps** — each commit compiles and does exactly one thing.
- **Test first when possible** — write expected behaviour before implementation.
- **Collective ownership** — any part of the codebase can be changed; no territory.
- **Continuous integration** — integrate to main frequently; long-lived branches are a smell.

## Non-Negotiable Constraints

### Correctness
- Propagate every error to the caller — never swallow with `let _ =`
- All SQL queries must be parameterised — no string interpolation
- Numeric types are deliberate: `f64` for emissions, `i64` for IDs/timestamps, `i32` for years
- Tauri commands return `Result<T, AppError>`, never `Result<T, String>`; `AppError` must implement `Serialize`

### Type safety
- No `serde_json::Value` in command return types — define explicit structs
- All Leptos signals carry the most specific type available
- Avoid `.unwrap()` in production paths; use `?` or pattern-match with meaningful fallbacks
- Tauri args structs carry `#[serde(rename_all = "camelCase")]`

### Minimal surface area
- No helper functions, abstractions, or utilities unless called from ≥2 sites
- No `mod utils` or `mod helpers`
- No feature flags or backwards-compat shims — change the code directly
- Delete dead code; never comment it out

### Cloudflare Worker
- All route handlers take `(Request, &Env)` — no global state
- JWT via HMAC-SHA256 (`hmac` + `sha2` crates) — no Web Crypto
- D1 column names validated against a compile-time whitelist — no arbitrary user-controlled SQL
- D1 binds use `JsValue::from_str` / `JsValue::from_f64` / `JsValue::null()` — no raw JS eval

## What Not to Do
- No comments explaining what code does — name things well instead
- No `README.md`, planning docs, or analysis files unless explicitly asked
- No error handling for paths that cannot happen
- No `cargo fmt` rewrites as part of a bug fix — keep diffs minimal
- No npm, pnpm, or any Node tooling — the entire project is Rust
