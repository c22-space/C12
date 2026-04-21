# C6

Desktop carbon accounting software for organisations reporting under GRI 305, ISO 14064-1, and UNGC COP. Works fully offline. No spreadsheets.

---

## What it does

- **GRI 305** — all 7 mandatory disclosures (305-1 through 305-7), generated automatically from entered data
- **ISO 14064-1:2018** — full organizational boundary configuration, uncertainty assessment, immutable audit trail, and verification-readiness export (ISO 14064-3 package)
- **UNGC COP** — 2025 questionnaire (environment + labour + human rights + governance), auto-populated from GRI 305 data, CEO Statement workflow, compliance level tracking (Beginner → LEAD)
- **Scope 1** — direct emissions with predefined category templates and GHG breakdown
- **Scope 2** — mandatory dual calculation: location-based AND market-based (RECs, PPAs, GGs)
- **Scope 3** — all 15 GHG Protocol Corporate Value Chain categories; excluded categories require documented reasons
- **IPCC AR6 GWP** — AR4/AR5/AR6 selectable per reporting period; AR6 recommended and default
- **Uncertainty propagation** — per-source ±% captured, combined uncertainty reported
- **Immutable audit trail** — every change logged with timestamp and reason; no deletes
- **OTA updates** — automatic, signed via Cloudflare R2

---

## Architecture

```
C6-accounting/
├── src-tauri/              # Rust backend (Tauri 2)
│   ├── src/
│   │   ├── engine/         # Calculation engine: scope1, scope2, scope3, gwp, uncertainty
│   │   ├── commands/       # Tauri commands: org, sources, calculate, reports, ungc
│   │   └── db.rs           # SQLite via rusqlite, WAL mode, migrations
│   └── migrations/
│       ├── 001_init.sql    # Full schema (organizations → audit_log)
│       └── 002_seed.sql    # IPCC GWP values (AR4/5/6) + emission factor library
│
├── src/                    # Svelte 5 frontend
│   ├── routes/
│   │   ├── Setup.svelte    # Org onboarding wizard (boundary method, base year, GWP)
│   │   ├── Dashboard.svelte
│   │   ├── Scope1.svelte
│   │   ├── Scope2.svelte   # Dual location/market-based entry
│   │   ├── Scope3.svelte   # 15-category accordion
│   │   ├── Reports.svelte  # GRI 305 disclosures + inventory summary
│   │   ├── Ungc.svelte     # COP questionnaire + CEO Statement
│   │   └── Settings.svelte # Org, periods, entities, audit trail, enterprise
│   └── lib/
│       ├── tauri.ts        # Typed invoke wrappers
│       └── stores/app.ts   # activeOrg, activePeriod, currentRoute
│
└── worker/                 # Cloudflare Worker → api.C6.c22.space
    ├── src/
    │   ├── index.ts        # Route dispatch
    │   ├── auth.ts         # JWT issue/refresh + 14-day trial activation
    │   ├── payments.ts     # DodoPayments webhook (HMAC-SHA256 verified)
    │   ├── updates.ts      # OTA manifest from R2 (GET /updates/check)
    │   └── sync.ts         # Enterprise D1 sync + team management API
    └── migrations/
        └── 0001_enterprise_schema.sql
```

---

## Standards compliance

| Standard | Coverage |
|---|---|
| **GRI 305-1** | Gross Scope 1 tCO₂e · gas breakdown · biogenic CO₂ separate |
| **GRI 305-2** | Location-based AND market-based · contractual coverage % |
| **GRI 305-3** | All 15 Scope 3 categories · upstream/downstream · exclusion reasons |
| **GRI 305-4** | Intensity ratio (configurable activity metric) |
| **GRI 305-5** | Reductions tracking · outsourcing/production-cut exclusion enforced |
| **GRI 305-6** | ODS in CFC-11 equivalent |
| **GRI 305-7** | NOx, SOx, VOC, PM in metric tons |
| **ISO 14064-1:2018** | Boundary method · data quality · uncertainty · audit trail · base year recalculation |
| **ISO 14064-3** | Verification readiness export (zip: data + evidence + methodology) |
| **UNGC COP 2025** | 66-question questionnaire · CEO Statement · compliance levels (Beginner/Active/Advanced/LEAD) |
| **IPCC AR6** | All GWP values · AR4/AR5 selectable for historical comparison |
| **GHG Protocol** | Corporate Standard · Scope 2 Guidance · Corporate Value Chain (Scope 3) |

---

## Feature tiers

| Feature | Community (Free) | Enterprise ($20/seat) |
|---|---|---|
| GRI 305 (all 7 disclosures) | ✓ | ✓ |
| ISO 14064-1 compliance | ✓ | ✓ |
| UNGC COP questionnaire | ✓ | ✓ |
| IPCC AR6 GWP library | ✓ | ✓ |
| Scope 2 dual method | ✓ | ✓ |
| Scope 3 all 15 categories | ✓ | ✓ |
| Uncertainty assessment | ✓ | ✓ |
| Immutable audit trail | ✓ | ✓ |
| PDF + CSV export | ✓ | ✓ |
| OTA updates | ✓ | ✓ |
| Offline, local SQLite | ✓ | ✓ |
| Email support | ✓ | — |
| 14-day free trial | — | ✓ |
| Multi-user (admin/editor/viewer) | — | ✓ |
| Team invite + seat management | — | ✓ |
| Cloud sync (Cloudflare D1) | — | ✓ |
| SSO (Okta, Azure AD, Google Workspace) | — | ✓ |
| Priority support + SLA | — | ✓ |

---

## Stack

| Layer | Choice |
|---|---|
| Desktop | Tauri 2.x (Rust + WebView) |
| Frontend | Svelte 5 + TypeScript + Tailwind CSS |
| Database | SQLite via rusqlite (bundled), WAL mode |
| Reports | Rust (`printpdf`) + CSV |
| OTA updates | `tauri-plugin-updater` + Cloudflare R2 |
| Worker | Cloudflare Workers (TypeScript) |
| Enterprise DB | Cloudflare D1 |
| Auth | Cloudflare Access (SAML/OIDC) + HS256 JWT |
| Payments | DodoPayments |

---

## Documentation

**[→ User Guide](docs/guide.md)** — step-by-step walkthrough: setup, adding emissions, generating reports, glossary.

---

## Getting started

```sh
# Install frontend dependencies
pnpm install

# Run in development mode
pnpm tauri dev

# Build for production
pnpm tauri build

# Worker: deploy to Cloudflare
cd worker && pnpm deploy
```

---

## OTA update flow

```
App (tauri-plugin-updater)
  → GET https://api.C6.c22.space/updates/check?platform=darwin-aarch64&version=x.y.z
  → Worker reads latest.json from R2
  → Returns { version, url (signed R2 URL), signature (ed25519) }
  → App verifies signature, downloads, applies
```

Release: GitHub Actions → build all platforms → sign → upload to R2 → update `latest.json` → all clients update within 24h.

---

## Related

- [GRI 305: Emissions](https://www.globalreporting.org/standards/media/1012/gri-305-emissions-2016.pdf)
- [ISO 14064-1:2018](https://www.iso.org/standard/66453.html)
- [UNGC Communication on Progress](https://unglobalcompact.org/participation/report/cop)
- [GHG Protocol Corporate Standard](https://ghgprotocol.org/corporate-standard)
- [IPCC AR6 WGI](https://www.ipcc.ch/report/sixth-assessment-report-working-group-i/)

---

Built by [c22](https://c22.space) · [Need custom sustainability software? Hire us →](https://c22.space/hire)
