# Skill: Sustainability Standards
GRI 305, ISO 14064-1:2018, and UNGC COP domain knowledge for C12.

## When to use
When implementing emission calculations, report generation, compliance checks, or any feature touching carbon accounting logic.

## GRI 305 — Emissions

### Scope definitions
| Scope | Definition | Examples |
|---|---|---|
| Scope 1 | Direct GHG emissions from owned/controlled sources | Combustion, process emissions, fugitive releases |
| Scope 2 | Indirect emissions from purchased electricity/heat/steam | Grid electricity, district heating |
| Scope 3 | All other indirect emissions in the value chain | Business travel, supply chain, waste, investments |

### Scope 2 methods
- **Location-based**: average grid emission factor for the region
- **Market-based**: supplier-specific factor (RECs, PPAs, guarantees of origin); falls back to location-based if unavailable
- Both must be reported; market-based is the primary figure for target-setting

### Biogenic CO₂
- Tracked separately from fossil GHG emissions per GRI 305-1
- **Not included** in the Scope 1 total — reported as a supplementary disclosure
- C12 stores `biogenic_co2_tco2e` on each emission source

### Key disclosures
- GRI 305-1: Scope 1 (gross, tCO₂e)
- GRI 305-2: Scope 2 (location-based and market-based, tCO₂e)
- GRI 305-3: Scope 3 (by category, tCO₂e)
- GRI 305-4: GHG intensity (tCO₂e per chosen metric)
- GRI 305-5: GHG reductions (against base year)
- GRI 305-6: ODS emissions
- GRI 305-7: NOₓ, SOₓ, and other significant air emissions

## ISO 14064-1:2018 — GHG Inventories

### Calculation formula
```
tCO₂e = (activity_value × emission_factor × GWP_100) / 1000
```
- `activity_value`: quantity of activity (kWh, litres, km, etc.)
- `emission_factor`: kg CO₂e per unit of activity
- `GWP_100`: 100-year Global Warming Potential of the gas (relative to CO₂ = 1)
- Divide by 1000 to convert kg → tonnes

### GWP versions
| Version | Year | Status in C12 |
|---|---|---|
| AR4 | 2007 | Supported (legacy) |
| AR5 | 2013 | Supported |
| AR6 | 2021 | Default (recommended) |

- GWP values are stored in the `gwp_values` table keyed by `(gas, ar_version)`
- The `gwp_ar_version` on a reporting period determines which values are used at calculation time
- CO₂ always has GWP = 1.0 regardless of AR version

### Organisational boundary methods
- **Equity share**: proportional to financial ownership stake
- **Financial control**: entities over which the organisation has financial control
- **Operational control**: entities over which the organisation has operational control

## UNGC COP — Communication on Progress

### Structure
The COP maps to the 10 UNGC Principles across 4 areas:

| Area | Principles |
|---|---|
| Human Rights | 1 (support), 2 (non-complicity) |
| Labour | 3 (freedom of association), 4 (forced labour), 5 (child labour), 6 (discrimination) |
| Environment | 7 (precautionary approach), 8 (environmental responsibility), 9 (clean tech) |
| Anti-Corruption | 10 (anti-corruption) |

### Compliance levels
| Level | Requirement |
|---|---|
| Learner | First-year participant; basic commitment |
| Active | Addresses all 4 areas with actions and outcomes |
| Advanced | All Active criteria + strategy integration + Board-level sign-off |

### CEO Statement
- Required for Active and Advanced levels
- C12 stores `ceo_signed_at` and `ceo_name` on the COP record
- The `sign_ceo_statement` Tauri command sets these fields

### Auto-population
- `auto_populate_cop` pulls GHG data from the period inventory to pre-fill environment-related questions
- Users review and confirm before finalising
