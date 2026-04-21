# C6 User Guide

C6 is a desktop app for measuring and reporting your organisation's greenhouse gas emissions. This guide walks you through the full workflow from first launch to a finished GRI 305 report.

---

## Table of contents

1. [First launch](#1-first-launch)
2. [Understanding scopes](#2-understanding-scopes)
3. [Adding Scope 1 emissions](#3-adding-scope-1-emissions)
4. [Adding Scope 2 emissions](#4-adding-scope-2-emissions)
5. [Adding Scope 3 emissions](#5-adding-scope-3-emissions)
6. [Supplemental disclosures](#6-supplemental-disclosures)
7. [Generating your report](#7-generating-your-report)
8. [UNGC COP questionnaire](#8-ungc-cop-questionnaire)
9. [Managing multiple periods](#9-managing-multiple-periods)
10. [Glossary](#10-glossary)

---

## 1. First launch

When you open C6 for the first time you will see a two-step setup wizard.

### Step 1 — Organisation

| Field | What to enter |
|---|---|
| **Organisation name** | Your legal entity name, e.g. "Acme Ltd" |
| **Boundary method** | How you decide which operations to include. Choose **Operational Control** if you are unsure — it is the most common method and covers every facility where you control day-to-day operations. |
| **Base year** | The first year you will use as a baseline for tracking reductions over time. Typically the year before your first reporting year. |
| **Currency** | Used on cost fields only. Emissions are always in tCO₂e regardless of currency. |

**Which boundary method should I pick?**

| Method | Include these operations |
|---|---|
| Operational Control | Any facility where you set operating procedures |
| Financial Control | Any entity whose financials you consolidate |
| Equity Share | Pro-rata — 50% ownership → 50% of emissions counted |

### Step 2 — Reporting period

| Field | What to enter |
|---|---|
| **Reporting year** | The calendar year you are reporting on, e.g. 2024 |
| **GWP values** | Leave as **AR6** unless your auditor specifies otherwise |

Once you click **Start accounting** you arrive at the Dashboard.

---

## 2. Understanding scopes

The GHG Protocol divides emissions into three scopes.

```
Scope 1  →  Your own combustion and fugitive releases
Scope 2  →  Electricity and energy you buy
Scope 3  →  Everything in your value chain
```

**Scope 1 — direct emissions**  
Gas boilers, diesel generators, company-owned vehicles, refrigerant leaks, and any other source you own or control.

**Scope 2 — energy indirect**  
Emissions produced by power stations when generating the electricity you consume. Reported two ways:
- *Location-based* — uses the average emission factor for your national grid
- *Market-based* — uses the factor from your specific energy contract or certificate

**Scope 3 — other indirect**  
The widest category. Includes your supply chain (purchased goods), business travel, employee commuting, waste, downstream product use, and investments. Organised into 15 categories defined by the GHG Protocol.

You do not have to complete all three scopes before generating a report. Scope 1 and Scope 2 are mandatory for GRI 305. Scope 3 categories can be excluded with a documented reason.

---

## 3. Adding Scope 1 emissions

Click **Scope 1** in the left sidebar, then **+ Add source**.

### What you need before you start

For each emission source you need three numbers from your records (invoices, meter readings, maintenance logs):

1. **How much** you consumed — the *activity value*
2. **In what unit** — litres, cubic metres, kilograms, kWh
3. **The emission factor** for that fuel or activity — kilograms of CO₂e released per unit consumed

### Field-by-field

| Field | Description | Example |
|---|---|---|
| **Category** | Pick the closest match from the dropdown | "Stationary combustion — natural gas" |
| **GHG type** | The gas being emitted. Use **CO2** for most combustion; use **CH4_fossil** for methane leaks from gas pipelines; use **HFC** or **PFC** for refrigerants | CO2 |
| **Activity value** | The quantity consumed during the reporting period | 45000 |
| **Activity unit** | Unit matching your invoice or meter | m³ |
| **Emission factor** | kg of CO₂e emitted per unit of activity. Look this up in the DEFRA or EPA tables (links below) | 2.042 |
| **EF unit** | Unit for the factor, matching activity unit | kgCO2e/m³ |
| **EF source** | Where you found the factor | DEFRA 2024 |
| **GWP value** | Global Warming Potential of the gas relative to CO₂. For CO2 this is always **1**. For CH4 (AR6) it is **29.8**. C6 uses AR6 by default — check the IPCC AR6 table if you change the gas type | 1 |
| **Uncertainty %** | Your confidence in the data. Use **5%** for invoice-based data, **10–15%** for estimates | 5 |

### Common emission factors

These are DEFRA 2024 UK values. For other countries use your national agency's published figures (see [Emission factor sources](#emission-factor-sources) below).

| Fuel | Unit | Factor (kgCO₂e/unit) |
|---|---|---|
| Natural gas | m³ | 2.042 |
| Natural gas | kWh (gross CV) | 0.18254 |
| Diesel | litre | 2.51 |
| Petrol | litre | 2.17 |
| LPG | litre | 1.56 |
| Coal (industrial) | kg | 2.54 |

### Refrigerants (fugitive emissions)

For refrigerant leaks, the GHG type is **HFC**, **PFC**, or **SF6** depending on the refrigerant. The emission factor is the GWP of the refrigerant itself (e.g. R-410A = 2,088 kgCO₂e/kg leakage). Set **activity value** to the kg of refrigerant lost during the year (from maintenance records).

---

## 4. Adding Scope 2 emissions

Click **Scope 2** in the sidebar. You will see two sections: **Location-based** and **Market-based**. You must complete both to be GRI 305-2 compliant.

### Location-based

Use the average grid emission factor for the country or region where electricity is consumed.

| Field | Example |
|---|---|
| Category | "Purchased electricity — grid" |
| Activity value | 180000 (kWh consumed for the year) |
| Activity unit | kWh |
| Emission factor | 0.23314 (UK 2024 grid average, kgCO₂e/kWh) |
| EF source | DEFRA 2024 |

### Market-based

Use the factor from your energy contract. If you have no contract (you buy from the standard supplier tariff), use the **residual mix** factor for your country, which is typically higher than the grid average.

| Instrument type | When to use |
|---|---|
| No instrument (residual mix) | Standard tariff, no green certificate |
| REC | US Renewable Energy Certificate |
| PPA | Power Purchase Agreement with a wind/solar farm |
| GG / Guarantee of Origin | European green energy certificate |

If you have a PPA or green certificate, your market-based factor may be 0. Enter 0 and select the correct instrument type — C6 records the contractual coverage percentage automatically.

### Common Scope 2 factors

| Country | Grid average (kgCO₂e/kWh) | Source |
|---|---|---|
| UK | 0.23314 | DEFRA 2024 |
| USA | 0.3866 | EPA eGRID 2023 |
| EU average | 0.276 | AIB 2023 |
| Australia | 0.510 | DCCEEW 2024 |

---

## 5. Adding Scope 3 emissions

Click **Scope 3** in the sidebar. The 15 GHG Protocol categories are shown as an accordion. Expand the categories relevant to your organisation.

### Which categories must I include?

You must include every category that is **material** (significant relative to total emissions or flagged as relevant by your stakeholders). Any excluded category must have a documented reason.

**Most organisations start with:**

| Category | Description | Typical data source |
|---|---|---|
| 1 — Purchased goods & services | Emissions from producing what you buy | Spend-based estimate or supplier data |
| 3 — Fuel & energy (upstream) | Extraction and distribution of fuel/energy before it reaches you | Calculated automatically from Scope 1+2 |
| 6 — Business travel | Flights, rail, taxis on company business | Travel agency report or expense system |
| 7 — Employee commuting | Staff travel to/from work | Staff survey (distance × mode × days) |
| 11 — Use of sold products | Emissions from customers using what you sell | Relevant for energy-using products |

### Excluding a category

If a category does not apply (e.g. Category 10 — Processing of sold products, for a software company), click **Exclude** and enter the reason. Common reasons:

- "Not applicable — no physical products sold"
- "Immaterial — estimated below 1% of total Scope 3"
- "Data not available — will include in next reporting period"

---

## 6. Supplemental disclosures

Go to **Settings → Supplemental** to enter data for GRI 305-4 through 305-7.

### 305-4 Intensity ratios

An intensity ratio expresses your emissions relative to a business metric (revenue, headcount, floor area, production volume). Example: 12.4 tCO₂e per £1 million revenue.

| Field | Example |
|---|---|
| Metric name | Revenue |
| Metric value | 8500000 |
| Metric unit | GBP |
| Include Scope 1 | Yes |
| Include Scope 2 | Yes |
| Include Scope 3 | Optional |

C6 calculates the ratio automatically and shows it in the report.

### 305-5 Emission reductions

Record deliberate reductions compared to a baseline — for example after switching to electric vehicles or improving insulation.

| Field | Example |
|---|---|
| Baseline year | 2022 |
| Baseline tCO₂e | 1250 |
| Current tCO₂e | 980 |
| Methodology | "Operational efficiency — boiler upgrade programme" |

C6 calculates the reduction (270 tCO₂e, 21.6%) automatically.

### 305-6 ODS emissions

Ozone-depleting substances (refrigerants such as R-22, R-404A) expressed in CFC-11 equivalent tonnes. Enter production, import, and export quantities from your maintenance records. The conversion is pre-loaded in C6.

### 305-7 Air quality emissions

Other significant air pollutants: NOx, SOx, VOC, PM, HAPs. Values in metric tonnes from your stack monitoring data or engineering estimates.

---

## 7. Generating your report

Click **Reports** in the sidebar. The report page shows all GRI 305 disclosures populated from your entered data:

| Disclosure | Content |
|---|---|
| 305-1 | Scope 1 gross tCO₂e, by gas, biogenic CO₂ |
| 305-2 | Scope 2 location-based and market-based tCO₂e |
| 305-3 | Scope 3 by category, exclusion reasons |
| 305-4 | Intensity ratios (from Settings → Supplemental) |
| 305-5 | Emission reductions |
| 305-6 | ODS in CFC-11 equivalent |
| 305-7 | NOx, SOx, VOC, PM in metric tonnes |

Use **Export PDF** for your sustainability report and **Export CSV** for auditor review or your ESG platform.

The ISO 14064-3 verification package (zip) includes your data, methodology notes, and the full audit trail — hand this to your external verifier.

---

## 8. UNGC COP questionnaire

If your organisation is a UN Global Compact signatory, click **UNGC COP** in the sidebar.

The questionnaire covers four principles areas: Environment, Labour, Human Rights, and Anti-Corruption. The Environment section (questions 1–7) is auto-populated from your GRI 305 data.

Fill in the remaining questions and generate the PDF to submit through the UNGC portal.

---

## 9. Managing multiple periods

Go to **Settings → Periods** to add a new reporting year. Each period is independent — you can have 2022, 2023, and 2024 all stored locally. Use the period selector at the top of the sidebar to switch between them.

**Base year recalculation:** If you change your methodology or boundary significantly, ISO 14064-1 requires you to recalculate your base year. Add the recalculated figures as a new source in the base year period with a note explaining the reason.

---

## 10. Glossary

| Term | Meaning |
|---|---|
| **tCO₂e** | Tonnes of CO₂ equivalent — the standard unit for comparing different greenhouse gases |
| **GWP** | Global Warming Potential — how much warming a gas causes relative to CO₂ over 100 years |
| **AR6** | IPCC Sixth Assessment Report (2021) — the current GWP reference |
| **Emission factor (EF)** | Published coefficient converting an activity (litres of diesel burned) into emissions (kgCO₂e) |
| **Location-based** | Scope 2 method using the average grid emission factor for your country |
| **Market-based** | Scope 2 method using the factor from your specific energy contract |
| **Residual mix** | The market-based factor used when you have no green energy contract |
| **Boundary method** | Rule for deciding which legal entities and facilities are included in your inventory |
| **Operational control** | Boundary method: include 100% of facilities where you set operating procedures |
| **Financial control** | Boundary method: include 100% of entities you financially consolidate |
| **Equity share** | Boundary method: include your ownership percentage of each entity's emissions |
| **Biogenic CO₂** | CO₂ from burning biomass — reported separately and not included in the Scope 1 total |
| **ODS** | Ozone-depleting substance (e.g. R-22 refrigerant) |
| **CFC-11 eq** | The standard unit for comparing ozone-depleting potential of different substances |
| **Intensity ratio** | Emissions divided by a business metric (revenue, headcount, output) |
| **Verification** | Third-party review of your inventory against ISO 14064-3 |

---

## Emission factor sources

| Source | Coverage | URL |
|---|---|---|
| DEFRA Conversion Factors | UK (updated annually) | https://www.gov.uk/government/collections/government-conversion-factors-for-company-reporting |
| EPA Emission Factors | USA | https://www.epa.gov/climateleadership/ghg-emission-factors-hub |
| IEA Electricity Factors | International grid averages | https://www.iea.org/data-and-statistics |
| IPCC AR6 GWP table | All gases | https://www.ipcc.ch/report/ar6/wg1/ Chapter 7, Table 7.SM.7 |
| GHG Protocol tools | Scope 3 spend-based | https://ghgprotocol.org/calculation-tools |

---

*Questions or issues? Open an issue on [GitHub](https://github.com/c22-space/C6-accounting/issues).*
