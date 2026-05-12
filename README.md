# Project 2 — Retail Sales ETL & KPI Analytics

**Target Role**: Data Analyst @ Accenture · Capgemini (Pune, India)
**Business Question**: Which customer segments and regions maximise Lifetime Value (LTV) while minimising churn cost?

---

## SCQA Framework (Consulting Presentation Structure)

| | |
|---|---|
| **Situation** | Client is a mid-size Indian retailer with 3 years of transaction history across 5 cities and 4 product categories |
| **Complication** | Revenue trends vary wildly by region and channel, but no unified KPI definition exists — Finance, Marketing, and Ops each calculate "churn" differently |
| **Question** | Which segments should the client invest in next FY to maximise ROI, and how should KPIs be standardised across departments? |
| **Answer** | Tier-2 cities + Electronics category show the highest LTV-to-churn-cost ratio; recommend standardising on 90-day inactivity as the churn definition company-wide |

---

## Why This Project Fits Accenture / Capgemini

| What They Value | How This Project Shows It |
|---|---|
| ETL pipeline thinking | Explicit Extract → Transform → Load stages with data quality gates |
| SQL depth | Window functions (RANK, LAG, running totals), CTEs, YoY growth queries |
| BI-ready deliverables | CSV exports formatted for direct Power BI import with pre-computed KPIs |
| SCQA storytelling | README and code comments follow the exact consulting narrative framework |

---

## Stack

| Tool | Version | Purpose |
|---|---|---|
| **Python** | ≥ 3.11 | ETL orchestration and data pipeline |
| **DuckDB** | ≥ 0.9 | In-memory SQL (Snowflake-compatible syntax) for all aggregations |
| **Pandas** | ≥ 2.0 | DataFrames and Power BI CSV exports |
| **Matplotlib / Seaborn** | ≥ 3.8 | Dashboard-style charts |
| **SQLite** | stdlib | Lightweight persistent storage (mirrors Snowflake for local dev) |

---

## How to Run

```bash
pip install -r requirements.txt
python sales_etl_kpi.py
# Outputs saved to outputs/
```

## Output Files

| File | Description |
|---|---|
| `outputs/revenue_by_region.png` | Bar chart: monthly revenue by city |
| `outputs/kpi_dashboard.png` | 4-panel KPI dashboard (LTV, Churn, ARPU, Conversion) |
| `outputs/monthly_growth.png` | MoM and YoY revenue growth waterfall |
| `outputs/powerbi_ready.csv` | Pre-computed KPIs formatted for direct Power BI import |
| `outputs/kpi_summary.csv` | Segment-level KPI table |

---

## Interview Talking Points

- *"Walk me through your ETL pipeline."* — Show the three explicit stages in the code with validation between each.
- *"How do you define churn?"* — Explain the 90-day inactivity threshold and why it was chosen over revenue-based definitions.
- *"Can this scale to production?"* — The DuckDB SQL is Snowflake-compatible; swap `duckdb.connect()` for a Snowflake connector and the queries run unchanged.
- *"How would you present this to a client?"* — Reference the SCQA summary printed at the end of the script.
