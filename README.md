# Lending Analytics Pipeline

A production-style credit analytics pipeline built with **dbt + DuckDB**, modeling 5,000 loan records through a layered data architecture — from raw ingestion to analytics-ready marts.

**Live demo:** [mathursuchit-lending-analytics.streamlit.app](https://mathursuchit-lending-analytics.streamlit.app)

---

## What it does

Takes raw loan application and performance data and transforms it through three model layers into analytics-ready tables used for credit risk reporting:

- **Default rate by risk grade** — how A through F grades compare on delinquency and expected loss
- **Borrower risk segmentation** — which FICO/DTI/income combinations carry the highest default risk
- **Vintage analysis** — whether credit quality is improving or deteriorating across origination cohorts

These are standard analyses in lending underwriting and portfolio monitoring.

---

## Architecture

```
seeds/raw_loans.csv
    |
    v
[staging]          stg_loans          — clean, type-cast, rename
    |
    v
[intermediate]     int_loan_performance  — row-level enrichment: default flags,
    |                                     recovery rates, FICO/DTI segments
    v
[marts]            loan_performance_by_grade   — aggregated by A-F grade
                   borrower_risk_segments      — cross-tab by FICO x DTI x income
                   vintage_analysis            — cohort performance by origination month
    |
    v
[dashboard]        Streamlit app reading directly from DuckDB
```

**Why DuckDB?**
Zero infrastructure — runs entirely in-process from a single `.duckdb` file. Identical SQL semantics to Snowflake/Redshift. Perfect for a portable portfolio project that anyone can clone and run.

---

## Model layers

| Layer | Materialization | Purpose |
|---|---|---|
| Staging | View | Clean raw data — casting, renaming, nullability |
| Intermediate | View | Row-level business logic reused across marts |
| Marts | Table | Aggregated, analytics-ready outputs |

---

## Tests

21 dbt tests across all layers:

- **Schema tests** — `unique`, `not_null`, `accepted_values` on keys and categorical columns
- **Custom test** — `assert_default_rate_within_bounds`: portfolio default rate must stay between 1% and 50% (catches data quality drift)
- **Generic macro** — `positive_values`: reusable test for numeric sanity checks

Run tests:
```bash
dbt test
```

---

## Run locally

**Requirements:** Python 3.11+

```bash
git clone https://github.com/mathursuchit/lending-analytics-pipeline
cd lending-analytics-pipeline
pip install -r requirements.txt
```

Generate seed data and run the pipeline:
```bash
cd lending_analytics
python seeds/generate_seeds.py
dbt seed
dbt run
dbt test
```

Launch the dashboard:
```bash
cd ../dashboard
streamlit run app.py
```

---

## Stack

- **dbt Core 1.8** — data transformation, testing, documentation
- **DuckDB 1.x** — embedded analytical database (Snowflake-compatible SQL)
- **Streamlit** — dashboard frontend
- **Plotly** — interactive charts
- **Python 3.11**

---

## Author

Suchit Mathur — [LinkedIn](https://www.linkedin.com/in/mathursuchit/) · [GitHub](https://github.com/mathursuchit)
