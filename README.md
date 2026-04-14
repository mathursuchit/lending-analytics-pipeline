# Lending Analytics Pipeline

A credit analytics pipeline built with dbt and DuckDB, modeling 90,000+ real Lending Club loans through a layered transformation architecture from raw ingestion to analytics-ready marts.

**Live demo:** [mathursuchit-lending-analytics.streamlit.app](https://mathursuchit-lending-analytics.streamlit.app)

## Why I built this

Most data analytics portfolios show a notebook with some pandas and a chart. I wanted something that reflects how analytics engineering actually works at a bank or fintech: layered SQL models, testable transformations, and a clear separation between raw data, business logic, and reporting outputs.

The stack here (dbt + DuckDB) maps directly to production setups using dbt + Snowflake or dbt + Redshift. Swapping the adapter is the only change needed.

## What it does

Takes raw loan application and performance data and transforms it into three analytics outputs used in credit risk reporting:

- Default rate by risk grade (A through F), with expected loss and interest rate overlays
- Borrower risk segmentation by FICO score, DTI ratio, and income band
- Vintage analysis showing how default rates evolve across origination cohorts

These are standard analyses in lending underwriting and portfolio monitoring.

## Architecture

```
seeds/raw_loans.csv
    |
    v
[staging]        stg_loans               clean, type-cast, rename
    |
    v
[intermediate]   int_loan_performance    row-level enrichment: default flags,
    |                                    recovery rates, FICO/DTI segments
    v
[marts]          loan_performance_by_grade    aggregated by A-F grade
                 borrower_risk_segments       cross-tab by FICO x DTI x income
                 vintage_analysis             cohort performance by origination month
    |
    v
[dashboard]      Streamlit app reading directly from DuckDB
```

Staging and intermediate layers are views. Marts are materialized as tables. The Streamlit app reads the pre-built `.duckdb` file directly so there is no dbt runtime needed in production.

## Tests

21 dbt tests across all layers:

- Schema tests: `unique`, `not_null`, `accepted_values` on keys and categorical columns
- Custom test: `assert_default_rate_within_bounds` checks that the portfolio default rate stays between 1% and 50%, catching data quality drift
- Generic macro: `positive_values` for numeric sanity checks on loan amounts and rates

```bash
dbt test
```

## Dataset

Real Lending Club loan data from Kaggle, 90,586 loans issued between 2007 and 2018. Covers the full credit cycle from origination through charge-off, with grades A through F, 36 and 60 month terms, and borrower attributes including FICO score, DTI, annual income, and derogatory marks.


## Run locally

Requires Python 3.11 (dbt-duckdb has a dependency that breaks on 3.12+).

```bash
git clone https://github.com/mathursuchit/lending-analytics-pipeline
cd lending-analytics-pipeline
pip install dbt-duckdb duckdb streamlit plotly pandas
```

Run the pipeline:

```bash
cd lending_analytics
dbt seed
dbt run
dbt test
```

Launch the dashboard:

```bash
cd ..
streamlit run app.py
```

## Stack

Python 3.11 · dbt Core 1.11 · DuckDB · Streamlit · Plotly

## Author

Suchit Mathur — [LinkedIn](https://www.linkedin.com/in/mathursuchit/) · [GitHub](https://github.com/mathursuchit)
