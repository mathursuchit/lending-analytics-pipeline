-- Mart: Vintage analysis — loan cohort performance by origination month.
-- Vintages are a standard tool in lending analytics to identify whether
-- credit quality is improving or deteriorating over time.
-- Each row = one origination month cohort.

with loans as (
    select * from {{ ref('int_loan_performance') }}
),

vintage as (
    select
        issue_year,
        issue_month,
        risk_grade,

        count(*)                                            as loans_originated,
        sum(loan_amount)                                    as total_originated_volume,
        round(avg(loan_amount), 2)                          as avg_loan_amount,
        round(avg(interest_rate_pct), 2)                    as avg_interest_rate_pct,
        round(avg(fico_midpoint), 0)                        as avg_fico,
        round(avg(debt_to_income_ratio), 2)                 as avg_dti,

        -- Default performance for this cohort
        sum(case when is_defaulted then 1 else 0 end)       as defaults,
        round(
            sum(case when is_defaulted then 1 else 0 end) * 100.0 / count(*),
            2
        )                                                   as default_rate_pct,

        -- Credit quality signals at origination
        round(avg(revolving_utilization_pct), 2)            as avg_revol_util_pct,
        sum(case when has_derogatory_marks then 1 else 0 end)   as loans_with_derogatory,

        -- Recovery
        round(sum(total_payment_received), 2)               as total_payments_received,
        round(
            sum(total_payment_received) / nullif(sum(funded_amount), 0),
            4
        )                                                   as portfolio_recovery_rate

    from loans
    group by issue_year, issue_month, risk_grade
)

select * from vintage
order by issue_month, risk_grade
