-- Mart: Loan performance aggregated by risk grade.
-- Primary use case: comparing default rates, average interest rates,
-- and recovery rates across the A-F grade spectrum.

with loans as (
    select * from {{ ref('int_loan_performance') }}
),

grade_summary as (
    select
        risk_grade,
        count(*)                                        as total_loans,
        sum(loan_amount)                                as total_loan_volume,
        round(avg(loan_amount), 2)                      as avg_loan_amount,
        round(avg(interest_rate_pct), 2)                as avg_interest_rate_pct,
        round(avg(fico_midpoint), 0)                    as avg_fico_score,
        round(avg(debt_to_income_ratio), 2)             as avg_dti,
        round(avg(annual_income), 2)                    as avg_annual_income,

        -- Default metrics
        sum(case when is_defaulted then 1 else 0 end)  as defaulted_loans,
        round(
            sum(case when is_defaulted then 1 else 0 end) * 100.0 / count(*),
            2
        )                                               as default_rate_pct,

        -- Recovery metrics
        round(avg(case when is_defaulted then loss_given_default end), 4)
                                                        as avg_loss_given_default,
        round(avg(recovery_rate), 4)                    as avg_recovery_rate,

        -- Volume by term
        sum(case when term_months = 36 then 1 else 0 end)  as loans_36mo,
        sum(case when term_months = 60 then 1 else 0 end)  as loans_60mo

    from loans
    group by risk_grade
)

select
    *,
    -- Expected loss proxy: default rate * average loss given default
    round(default_rate_pct / 100.0 * coalesce(avg_loss_given_default, 0), 4)
        as expected_loss_rate
from grade_summary
order by risk_grade
