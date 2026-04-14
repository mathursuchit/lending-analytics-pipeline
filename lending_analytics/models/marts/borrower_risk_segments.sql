-- Mart: Borrower risk profiling by FICO segment and DTI bucket.
-- Surfaces how credit score and debt load interact with default rates —
-- the core question in lending underwriting decisions.

with loans as (
    select * from {{ ref('int_loan_performance') }}
),

segment_summary as (
    select
        fico_segment,
        dti_segment,
        income_segment,
        count(*)                                            as total_loans,
        round(avg(loan_amount), 2)                          as avg_loan_amount,
        round(avg(interest_rate_pct), 2)                    as avg_interest_rate_pct,
        round(avg(fico_midpoint), 0)                        as avg_fico,
        round(avg(debt_to_income_ratio), 2)                 as avg_dti,

        -- Default behavior per segment
        sum(case when is_defaulted then 1 else 0 end)       as defaults,
        round(
            sum(case when is_defaulted then 1 else 0 end) * 100.0 / count(*),
            2
        )                                                   as default_rate_pct,

        -- Derogatory mark prevalence
        sum(case when has_derogatory_marks then 1 else 0 end)   as loans_with_derogatory,
        round(
            sum(case when has_derogatory_marks then 1 else 0 end) * 100.0 / count(*),
            2
        )                                                   as derogatory_rate_pct,

        round(avg(revolving_utilization_pct), 2)            as avg_revol_util_pct,
        round(avg(payment_to_income_pct), 2)                as avg_payment_to_income_pct,
        round(avg(loss_given_default), 4)                   as avg_loss_given_default

    from loans
    group by fico_segment, dti_segment, income_segment
),

-- Only return segments with enough volume to be meaningful
filtered as (
    select *
    from segment_summary
    where total_loans >= 5
)

select * from filtered
order by default_rate_pct desc
