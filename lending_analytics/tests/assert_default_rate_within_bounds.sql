-- Custom test: overall portfolio default rate must stay between 1% and 50%.
-- Flags data quality issues — e.g. all records marked as defaulted,
-- or seed generation producing unrealistic data distributions.

with portfolio_stats as (
    select
        count(*)                                            as total_loans,
        sum(case when is_defaulted then 1 else 0 end)       as defaulted_loans,
        sum(case when is_defaulted then 1 else 0 end) * 100.0 / count(*)
                                                            as default_rate_pct
    from {{ ref('int_loan_performance') }}
)

select *
from portfolio_stats
where default_rate_pct < 1.0
   or default_rate_pct > 50.0
