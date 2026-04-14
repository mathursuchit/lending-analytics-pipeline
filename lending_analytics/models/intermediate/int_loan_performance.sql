-- Intermediate model: enrich each loan with performance classification
-- and risk bucketing. Sits between staging and marts — no aggregation yet,
-- just row-level business logic that multiple marts can reuse.

with loans as (
    select * from {{ ref('stg_loans') }}
),

enriched as (
    select
        *,

        -- Whether the loan resulted in a loss for the lender
        case
            when loan_status in ('Charged Off', 'Default') then true
            else false
        end as is_defaulted,

        -- Whether loan is still active
        case
            when loan_status = 'Current' then true
            else false
        end as is_active,

        -- Recovery rate: how much was recovered relative to funded amount
        case
            when funded_amount > 0
                then round(total_payment_received / funded_amount, 4)
            else null
        end as recovery_rate,

        -- Loss given default: unrecovered principal as % of funded amount
        case
            when loan_status in ('Charged Off', 'Default') and funded_amount > 0
                then round(1.0 - (total_payment_received / funded_amount), 4)
            else 0
        end as loss_given_default,

        -- FICO risk bucket for segmentation
        case
            when fico_midpoint >= 750 then 'Prime (750+)'
            when fico_midpoint >= 700 then 'Near-Prime (700-749)'
            when fico_midpoint >= 650 then 'Sub-Prime (650-699)'
            else 'Deep Sub-Prime (<650)'
        end as fico_segment,

        -- DTI risk bucket
        case
            when debt_to_income_ratio < 10  then 'Low (<10%)'
            when debt_to_income_ratio < 20  then 'Moderate (10-20%)'
            when debt_to_income_ratio < 30  then 'High (20-30%)'
            else 'Very High (30%+)'
        end as dti_segment,

        -- Income segment
        case
            when annual_income >= 150000 then 'High (150K+)'
            when annual_income >= 75000  then 'Middle (75-150K)'
            when annual_income >= 40000  then 'Lower-Middle (40-75K)'
            else 'Low (<40K)'
        end as income_segment,

        -- Credit derogatory flag
        case
            when delinquencies_2yr > 0 or public_records > 0 then true
            else false
        end as has_derogatory_marks,

        -- Monthly income estimate from annual
        round(annual_income / 12.0, 2) as monthly_income,

        -- Payment-to-income ratio
        case
            when annual_income > 0
                then round((monthly_installment * 12.0) / annual_income * 100, 2)
            else null
        end as payment_to_income_pct

    from loans
)

select * from enriched
