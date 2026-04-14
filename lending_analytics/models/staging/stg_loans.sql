-- Staging model: clean and type-cast raw loan records.
-- One row per loan. No business logic here — just renaming,
-- casting, and light nullability cleanup before downstream models.

with source as (
    select * from {{ source('raw', 'raw_loans') }}
),

cleaned as (
    select
        loan_id,
        cast(loan_amnt    as decimal(12, 2)) as loan_amount,
        cast(funded_amnt  as decimal(12, 2)) as funded_amount,
        trim(term)                           as term,
        cast(int_rate     as decimal(6, 2))  as interest_rate_pct,
        cast(installment  as decimal(10, 2)) as monthly_installment,
        upper(trim(grade))                   as risk_grade,
        upper(trim(sub_grade))               as risk_sub_grade,
        trim(emp_length)                     as employment_length,
        upper(trim(home_ownership))          as home_ownership,
        cast(annual_inc   as decimal(14, 2)) as annual_income,
        cast(issue_date   as date)           as issue_date,
        trim(loan_status)                    as loan_status,
        lower(trim(purpose))                 as loan_purpose,
        upper(trim(addr_state))              as borrower_state,
        cast(dti          as decimal(6, 2))  as debt_to_income_ratio,
        cast(delinq_2yrs  as integer)        as delinquencies_2yr,
        cast(fico_range_low  as integer)     as fico_low,
        cast(fico_range_high as integer)     as fico_high,
        cast(open_acc     as integer)        as open_credit_lines,
        cast(pub_rec      as integer)        as public_records,
        cast(revol_util   as decimal(6, 2))  as revolving_utilization_pct,
        cast(total_acc    as integer)        as total_credit_lines,
        cast(total_pymnt  as decimal(14, 2)) as total_payment_received,

        -- Derived fields at staging layer
        (cast(fico_range_low as integer) + cast(fico_range_high as integer)) / 2.0
            as fico_midpoint,

        case
            when trim(term) = '36 months' then 36
            when trim(term) = '60 months' then 60
        end as term_months,

        date_trunc('month', cast(issue_date as date)) as issue_month,
        date_part('year', cast(issue_date as date))   as issue_year

    from source
    where loan_id is not null
)

select * from cleaned
