{{ config(materialized='table') }}

with offers as (
    select
        f.offer_id,
        f.contract_type,
        f.contract_type_label,
        f.is_alternance,
        f.is_active,
        dr.rome_code,
        dr.rome_label
    from {{ ref('fact_job_offers') }} f
    left join {{ ref('dim_rome') }} dr on f.rome_key = dr.rome_key
),

overall_distribution as (
    select
        'overall' as aggregation_level,
        cast(null as string) as rome_code,
        cast(null as string) as rome_label,
        contract_type,
        cast(null as bool) as is_alternance,
        count(*) as total_offers,
        countif(is_active) as active_offers
    from offers
    group by contract_type
),

rome_distribution as (
    select
        'rome' as aggregation_level,
        rome_code,
        rome_label,
        contract_type,
        cast(null as bool) as is_alternance,
        count(*) as total_offers,
        countif(is_active) as active_offers
    from offers
    where rome_code is not null
    group by rome_code, rome_label, contract_type
),

rome_alternance_distribution as (
    select
        'rome_alternance' as aggregation_level,
        rome_code,
        rome_label,
        cast(null as string) as contract_type,
        is_alternance,
        count(*) as total_offers,
        countif(is_active) as active_offers
    from offers
    where rome_code is not null
    group by rome_code, rome_label, is_alternance
)

select * from overall_distribution
union all
select * from rome_distribution
union all
select * from rome_alternance_distribution

