{{ config(materialized='table') }}

with lifecycle as (

    select
        f.offer_id,
        f.contract_type,
        f.is_active,
        f.date_created,
        f.date_expired,
        f.offer_lifetime_days,
        dr.rome_code,
        dr.rome_label,
        dl.dept_code,
        dl.dept_name,
        dl.region_name
    from {{ ref('fact_job_offers') }} f
    left join {{ ref('dim_rome') }}     dr on f.rome_key     = dr.rome_key
    left join {{ ref('dim_location') }} dl on f.location_key = dl.location_key

),

-- Only offers with a known lifecycle (expired)
expired as (

    select *
    from lifecycle
    where date_expired is not null
      and offer_lifetime_days is not null
      and offer_lifetime_days >= 0 

),

by_rome_contract as (

    select
        'by_rome_contract' as analysis_type,
        rome_code,
        rome_label,
        contract_type,
        cast(null as string)                                         as region_name,
        count(*)                                                     as total_expired_offers,
        avg(offer_lifetime_days)                                     as avg_lifetime_days,
        min(offer_lifetime_days)                                     as min_lifetime_days,
        max(offer_lifetime_days)                                     as max_lifetime_days,
        APPROX_QUANTILES(offer_lifetime_days, 4)[OFFSET(2)]         as median_lifetime_days
    from expired
    group by rome_code, rome_label, contract_type

),
 
by_rome as (

    select
        'by_rome' as analysis_type,
        rome_code,
        rome_label,
        cast(null as string)                                         as contract_type,
        cast(null as string)                                         as region_name,
        count(*)                                                     as total_expired_offers,
        avg(offer_lifetime_days)                                     as avg_lifetime_days,
        min(offer_lifetime_days)                                     as min_lifetime_days,
        max(offer_lifetime_days)                                     as max_lifetime_days,
        APPROX_QUANTILES(offer_lifetime_days, 4)[OFFSET(2)]         as median_lifetime_days
    from expired
    group by rome_code, rome_label

),

by_region as (

    select
        'by_region' as analysis_type,
        cast(null as string)                                         as rome_code,
        cast(null as string)                                         as rome_label,
        cast(null as string)                                         as contract_type,
        region_name,
        count(*)                                                     as total_expired_offers,
        avg(offer_lifetime_days)                                     as avg_lifetime_days,
        min(offer_lifetime_days)                                     as min_lifetime_days,
        max(offer_lifetime_days)                                     as max_lifetime_days,
        APPROX_QUANTILES(offer_lifetime_days, 4)[OFFSET(2)]         as median_lifetime_days
    from expired
    where region_name is not null
    group by region_name

)

select * from by_rome_contract
union all
select * from by_rome
union all
select * from by_region