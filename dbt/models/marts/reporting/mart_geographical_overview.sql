{{ config(materialized='table') }}

with offers as (
    select
        f.offer_id,
        f.is_active,
        dl.region_name,
        dl.dept_code,
        dl.dept_name,
        dl.commune_name,
        dc.company_size
    from {{ ref('fact_job_offers') }} f
    left join {{ ref('dim_location') }} dl on f.location_key = dl.location_key
    left join {{ ref('dim_company') }} dc on f.company_key = dc.company_key
),

by_region as (
    select
        'region' as aggregation_level,
        region_name,
        cast(null as string) as dept_code,
        cast(null as string) as dept_name,
        cast(null as string) as commune_name,
        count(*) as total_offers,
        countif(is_active) as active_offers
    from offers
    where region_name is not null
    group by region_name
),

by_department as (
    select
        'department' as aggregation_level,
        region_name,
        dept_code,
        dept_name,
        cast(null as string) as commune_name,
        count(*) as total_offers,
        countif(is_active) as active_offers
    from offers
    where dept_code is not null
    group by region_name, dept_code, dept_name
)

by_commune as (
    select
        'commune' as aggregation_level,
        region_name,
        dept_code,
        dept_name,
        commune_name,
        count(*) as total_offers,
        countif(is_active) as active_offers
    from offers
    where commune_name is not null
    group by region_name, dept_code, dept_name, commune_name
    having count(*) > 10
)

select * from by_region
union all
select * from by_department
union all
select * from by_commune
