{{ config(materialized='table') }}

with skills as (

    select distinct
        offer_id,
        skill_label,
        exigence_level,
        extraction_source,
    from {{ ref('stg_job_skills_clean') }}

),

lifecycle as (

    select
        offer_id,
        date_created,
        date_expired,
        rome_code,
        dept_code,
        commune_code,
        commune_name,
        code_postal,
        company_name,
        naf_code,
        sector_code
    from {{ ref('int_offer_lifecycle') }}

),

dim_dates as (

    select *
    from {{ ref('dim_date') }}

),

joined as (

    select
        s.offer_id,
        s.skill_label,
        s.exigence_level,
        s.extraction_source,

        dl.location_key,
        dc.company_key,
        dr.rome_key,

        dc_date.date_day as date_key_created,
        de_date.date_day as date_key_expired

    from skills s
    inner join lifecycle l
        on l.offer_id = s.offer_id

    left join {{ ref('dim_location') }} dl
        on dl.dept_code = l.dept_code
       and dl.commune_code = l.commune_code
       and dl.code_postal = l.code_postal
       and dl.commune_name = l.commune_name

    left join {{ ref('dim_company') }} dc
        on dc.company_name = l.company_name
       and dc.naf_code = l.naf_code
       and dc.sector_code = l.sector_code

    left join {{ ref('dim_rome') }} dr
        on dr.rome_code = l.rome_code

    left join dim_dates dc_date
        on l.date_created is not null
       and date(l.date_created) = dc_date.date_day

    left join dim_dates de_date
        on l.date_expired is not null
       and date(l.date_expired) = de_date.date_day

)

select distinct *
from joined