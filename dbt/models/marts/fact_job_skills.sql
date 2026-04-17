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

        {{ dbt_utils.generate_surrogate_key(["coalesce(l.commune_name, '')", "coalesce(l.code_postal, '')"]) }} as location_key,
        {{ dbt_utils.generate_surrogate_key(["coalesce(l.company_name, '')", "coalesce(l.naf_code, '')", "coalesce(l.sector_code, '')"]) }} as company_key,
        {{ dbt_utils.generate_surrogate_key(["coalesce(l.rome_code, '')"]) }} as rome_key,

        dc_date.date_day as date_key_created,
        de_date.date_day as date_key_expired

    from skills s
    inner join lifecycle l
        on l.offer_id = s.offer_id



    left join dim_dates dc_date
        on l.date_created is not null
       and date(l.date_created) = dc_date.date_day

    left join dim_dates de_date
        on l.date_expired is not null
       and date(l.date_expired) = de_date.date_day

)

select distinct *
from joined