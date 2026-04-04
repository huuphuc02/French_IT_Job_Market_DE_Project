    {{ config(materialized='table') }}

with skills as (

    select
        s.offer_id,
        s.skill_label,
        s.exigence_level,           -- 'E' = required, 'S' = desired
        dl.region_name,
        dl.dept_name,
        dl.dept_code,
        dr.rome_code,
        dr.rome_label,
        f.contract_type,
        f.experience_required,
        f.experience_label,
        f.is_active,
        date(f.date_created)        as date_created
    from {{ ref('fact_job_skills') }} s
    left join {{ ref('fact_job_offers') }} f  on s.offer_id      = f.offer_id
    left join {{ ref('dim_location') }}   dl  on f.location_key  = dl.location_key
    left join {{ ref('dim_rome') }}       dr  on f.rome_key      = dr.rome_key
    where s.skill_label is not null

),

-- Overall skill demand across all offers (active + historical)
overall_demand as (

    select
        skill_label,
        exigence_level,
        cast(null as string) as region_name,
        cast(null as string) as dept_name,
        cast(null as string) as dept_code,
        cast(null as string) as rome_code,
        cast(null as string) as rome_label,
        cast(null as string) as contract_type,
        cast(null as bool)   as is_active_filter,
        'overall'            as analysis_type,
        count(distinct offer_id)   as offer_count
    from skills
    group by skill_label, exigence_level
    having count(distinct offer_id) > 10
),

-- Skill demand filtered to active offers only (current market snapshot)
active_demand as (

    select
        skill_label,
        exigence_level,
        cast(null as string) as region_name,
        cast(null as string) as dept_name,
        cast(null as string) as dept_code,
        cast(null as string) as rome_code,
        cast(null as string) as rome_label,
        cast(null as string) as contract_type,
        true                 as is_active_filter,
        'active_only'        as analysis_type,
        count(distinct offer_id)   as offer_count
    from skills
    where is_active = true
    group by skill_label, exigence_level
    having count(distinct offer_id) > 10
),

-- Skill demand by ROME code
by_rome as (

    select
        skill_label,
        cast(null as string) as region_name,
        cast(null as string) as dept_name,
        cast(null as string) as dept_code,
        rome_code,
        rome_label,
        cast(null as string) as contract_type,
        cast(null as bool)   as is_active_filter,
        'by_rome'            as analysis_type,
        count(distinct offer_id)   as offer_count
    from skills
    where rome_code is not null
    group by skill_label, rome_code, rome_label
    having count(distinct offer_id) > 10

),

-- Skill demand by experience level
by_experience as (

    select
        skill_label,
        exigence_level,
        cast(null as string) as region_name,
        cast(null as string) as dept_name,
        cast(null as string) as dept_code,
        cast(null as string) as rome_code,
        cast(null as string) as rome_label,
        contract_type,
        cast(null as bool)   as is_active_filter,
        'by_contract'        as analysis_type,
        count(distinct offer_id)   as offer_count
    from skills
    where contract_type is not null
    group by skill_label, exigence_level, contract_type
    having count(distinct offer_id) > 10

)

select * from overall_demand
union all
select * from active_demand
union all
select * from by_rome
union all
select * from by_contract
