{{ config(materialized='table') }}

with offers as (
    select 
        f.offer_id,
        f.contract_type,
        f.experience_required,
        f.experience_label,
        case 
            when f.salary_min_annual is not null and f.salary_max_annual is not null
                then (f.salary_min_annual + f.salary_max_annual) / 2.0
            when f.salary_min_annual is not null then f.salary_min_annual
            when f.salary_max_annual is not null then f.salary_max_annual
        end as salary_annual_mid,
        dl.dept_name,
        dl.region_name,
        dl.dept_code,
        dr.rome_code,
        dr.rome_label
    from {{ ref('fact_job_offers') }} f 
    left join {{ ref('dim_location') }} dl on f.location_key = dl.location_key
    left join {{ ref('dim_rome') }} dr     on f.rome_key = dr.rome_key
    where f.salary_min_annual is not null or f.salary_max_annual is not null
),

skills as (
    select offer_id, skill_label
    from {{ ref('fact_job_skills') }}
    where skill_label is not null
),

joined as (
    select o.*, s.skill_label
    from offers o
    left join skills s on o.offer_id = s.offer_id
),

-- 1) Salary by region only
agg_region as (
    select
        'region'        as analysis_type,
        region_name,
        dept_name,
        dept_code,
        cast(null as string)  as experience_label,
        cast(null as string)   as experience_required,
        cast(null as string)  as contract_type,
        cast(null as string)  as rome_code,
        cast(null as string)  as rome_label,
        cast(null as string)  as skill_label,
        count(*)                                           as offer_count,
        avg(salary_annual_mid)                             as avg_salary_annual,
        min(salary_annual_mid)                             as min_salary_annual,
        max(salary_annual_mid)                             as max_salary_annual,
        APPROX_QUANTILES(salary_annual_mid, 4)[OFFSET(2)]  as median_salary_annual
    from joined
    group by region_name, dept_name, dept_code
    having count(*) > 10
),

-- 2) Salary by experience level only
agg_experience as (
    select
        'experience'    as analysis_type,
        cast(null as string)  as region_name,
        cast(null as string)  as dept_name,
        cast(null as string)  as dept_code,
        experience_label,
        experience_required,
        cast(null as string)  as contract_type,
        cast(null as string)  as rome_code,
        cast(null as string)  as rome_label,
        cast(null as string)  as skill_label,
        count(*)                                           as offer_count,
        avg(salary_annual_mid)                             as avg_salary_annual,
        min(salary_annual_mid)                             as min_salary_annual,
        max(salary_annual_mid)                             as max_salary_annual,
        APPROX_QUANTILES(salary_annual_mid, 4)[OFFSET(2)]  as median_salary_annual
    from joined
    group by experience_required, experience_label
    having count(*) > 10
),

-- 3) Salary by contract type only
agg_contract as (
    select
        'contract'      as analysis_type,
        cast(null as string)  as region_name,
        cast(null as string)  as dept_name,
        cast(null as string)  as dept_code,
        cast(null as string)  as experience_label,
        cast(null as string)   as experience_required,
        contract_type,
        cast(null as string)  as rome_code,
        cast(null as string)  as rome_label,
        cast(null as string)  as skill_label,
        count(*)                                           as offer_count,
        avg(salary_annual_mid)                             as avg_salary_annual,
        min(salary_annual_mid)                             as min_salary_annual,
        max(salary_annual_mid)                             as max_salary_annual,
        APPROX_QUANTILES(salary_annual_mid, 4)[OFFSET(2)]  as median_salary_annual
    from joined
    group by contract_type
    having count(*) > 10
),

-- 4) Salary by region × contract type (cross-cut worth keeping)
agg_region_contract as (
    select
        'region_contract' as analysis_type,
        region_name,
        cast(null as string)  as dept_name,
        cast(null as string)  as dept_code,
        cast(null as string)  as experience_label,
        cast(null as string)   as experience_required,
        contract_type,
        cast(null as string)  as rome_code,
        cast(null as string)  as rome_label,
        cast(null as string)  as skill_label,
        count(*)                                           as offer_count,
        avg(salary_annual_mid)                             as avg_salary_annual,
        min(salary_annual_mid)                             as min_salary_annual,
        max(salary_annual_mid)                             as max_salary_annual,
        APPROX_QUANTILES(salary_annual_mid, 4)[OFFSET(2)]  as median_salary_annual
    from joined
    group by region_name, contract_type
    having count(*) > 10
),

-- 5) Salary by ROME category
agg_rome as (
    select
        'rome'          as analysis_type,
        cast(null as string)  as region_name,
        cast(null as string)  as dept_name,
        cast(null as string)  as dept_code,
        cast(null as string)  as experience_label,
        cast(null as string)   as experience_required,
        cast(null as string)  as contract_type,
        rome_code,
        rome_label,
        cast(null as string)  as skill_label,
        count(*)                                           as offer_count,
        avg(salary_annual_mid)                             as avg_salary_annual,
        min(salary_annual_mid)                             as min_salary_annual,
        max(salary_annual_mid)                             as max_salary_annual,
        APPROX_QUANTILES(salary_annual_mid, 4)[OFFSET(2)]  as median_salary_annual
    from joined
    group by rome_code, rome_label
    having count(*) > 10
),

-- 6) Salary by skill
agg_skill as (
    select
        'skill'         as analysis_type,
        cast(null as string)  as region_name,
        cast(null as string)  as dept_name,
        cast(null as string)  as dept_code,
        cast(null as string)  as experience_label,
        cast(null as string)   as experience_required,
        cast(null as string)  as contract_type,
        cast(null as string)  as rome_code,
        cast(null as string)  as rome_label,
        skill_label,
        count(distinct offer_id)                           as offer_count,
        avg(salary_annual_mid)                             as avg_salary_annual,
        min(salary_annual_mid)                             as min_salary_annual,
        max(salary_annual_mid)                             as max_salary_annual,
        APPROX_QUANTILES(salary_annual_mid, 4)[OFFSET(2)]  as median_salary_annual
    from joined
    where skill_label is not null
    group by skill_label
    having count(distinct offer_id) > 10
)

select * from agg_region
union all
select * from agg_experience
union all
select * from agg_contract
union all
select * from agg_region_contract
union all
select * from agg_rome
union all
select * from agg_skill