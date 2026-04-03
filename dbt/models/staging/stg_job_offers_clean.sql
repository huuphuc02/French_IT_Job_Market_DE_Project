with source as (
    select * from {{ source('french_it_jobs', 'stg_job_offers')}}
),

cleaned as (
    select 
        -- identifiers
        cast(offer_id as string) as offer_id,
        upper(cast(event_type as string)) as event_type,

        -- timestamps
        cast(ingested_at as timestamp) as ingested_at,
        cast(is_active as bool) as is_active,
        cast(date_created as timestamp) as date_created,
        cast(date_updated as timestamp) as date_updated,

        -- title
        nullif(trim(cast(title as string)), '') as title,
        nullif(trim(cast(description as string)), '') as description,

        nullif(trim(cast(rome_code as string)), '') as rome_code,
        nullif(trim(cast(rome_label as string)), '') as rome_label,
        nullif(trim(cast(appellation_label as string)), '') as appellation_label,

        -- positions
        nullif(trim(cast(dept_code as string)), '') as dept_code,
        nullif(trim(cast(commune_code as string)), '') as commune_code,
        nullif(trim(cast(commune_name as string)), '') as commune_name,
        nullif(trim(cast(code_postal as string)), '') as code_postal,
        cast(latitude as float64) as latitude,
        cast(longitude as float64) as longitude,

        -- company
        nullif(trim(cast(company_name as string)), '') as company_name,
        nullif(trim(cast(company_size as string)), '') as company_size,
        nullif(trim(cast(naf_code as string)), '') as naf_code,
        nullif(trim(cast(sector_code as string)), '') as sector_code,
        nullif(trim(cast(sector_label as string)), '') as sector_label,

        -- contract
        nullif(trim(cast(contract_type as string)), '') as contract_type,
        nullif(trim(cast(contract_type_label as string)), '') as contract_type_label,
        nullif(trim(cast(work_duration as string)), '') as work_duration,
        cast(is_alternance as bool) as is_alternance,

        -- experience and education
        nullif(trim(cast(experience_required as string)), '') as experience_required,
        nullif(trim(cast(experience_label as string)), '') as experience_label,
        nullif(trim(cast(qualification_label as string)), '') as qualification_label,
        nullif(trim(cast(education_level as string)), '') as education_level,

        -- salary
        nullif(trim(cast(salary_text as string)), '') as salary_text,
        cast(salary_min_annual as numeric) as salary_min_annual,
        cast(salary_max_annual as numeric) as salary_max_annual,

        cast(is_accessible_th as bool) as is_accessible_th,
        cast(num_positions as int64) as num_positions
    from source
)

select *
from cleaned
where offer_id is not null