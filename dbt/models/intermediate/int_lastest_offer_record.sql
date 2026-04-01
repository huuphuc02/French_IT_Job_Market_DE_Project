with base as (
    select *
    from {{ ref('stg_job_offers_clean')}}
    where event_type in ('INSERT', 'UPDATE')
),

ranked as (
    select *,
    row_number() over (
        partition by offer_id
        order by coalesce(date_updated, date_created) desc,
            ingested_at desc
    ) as rn
    from base
)

select
  offer_id,
  event_type,
  ingested_at,
  is_active,
  date_created,
  date_updated,
  title,
  description,
  rome_code,
  rome_label,
  appellation_label,
  dept_code,
  commune_code,
  commune_name,
  code_postal,
  latitude,
  longitude,
  company_name,
  company_size,
  naf_code,
  sector_code,
  sector_label,
  contract_type,
  contract_type_label,
  work_duration,
  is_alternance,
  experience_required,
  experience_label,
  qualification_label,
  education_level,
  salary_text,
  salary_min_annual,
  salary_max_annual,
  is_accessible_th,
  num_positions
from ranked
where rn = 1

