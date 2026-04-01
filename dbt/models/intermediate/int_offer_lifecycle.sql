{{ config(materialized='table') }}

with latest_offer as (
    select * from {{ ref('int_latest_offer_record')}}
),

latest_expire as (
    select * from {{ ref('int_offer_expiration_latest')}}
),

joined as (
    select o.*,
    e.date_expired,
    case 
        when e.offer_id is null then true
        else false
    end as is_active_final,
    case 
        when o.date_created is not null and e.date_expired is not null
        then date_diff(date(e.date_expired), date(o.date_created), day)
        else null 
    end as offer_lifetime_days
    from latest_offer o
    left join latest_expire e
    on o.offer_id = e.offer_id
)

select
  offer_id,
  event_type,
  ingested_at,
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
  num_positions,
  is_active_final as is_active,
  date_expired,
  offer_lifetime_days
from joined