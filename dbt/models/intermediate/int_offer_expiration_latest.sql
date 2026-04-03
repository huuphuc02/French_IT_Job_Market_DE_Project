{{ config(materialized='table') }}

with base as (
  select
      offer_id,
      cast(expired_at as timestamp) as date_expired
  from {{ ref('stg_offer_expirations_clean') }}
  where offer_id is not null
),

ranked as (
  select
      offer_id,
      date_expired,
      row_number() over (
          partition by offer_id
          order by date_expired desc
      ) as rn
  from base
)

select
  offer_id,
  date_expired
from ranked
where rn = 1