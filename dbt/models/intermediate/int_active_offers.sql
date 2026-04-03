{{ config(materialized='view')}}

select *
from {{ ref('int_offer_lifecycle')}}
where is_active = true