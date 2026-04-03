{{ config(materialized='table') }}

with base as (

    select distinct
        rome_code,
        rome_label
    from {{ ref('int_offer_lifecycle') }}

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key(["coalesce(rome_code, '')"]) }} as rome_key,
        rome_code,
        rome_label
    from base

)

select * from final
where rome_code is not null and rome_label is not null