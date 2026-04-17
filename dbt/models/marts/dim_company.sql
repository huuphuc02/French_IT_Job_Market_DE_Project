{{ config(materialized='table') }}

with dedup as (
    select 
        coalesce(company_name, '') as company_name,
        coalesce(naf_code, '') as naf_code,
        coalesce(sector_code, '') as sector_code,
        max(company_size) as company_size,
        max(sector_label) as sector_label
    from {{ ref('int_offer_lifecycle')}}
    group by 1, 2, 3
),

with_key as (
    select {{ dbt_utils.generate_surrogate_key([
          "coalesce(company_name, '')",
          "coalesce(naf_code, '')",
          "coalesce(sector_code, '')"
      ]) }} as company_key,
      company_name,
        company_size,
        naf_code,
        sector_code,
        sector_label
    from dedup
)

select * from with_key
where company_name is not null