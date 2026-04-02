{{ config(materialized='table') }}

with dedup as (

    select distinct
        dept_code,
        commune_code,
        commune_name,
        code_postal
    from {{ ref('int_offer_lifecycle') }}

),

enriched as (
    select
        d.*,
        s.dept_name,
        s.region_name
    from dedup d
    left join {{ ref('dept_region') }} s
      on d.dept_code = s.dept_code

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key([
            "coalesce(commune_name, '')",
            "coalesce(code_postal, '')"
        ]) }} as location_key,
        commune_code,
        commune_name,
        code_postal,
        dept_code,
        dept_name,
        region_name
    from enriched

)

select * from final
where commune_name is not null or code_postal is not null