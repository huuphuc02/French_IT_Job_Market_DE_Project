{{ config(materialized='table') }}

with dedup as (

    select 
        max(dept_code) as dept_code,
        max(commune_code) as commune_code,
        coalesce(commune_name, '') as commune_name,
        coalesce(code_postal, '') as code_postal
    from {{ ref('int_offer_lifecycle') }}
    group by 3, 4

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
        case 
            when starts_with(commune_name, 'Paris') then 'Paris'
            when starts_with(commune_name, 'Lyon') then 'Lyon'
            when starts_with(commune_name, 'Marseille') then 'Marseille'
            else commune_name
        end as commune_name,
        code_postal,
        dept_code,
        dept_name,
        region_name
    from enriched

)

select * from final
where commune_name is not null or code_postal is not null