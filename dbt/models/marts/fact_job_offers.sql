{{ config(materialized='table') }}

with lifecycle as (

    select *
    from {{ ref('int_offer_lifecycle') }}

),

dim_dates as (

    select *
    from {{ ref('dim_date') }}

),

joined as (

    select
        l.offer_id,
        l.title,
        l.description,

        -- Date keys (DATE grain)
        dc.date_day as date_key_created,
        du.date_day as date_key_updated,
        de.date_day as date_key_expired,

        -- Lifecycle
        l.is_active,
        l.date_created,
        l.date_updated,
        l.date_expired,
        l.offer_lifetime_days,

        -- Dimension keys
        dl.location_key,
        dcw.company_key,
        dr.rome_key,

        -- Measures / descriptive attributes carried in the fact
        l.contract_type,
        l.contract_type_label,
        l.work_duration,
        l.is_alternance,

        l.experience_required,
        l.experience_label,
        l.qualification_label,

        l.salary_min_annual,
        l.salary_max_annual,
        l.salary_text,

        l.is_accessible_th,
        l.num_positions

    from lifecycle l

    left join dim_dates dc
        on l.date_created is not null
       and date(l.date_created) = dc.date_day

    left join dim_dates du
        on l.date_updated is not null
       and date(l.date_updated) = du.date_day

    left join dim_dates de
        on l.date_expired is not null
       and date(l.date_expired) = de.date_day

    left join {{ ref('dim_location') }} dl
        on dl.code_postal = l.code_postal
       and dl.commune_name = l.commune_name

    left join {{ ref('dim_company') }} dcw
        on dcw.company_name = l.company_name
       and dcw.naf_code = l.naf_code
       and dcw.sector_code = l.sector_code

    left join {{ ref('dim_rome') }} dr
        on dr.rome_code = l.rome_code

)

select *
from joined