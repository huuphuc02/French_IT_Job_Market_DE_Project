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
        case l.contract_type
            when 'MIS' then 'Interim'
            when 'LIB' then 'Liberal'
            when 'APP' then 'Apprenticeship'
            when 'PRO' then 'Professional training contract'
            else l.contract_type
        end as contract_type,
        l.contract_type_label,
        l.is_alternance,

        case 
            when l.experience_required = 'E' then 'Required'
            when l.experience_required = 'D' then 'Beginner accepted'
            when l.experience_required = 'S' then 'Desired'
            else ''
        end as experience_required,
        case 
            when l.experience_label = 'Débutant accepté' then '0 An(s)'
            when l.experience_label = 'Expérience exigée' then 'Experience required (not specified)'
            else {{ first_element("l.experience_label", "' - '") }}
        end as experience_label,
        l.qualification_label,

        l.salary_min_annual,
        l.salary_max_annual,
        l.salary_text,
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

),

experience_grouped as (
    select
        *,
        case experience_label
            -- Entry level: 0 to 6 months
            when 'Débutant accepté'                    then 'Entry Level (0-6 months)'
            when '0 An(s)'                             then 'Entry Level (0-6 months)'
            when '2 Mois'                              then 'Entry Level (0-6 months)'
            when '3 Mois'                              then 'Entry Level (0-6 months)'
            when '4 Mois'                              then 'Entry Level (0-6 months)'
            when '5 Mois'                              then 'Entry Level (0-6 months)'
            when '6 Mois'                              then 'Entry Level (0-6 months)'
            -- Junior: 6 to 18 months
            when '12 Mois'                             then 'Junior (6-18 months)'
            when '18 Mois'                             then 'Junior (6-18 months)'
            when '1 An(s)'                             then 'Junior (6-18 months)'
            -- Mid-level: 2 to 3 years
            when '2 An(s)'                             then 'Mid-Level (2-3 years)'
            when '24 Mois'                             then 'Mid-Level (2-3 years)'
            when '32 Mois'                             then 'Mid-Level (2-3 years)'
            when '36 Mois'                             then 'Mid-Level (2-3 years)'
            -- Experienced: 3 to 5 years
            when '3 An(s)'                             then 'Experienced (3-5 years)'
            when '4 An(s)'                             then 'Experienced (3-5 years)'
            when '48 Mois'                             then 'Experienced (3-5 years)'
            -- Senior: 5+ years
            when '5 An(s)'                             then 'Senior (5+ years)'
            when '6 An(s)'                             then 'Senior (5+ years)'
            when '7 An(s)'                             then 'Senior (5+ years)'
            when '8 An(s)'                             then 'Senior (5+ years)'
            when '9 An(s)'                             then 'Senior (5+ years)'
            when '10 An(s)'                            then 'Senior (5+ years)'
            when '11 An(s)'                            then 'Senior (5+ years)'
            when '15 An(s)'                            then 'Senior (5+ years)'
            -- Unknown
            when 'Experience required (not specified)' then 'Not Specified'
            else                                            'Not Specified'
    end as experience_group,
    from joined
)

select *
from experience_grouped