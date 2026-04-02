{{ config(materialized='table') }}

with dates_from_offers as (

    select date(date_created) as date_day
    from {{ ref('int_offer_lifecycle') }}
    where date_created is not null

    union distinct

    select date(date_updated) as date_day
    from {{ ref('int_offer_lifecycle') }}
    where date_updated is not null

    union distinct

    select date(date_expired) as date_day
    from {{ ref('int_offer_lifecycle') }}
    where date_expired is not null

),

final as (

    select
        date_day,
        extract(year from date_day) as year,
        extract(quarter from date_day) as quarter,
        extract(month from date_day) as month,
        format_date('%B', date_day) as month_name,
        extract(week from date_day) as week_of_year,
        extract(day from date_day) as day_of_month,
        extract(dayofweek from date_day) as day_of_week,
        format_date('%A', date_day) as day_name,
        case
            when extract(dayofweek from date_day) in (1, 7) then true
            else false
        end as is_weekend
    from dates_from_offers

)

select * from final