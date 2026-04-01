with source as (
    select * from {{ source('french_it_jobs', 'stg_offer_expirations')}}
),

cleaned as (
    select cast(offer_id as string) as offer_id,
        cast(ingested_at as timestamp) as ingested_at,
        cast(expired_at as timestamp) as expired_at
    from source
    where offer_id is not null
),

dedup as (
    select distinct offer_id,
        ingested_at,
        expired_at
    from cleaned
)

select * from dedup