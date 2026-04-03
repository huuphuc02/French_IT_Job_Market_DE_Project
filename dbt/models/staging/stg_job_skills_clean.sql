with source as (

    select * from {{ source('french_it_jobs', 'stg_job_skills') }}

),

cleaned as (

    select
        cast(offer_id as string) as offer_id,
        trim(cast(skill_label as string)) as skill_label,
        upper(cast(exigence_level as string)) as exigence_level,
        lower(cast(extraction_source as string)) as extraction_source,
        cast(ingested_at as timestamp) as ingested_at
    from source

),

filtered as (

    select *
    from cleaned
    where offer_id is not null
      and skill_label is not null
      and skill_label != ''

),

dedup as (

    select distinct
        offer_id,
        skill_label,
        exigence_level,
        extraction_source,
        ingested_at
    from filtered

)

select * from dedup