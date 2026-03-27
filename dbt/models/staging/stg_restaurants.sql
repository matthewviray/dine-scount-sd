{{ config(materialized='view', schema='STAGING') }}
with source as (
    select * from {{ source('raw', 'raw_restaurants') }}
), 

cleaned as (
    select
        place_id,
        name,
        rating::float as rating,
        review_count::int as review_count,
        address,
        lat::float as lat,
        lng::float as lng,
        neighborhood,
        website,
        photo_url,
        ingested_at,
        case price_level
        when 'PRICE_LEVEL_INEXPENSIVE' then 1
        when 'PRICE_LEVEL_MODERATE' then 2
        when 'PRICE_LEVEL_EXPENSIVE' then 3
        when 'PRICE_LEVEL_VERY_EXPENSIVE' then 4
        else null
        end as price_level,
        types as all_types,
        parse_json(hours) as hours
    from source
    where rating is not null
)
select * from cleaned