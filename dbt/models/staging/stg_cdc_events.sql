{{ config(materialized='view', schema='STAGING') }}
with source as (
    select * from {{ source('raw', 'cdc_events') }}
),

cleaned as (
    select
        event_id,
        place_id,
        change_type,
        detected_at,

        parse_json(new_value):rating::float as new_rating,
        parse_json(old_value):rating::float as old_rating,
        
        parse_json(new_value):review_count::int as new_review_count,
        parse_json(old_value):review_count::int as old_review_count
   from source
   where change_type = 'UPDATE'
    or change_type = 'INSERT')
select * from cleaned