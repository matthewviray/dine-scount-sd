{{ config(materialized='table', schema='MARTS') }}

with restaurants as (
    select * from {{ ref('stg_restaurants') }}
),
cdc_events as (
    select * from {{ ref('stg_cdc_events') }}
),

--neighborhood avg popularity and newcomer score
neighborhood_scores as (
    select
        neighborhood,
        avg(review_count) as avg_review_count,
        avg(rating) as avg_rating
        from restaurants
        group by neighborhood),

-- velocity socre rating change over last 30 days
velocity as (
    select
        place_id,
        sum(new_rating - old_rating) as velocity_score
        from cdc_events
        where detected_at >= dateadd(day, -30, current_date)
        and change_type = 'UPDATE'
        and old_rating is not null
        and new_rating is not null
        group by place_id

),
first_seen as (
    select
        place_id,
        min(detected_at) as first_seen_at
        from {{ source('raw', 'cdc_events') }}
        where change_type = 'INSERT'
        group by place_id
),

scored as (
    select
        r.place_id,
        r.name,
        r.rating,
        r.review_count,
        r.price_level,
        r.neighborhood,
        r.website,
        r.photo_url,
        r.address,
        r.all_types,
        r.hours,
        r.lat,
        r.lng,
        r.ingested_at,
        round(r.rating *2,2) as rating_score,
        round(least(r.review_count/ nullif(n.avg_review_count,0) * 5,10),2) as popularity_score,
        round(coalesce(v.velocity_score,0) * 2,2) as velocity_score,
        round(r.rating/nullif(log(10,r.review_count + 1),0),2) as hidden_gem_score,

        case
            when f.first_seen_at >= dateadd(day, -180, current_date)
            and r.rating >= n.avg_rating then 1
            else 0
        end as newcomer_score,

        case when r.rating * 2 >= 8
            and r.review_count >= n.avg_review_count
            then true else false
        end as is_best_overall,

        case when coalesce(v.velocity_score, 0) > 0
            then true else false
        end as is_hot_right_now,

        case when r.rating / nullif(log(10,r.review_count + 1), 0) >= 1.8 and r.review_count >= 30
            then true else false -- high enough rating relative to review count to be a hidden gem(ex 4.5 rating with 300 reviews is ~1.8)
        end as is_hidden_gem,

        case when f.first_seen_at >= dateadd(day, -180, current_timestamp)
            and r.rating >= n.avg_rating
            then true else false
        end as is_new_spot,
        f.first_seen_at
        from restaurants r
        left join neighborhood_scores n 
            on r.neighborhood = n.neighborhood
        left join velocity v
            on r.place_id = v.place_id
        left join first_seen f
            on r.place_id = f.place_id
)
select * from scored

        