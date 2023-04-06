-- Perfrom aggregations, conversions, 1:1 matching data source, pick important fields
-- Materialize as view

{{ config(alias = 'stg_chicago_taxi', materialized='view') }}

WITH chicago_taxi as (
    SELECT * FROM {{ ref('src_chicago_taxi') }}

), 

duplicates as (
    SELECT 
        trip_id, 
        taxi_id, 
        trip_start_timestamp, 
        trip_end_timestamp, 
        trip_seconds, 
        trip_miles, 
        pickup_census_tract, 
        dropoff_census_tract,
        pickup_community_area,
        dropoff_community_area,
        fare,
        tips,
        tolls,
        extras,
        -- trip_total,
        payment_type,
        company,
        -- pickup_centroid_latitude,
        -- pickup_centroid_longitude,
        pickup_centroid_location,
        -- dropoff_centroid_latitude,
        -- dropoff_centroid_longitude,
        dropoff_centroid_location,
        ROW_NUMBER() OVER(PARTITION BY TRIP_ID ORDER BY TRIP_ID) AS DUP_COUNT
    FROM chicago_taxi
),

final as (
    SELECT * 
    FROM duplicates 
    WHERE DUP_COUNT = 1
)

SELECT * FROM final
