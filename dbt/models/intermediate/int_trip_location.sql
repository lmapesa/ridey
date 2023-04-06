-- Materialize as view

{{ config(alias = 'Trip Location details', materialized='view') }}

WITH trip_location as (
    SELECT * FROM {{ ref('stg_chicago_taxi') }}

),

final as (
    SELECT 
        trip_id,
        pickup_census_tract,
        dropoff_census_tract,
        pickup_community_area,
        dropoff_community_area,
        pickup_centroid_location,
        dropoff_centroid_location
    FROM trip_location
)

SELECT * FROM final