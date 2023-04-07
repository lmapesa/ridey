WITH trip_location as (
    SELECT * FROM {{ ref('stg_chicago_taxi') }}

),

final as (
    SELECT 
        trip_id,
        pickup_census_tract,
        dropoff_census_tract,
        pickup_community_area,
        -- COALESCE(df.film_id, 'O') AS film_id,
        dropoff_community_area,
        pickup_centroid_location,
        dropoff_centroid_location
    FROM trip_location
)

SELECT * FROM final