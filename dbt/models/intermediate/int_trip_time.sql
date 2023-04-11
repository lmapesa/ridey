WITH trip_time as (
    SELECT * FROM {{ ref('stg_chicago_taxi') }}

),

final as (
    SELECT 
        trip_id,
        trip_start_timestamp,
        trip_end_timestamp,
        trip_seconds,
        (trip_seconds)/60 AS trip_minutes,
        (trip_seconds)/3600 AS trip_hours,
        trip_miles,
        ROUND(CAST(trip_miles * 1.60934 AS numeric), 2) AS trip_kilometers,
        DATE_PART('hour', trip_start_timestamp) AS hour,
        COUNT(trip_id) AS no_of_trips,
        EXTRACT(DOW FROM trip_start_timestamp) AS day_of_week,
        date_part('dow', trip_start_timestamp::date) AS day_of_week2,
        EXTRACT(MONTH FROM trip_start_timestamp) AS month,
        EXTRACT(YEAR FROM trip_start_timestamp) AS year
    FROM trip_time
    GROUP BY trip_id, trip_start_timestamp, trip_end_timestamp, trip_seconds, trip_miles
    ORDER BY no_of_trips DESC
)

SELECT * FROM final