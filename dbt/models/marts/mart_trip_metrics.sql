WITH trip_metrics as (
    SELECT * FROM {{ref('int_trip_metrics') }}

),

trip_time as (
    SELECT * FROM {{ref('int_trip_time') }}
),

final as (
    SELECT
        trip_metrics.trip_id,
        trip_time.trip_seconds,
        trip_time.trip_minutes,
        trip_hours,
        trip_time.hour,
        trip_time.trip_kilometers,
        CASE WHEN trip_time.trip_hours > 0 
            THEN trip_time.trip_kilometers/trip_time.trip_hours 
            ELSE NULL 
        END AS speed_kph,
        trip_time.trip_miles,
        trip_time.trip_start_timestamp as trip_start_time,
        trip_time.trip_end_timestamp as trip_end_time,
        COUNT(trip_metrics.trip_id) as no_of_trips
    FROM trip_metrics
    LEFT JOIN trip_time ON trip_metrics.trip_id = trip_time.trip_id
    GROUP BY 1,2,3,4,5,6,7,8,9,10
)

SELECT * FROM final