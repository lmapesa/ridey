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
        trip_time.trip_miles,
        trip_time.trip_start_timestamp as trip_start_time,
        trip_time.trip_end_timestamp as trip_end_time,
        COUNT(trip_metrics.trip_id) as no_of_trips
    FROM trip_metrics
    LEFT JOIN trip_time ON trip_metrics.trip_id = trip_time.trip_id
    GROUP BY trip_metrics.trip_id, trip_seconds, trip_miles, trip_start_time, trip_end_time
)

SELECT * FROM final