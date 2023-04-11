WITH trip_time AS (
    SELECT * FROM {{ref('int_trip_time')}}
),

final as (
    SELECT
        trip_id,
        trip_start_timestamp,
        trip_end_timestamp,
        trip_seconds,
        trip_miles,
        trip_kilometers,
        hour,
        CASE 
            WHEN day_of_week = 0 THEN 'Sunday'
            WHEN day_of_week = 1 THEN 'Monday'
            WHEN day_of_week = 2 THEN 'Tuesday'
            WHEN day_of_week = 3 THEN 'Wednesday'
            WHEN day_of_week = 4 THEN 'Thursday'
            WHEN day_of_week = 5 THEN 'Friday'
            WHEN day_of_week = 6 THEN 'Saturday'
            WHEN day_of_week = 7 THEN 'Sunday'
            ELSE NULL
        END AS day_of_week,
        trip_minutes,
        no_of_trips,
        CASE 
            WHEN month = 1 THEN 'January'
            WHEN month = 2 THEN 'February'
            WHEN month = 3 THEN 'March'
            WHEN month = 4 THEN 'April'
            WHEN month = 5 THEN 'May'
            WHEN month = 6 THEN 'June'
            WHEN month = 7 THEN 'July'
            WHEN month = 8 THEN 'August'
            WHEN month = 9 THEN 'September'
            WHEN month = 10 THEN 'October'
            WHEN month = 11 THEN 'November'
            WHEN month = 12 THEN 'December'
            ELSE NULL
        END AS month,
        year
    FROM trip_time
)

SELECT * FROM final