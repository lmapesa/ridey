WITH trip_metrics as (
    SELECT * FROM {{ ref('stg_chicago_taxi') }}

),

final as (
    SELECT 
            trip_id,
            taxi_id
    FROM trip_metrics
)

SELECT * FROM final