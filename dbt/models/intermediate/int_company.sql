WITH company as (
    SELECT * FROM {{ ref('stg_chicago_taxi') }}

),

final as (
    SELECT 
        taxi_id,
        trip_id,
        company
    FROM company
)

SELECT * FROM final