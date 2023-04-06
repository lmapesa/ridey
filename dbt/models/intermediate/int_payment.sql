-- Materialize as view

{{ config(alias = 'Payment details', materialized='view') }}

WITH payment as (
    SELECT * FROM {{ ref('stg_chicago_taxi') }}

),

final as (
    SELECT 
        trip_id,
        taxi_id,
        fare,
        tips,
        tolls,
        extras,
        SUM(fare + tips + tolls + extras) AS trip_total,
        -- SUM(fare + tips + tolls + extras) OVER (PARTITION BY trip_id) AS trip_total,
        payment_type
    FROM payment
    GROUP BY trip_id, taxi_id, fare, tips, tolls, extras, payment_type
)

SELECT * FROM final