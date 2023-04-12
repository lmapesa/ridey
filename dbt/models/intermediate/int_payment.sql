{% set payment_types = ['Credit Card', 'Mobile', 'Unknown', 'No Charge', 'Dispute', 'Pcard', 'Prepaid', 'Prcard', 'Split'] %}

WITH payment as (
    SELECT * FROM {{ ref('stg_chicago_taxi') }}

),

stage as (
    SELECT 
        trip_id,
        taxi_id,
        fare,
        tips,
        tolls,
        extras,
        SUM(fare + tips + tolls + extras) AS trip_total,
        COALESCE(payment_type, 'Unknown') AS payment_type

    FROM payment
    GROUP BY trip_id, taxi_id, fare, tips, tolls, extras, payment_type
),

final as (
    SELECT 
        trip_id,
        taxi_id,
        fare,
        tips,
        tolls,
        extras,
        trip_total,
        {% for payment_method in payment_methods -%}
            sum(case when payment_type = '{{ payment_type }}' then trip_total else 0 end) as {{ payment_type }}_trip_total,
        {% endfor -%}

        payment_type
    FROM stage

)

SELECT * FROM final