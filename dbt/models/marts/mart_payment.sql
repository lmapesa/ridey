WITH trip_metrics as (
    SELECT * FROM {{ref('int_trip_metrics')}}

),

payment as (
    SELECT * FROM {{ref('int_payment') }}

),

trip_time as (
    SELECT * FROM {{ref('int_trip_time')}}

),

final as (
    SELECT
        payment.trip_id,
        trip_metrics.taxi_id,
        payment.payment_type,
        payment.trip_total,
        trip_time.trip_seconds,
        trip_time.trip_miles
    FROM payment
    LEFT JOIN trip_metrics ON payment.trip_id = trip_metrics.trip_id
    LEFT JOIN trip_time ON payment.trip_id = trip_time.trip_id
)

SELECT * FROM final