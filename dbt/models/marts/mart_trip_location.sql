WITH trip_location as (
    SELECT * FROM {{ref('int_trip_location')}}
),

trip_metrics as (
    SELECT * FROM {{ref('int_trip_metrics')}}
),

payment as (
    SELECT * FROM {{ref('int_payment')}}
),

final as (
    SELECT
        trip_location.trip_id,
        trip_metrics.taxi_id,
        trip_location.pickup_census_tract as pickup_area,
        trip_location.dropoff_census_tract as dropoff_area,
        COUNT(trip_location.trip_id) as no_of_trips,
        COUNT(trip_location.pickup_census_tract) as no_of_trips_requested,
        COUNT(trip_location.dropoff_census_tract) as no_of_trips_arrived,
        -- IS WITHIN TONW?
        SUM(payment.trip_total) as amount_spent_on_route
    FROM trip_location
    LEFT JOIN trip_metrics ON trip_location.trip_id = trip_location.trip_id
    LEFT JOIN payment ON payment.trip_id = trip_location.trip_id
    GROUP BY trip_location.trip_id, trip_metrics.taxi_id, pickup_area, dropoff_area
)

SELECT * FROM final