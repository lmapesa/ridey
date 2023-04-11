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
        trip_location.pickup_census_tract AS pickup_area,
        trip_location.dropoff_census_tract AS dropoff_area,
        pickup_community_area,
        dropoff_community_area,
        COUNT(*) AS no_of_trips,
        COUNT(trip_location.pickup_census_tract) AS no_of_trips_requested,
        COUNT(trip_location.dropoff_census_tract) AS no_of_trips_arrived,
        SUM(payment.trip_total) AS amount_spent_on_route
    FROM trip_location
    LEFT JOIN trip_metrics ON trip_location.trip_id = trip_metrics.trip_id
    LEFT JOIN payment ON payment.trip_id = trip_location.trip_id
    GROUP BY 1,2,3,4,5,6
)

SELECT * FROM final