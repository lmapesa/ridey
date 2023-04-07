WITH company as (
    SELECT * FROM {{ref('int_company') }}
),

trip_metrics as (
    SELECT * FROM {{ref('int_trip_metrics') }}
),

payment as (
    SELECT * FROM {{ref('int_payment') }}
),

trip_location as (
    SELECT * FROM {{ref('int_trip_location')}}
),

final as (
    SELECT
        company.company,
        COUNT(DISTINCT company.trip_id) AS no_of_trips, -- verify trip_id to use upstream company or trip_details then rm from int comp
        SUM(payment.trip_total) AS amount_earned,
        trip_location.pickup_census_tract,
        trip_location.dropoff_census_tract,
        COUNT(DISTINCT trip_metrics.taxi_id) AS no_of_taxis
    FROM company
    LEFT JOIN trip_metrics ON company.taxi_id = trip_metrics.taxi_id
    LEFT JOIN trip_location ON trip_location.trip_id = trip_metrics.trip_id
    LEFT JOIN Payment ON payment.trip_id = trip_location.trip_id
    GROUP BY company.company, trip_location.pickup_census_tract, trip_location.dropoff_census_tract
)

SELECT * FROM final