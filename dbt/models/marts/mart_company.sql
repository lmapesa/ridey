WITH 
company AS (
    SELECT 
        taxi_id,
        trip_id,
        company
    FROM {{ref('int_company')}}
),

trip_metrics AS (
    SELECT 
        taxi_id,
        COUNT(trip_id) AS no_of_trips_per_taxi
    FROM {{ref('int_trip_metrics')}}
    GROUP BY taxi_id
),

payment AS (
    SELECT 
        trip_id,
        SUM(trip_total) AS amount_earned
    FROM {{ref('int_payment')}}
    GROUP BY trip_id
),

trip_location AS (
    SELECT 
        trip_id,
        pickup_community_area,
        dropoff_community_area
    FROM {{ref('int_trip_location')}}
),

summary AS (
    SELECT 
        company.company, 
        mode(trip_location.pickup_community_area) AS most_common_pickup_community_area,
        mode(trip_location.dropoff_community_area) AS most_common_dropoff_community_area,
        COUNT(DISTINCT company.taxi_id) AS no_of_taxis,
        COALESCE(SUM(payment.amount_earned), 0) AS amount_earned
    FROM trip_location
    JOIN company ON trip_location.trip_id = company.trip_id
    LEFT JOIN payment ON payment.trip_id = company.trip_id
    GROUP BY company
),

final AS (
    SELECT
        company.company,
        summary.no_of_taxis,
        summary.amount_earned,
        summary.most_common_pickup_community_area,
        summary.most_common_dropoff_community_area
    FROM company
    JOIN summary ON summary.company = company.company
    JOIN trip_metrics ON company.taxi_id = trip_metrics.taxi_id
    GROUP BY 1,2,3,4,5
)

SELECT * FROM final
