WITH company as (
    SELECT * FROM {{ ref('stg_chicago_taxi') }}

),

final as (
    SELECT 
        {{ dbt_utils.surrogate_key(['company' ]) }} as company_id,
        taxi_id,
        trip_id,
        company
    FROM company
)

SELECT * FROM final