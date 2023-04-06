-- Materialize as view

{{ config(alias = 'Company details', materialized='view') }}

WITH company as (
    SELECT * FROM {{ ref('stg_chicago_taxi') }}

),

final as (
    SELECT 
        taxi_id,
        company
    FROM company
)

SELECT * FROM final