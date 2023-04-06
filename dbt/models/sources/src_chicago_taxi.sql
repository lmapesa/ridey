-- Materialize as view

{{ config(alias = 'src_chicago_taxi', materialized='view') }}


WITH chicago_taxi as (
    SELECT 
*

    FROM {{source ('dbt_raw_data', 'chicago_data')}}
    LIMIT 100000
)

SELECT * FROM chicago_taxi