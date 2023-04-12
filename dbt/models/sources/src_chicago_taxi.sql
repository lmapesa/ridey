{{ 
  config(
    materialized='incremental',
    unique_key='trip_id',
    sort='trip_start_timestamp',
    on_schema_change='fail',
    incremental_strategy='merge'
  ) 
}}

WITH chicago_taxi as (
    SELECT 
*

    FROM {{source ('dbt_raw_data', 'chicago_data')}}
    WHERE trip_start_timestamp BETWEEN '2021-01-31T00:00:00' AND '2023-01-01T00:00:00'
    LIMIT 100
)

SELECT * FROM chicago_taxi