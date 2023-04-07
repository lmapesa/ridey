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
    -- LIMIT 50000
)

SELECT * FROM chicago_taxi