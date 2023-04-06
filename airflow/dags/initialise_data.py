from airflow import DAG, macros
from airflow.operators.bash_operator import BashOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.dates import days_ago
from datetime import datetime


# [START default_args]
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1
}
# [END default_args]

# [START instantiate_dag]
load_initial_data_dag = DAG(
    '1_load_initial_data',
    default_args=default_args,
    schedule_interval = None,
)

t1 = PostgresOperator(task_id='create_schema',
                      sql="CREATE SCHEMA IF NOT EXISTS dbt_raw_data;",
                      postgres_conn_id='dbt_postgres_instance_raw_data',
                      autocommit=True,
                      database="dbtdb",
                      dag=load_initial_data_dag)

t2 = PostgresOperator(task_id='drop_table_chicago_data',
                      sql="DROP TABLE IF EXISTS chicago_data;",
                      postgres_conn_id='dbt_postgres_instance_raw_data',
                      autocommit=True,
                      database="dbtdb",
                      dag=load_initial_data_dag)

t3 = PostgresOperator(task_id='create_chicago_data',
                      sql="create table if not exists dbt_raw_data.chicago_data (trip_id VARCHAR(200) PRIMARY KEY,taxi_id VARCHAR(200),trip_start_timestamp TIMESTAMP, trip_end_timestamp TIMESTAMP, trip_seconds INTEGER, trip_miles FLOAT, pickup_census_tract TEXT, dropoff_census_tract TEXT, pickup_community_area INTEGER, dropoff_community_area INTEGER, fare FLOAT, tips FLOAT,tolls FLOAT, extras FLOAT, trip_total FLOAT,payment_type TEXT, company VARCHAR,pickup_centroid_latitude FLOAT,pickup_centroid_longitude FLOAT,pickup_centroid_location VARCHAR(100), dropoff_centroid_latitude FLOAT, dropoff_centroid_longitude FLOAT, dropoff_centroid_location VARCHAR(100));",
                      postgres_conn_id='dbt_postgres_instance_raw_data',
                      autocommit=True,
                      database="dbtdb",
                      dag=load_initial_data_dag)

t4 = PostgresOperator(task_id='load_chicago_data',
                      sql="COPY dbt_raw_data.chicago_data FROM '/sample_data/chicago_data.csv' DELIMITER ',' CSV HEADER;",
                      postgres_conn_id='dbt_postgres_instance_raw_data',
                      autocommit=True,
                      database="dbtdb",
                      dag=load_initial_data_dag)

  

t1 >> t2 >> t3 >> t4