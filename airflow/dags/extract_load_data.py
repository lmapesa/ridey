from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import requests
import csv
import os
import json
import snowflake.connector


# Define the default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1
}


# Define the DAG
dag = DAG(
    'chicago_data_dag',
    default_args=default_args,
    description='DAG to extract Chicago taxi data from API, load to local storage, and then load to Snowflake',
    schedule_interval=None
)


# Define the first task to extract data from API to local storage
def extract_data():
    # Define the API endpoint and parameters
    url = "https://data.cityofchicago.org/resource/wrvz-psew.csv"
    params = {
        '$select': '*',
        '$where': 'trip_start_timestamp >= "2022-01-01T00:00:00"',
        '$order': 'trip_start_timestamp ASC',
        '$limit': 5
    }

    # Make a GET request to the API endpoint with the specified parameters
    response = requests.get(url, params=params)

    # Parse the response as CSV and store it in a list of dictionaries
    data = list(csv.DictReader(response.text.splitlines()))

    # Open a new CSV file in write mode
    with open('chicago_data.csv', mode='w', newline='') as file:
        # Create a writer object
        writer = csv.writer(file)

        # Write the header row
        writer.writerow(data[0].keys())

        # Write each row of data to the CSV file
        for row in data:
            writer.writerow(row.values())

    print('Data has been extracted to local storage')


# Define the second task to load data to Snowflake stage
def load_to_stage():
    csv_file_path = os.path.join('data', 'chicago_data.csv')

    # Load Snowflake configuration from file
    with open('config/snowflake_config.json') as f:
        snowflake_config = json.load(f)

    # Create connection to Snowflake
    conn = snowflake.connector.connect(
        account=snowflake_config['account'],
        user=snowflake_config['user'],
        password=snowflake_config['password'],
        database=snowflake_config['database'],
        warehouse=snowflake_config['warehouse'],
        schema=snowflake_config['schema']
    )

    # Create a stage for the CSV data
    stage_name = 'tmp_stage'
    create_stage_query = f"CREATE STAGE IF NOT EXISTS {stage_name}"
    conn.cursor().execute(create_stage_query)

    # Upload the CSV file to the stage
    put_query = f"PUT file://{csv_file_path} @{stage_name}/"
    conn.cursor().execute(put_query)

    # Close Snowflake connection
    conn.close()

    print('Data has been loaded to Snowflake stage')


# Define the third task to load data to Snowflake table
def load_to_table():
    # Load Snowflake configuration from file
    with open('config/snowflake_config.json') as f:
        snowflake_config = json.load(f)

try:
    # Connect to Snowflake
    conn = snowflake.connector.connect(
        account=snowflake_config['account'],
        user=snowflake_config['user'],
        password=snowflake_config['password'],
        database=snowflake_config['database'],
        warehouse=snowflake_config['warehouse'],
        schema=snowflake_config['schema']
    )

    # Create schema if not exists
    cursor = conn.cursor()
    cursor.execute("""
    CREATE SCHEMA IF NOT EXISTS dbt_raw_data
    """)

    # Drop table if exists
    cursor.execute("""
    DROP TABLE IF EXISTS dbt_raw_data.chicago_data
    """)

    # Create table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS dbt_raw_data.chicago_data (
        trip_id VARCHAR(200) PRIMARY KEY,
        taxi_id VARCHAR(200),
        trip_start_timestamp TIMESTAMP,
        trip_end_timestamp TIMESTAMP,
        trip_seconds INTEGER,
        trip_miles FLOAT,
        pickup_census_tract TEXT,
        dropoff_census_tract TEXT,
        pickup_community_area INTEGER,
        dropoff_community_area INTEGER,
        fare FLOAT,
        tips FLOAT,
        tolls FLOAT,
        extras FLOAT,
        trip_total FLOAT,
        payment_type TEXT,
        company VARCHAR,
        pickup_centroid_latitude FLOAT,
        pickup_centroid_longitude FLOAT,
        pickup_centroid_location VARCHAR(100),
        dropoff_centroid_latitude FLOAT,
        dropoff_centroid_longitude FLOAT,
        dropoff_centroid_location VARCHAR(100)
    );
    """)

    # Copy data from stage to table
    cursor.execute("""
    COPY INTO dbt_raw_data.chicago_data
    FROM @my_stage/chicago_data.csv
    FILE_FORMAT = (TYPE = 'CSV', FIELD_DELIMITER = ',', NULL_IF = '')
    ON_ERROR = 'CONTINUE'
    """)

    conn.commit()

    print("Data has been ingested into the Snowflake warehouse successfully!")

except Exception as e:
    print("Error:", e)

finally:
    # Close Snowflake connection
    if 'conn' in locals() and conn:
        conn.close()

t1 = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    dag=dag,
)

t2 = PythonOperator(
    task_id='load_to_stage',
    python_callable=load_to_stage,
    dag=dag,
)

t3 = PythonOperator(
    task_id='load_to_table',
    python_callable=load_to_table,
    dag=dag,
)

t1 >> t2 >> t3