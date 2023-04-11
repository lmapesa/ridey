### Creative Fabrica Analytics Engineer Task

# Deliverabales
1. Extraction of data from https://data.cityofchicago.org/Transportation/Taxi-Trips/wrvz-psew/data.
2. Design a new database to hold the data
3. Load the Chicago Taxi data to the new database
4. Clean and transform the data
5. Store the transformed data
6. Attach a BI Tool to the data warehouse for self-service analytics
7. Ensure a performant, reliable, scalable, and transparent data pipeline



# Tools used
1. DBT - For data modeling and transformations
2. Airflow - For orchastrating DBT tasks
3. Docker - Containerization to ship app
4. Metabase - Visualization 
5. Snowflake - Data warehouse


# How to run the project
1. Go to the project root directory
2. Run command: docker-compose up
3. Open http://localhost:8080
4. Log in using
    username: airflow
    password: airflow
5. Go to Admin Tab -> Variables
6. Add new variables
    dbt_user: Username in CTO Documentation (Chapter 7: Running the project)
    dbt_password: Passowrd in CTO Documentation (Chapter 7: Running the project)
7. Go to DAGs 
8. Unpause and activate
9. Check views and tables in snowflake

# Check the views and Tables in Snowflake
Username and password in CTO Documentation (Chapter 7: Running the project)
