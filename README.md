### Creative Fabrica Analytics Engineer Task

# Deliverabales
1. Design a Billing data mart + diagram
2. Load Data as views in snowflake
3. Scripts for loading data -- N/A as DBT is doing transformations withing the warehouse
4. Data cleaning 
5. EDA with a tool of your choice (BI tool, python, Excel, ..)
6. Answer some business questions i.e
    - What is the average price of the top-5 prescribed products
    - Typical treatment for diagnosis X
    - Charge for products between different providers


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
    dbt_user: dbt_user
    dbt_password: pssd
7. Go to DAGs 
8. Unpause and activate
9. Check views and tables in snowflake