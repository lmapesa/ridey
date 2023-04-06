import os
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    'start_date': datetime(2020,8,1),
    "email": ["levinm16@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "catchup": False
}

with DAG('3_run_dbt_models', default_args=default_args, schedule_interval='@once') as dag:
    src_chicago_taxi = BashOperator(
        task_id='src_chicago_taxi',
        bash_command='cd /dbt && dbt run --models sources.src_chicago_taxi --profiles-dir .',
        env={
            'dbt_user': '{{ var.value.dbt_user }}',
            'dbt_password': '{{ var.value.dbt_password }}',
            **os.environ
        },
        dag=dag
    )


    stg_chicago_taxi = BashOperator(
        task_id='stg_chicago_taxi',
        bash_command='cd /dbt && dbt run --models staging.stg_chicago_taxi --profiles-dir .',
        env={
            'dbt_user': '{{ var.value.dbt_user }}',
            'dbt_password': '{{ var.value.dbt_password }}',
            **os.environ
        },
        dag=dag
    )


src_chicago_taxi >> stg_chicago_taxi