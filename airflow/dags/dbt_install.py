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

with DAG('2_install_dbt_packages', default_args=default_args, schedule_interval='@once') as dag:
    task_1 = BashOperator(
        task_id='2_install_dbt_packages',
        bash_command='cd /dbt && dbt deps --profiles-dir .',
        env={
            'dbt_user': '{{ var.value.dbt_user }}',
            'dbt_password': '{{ var.value.dbt_password }}',
            **os.environ
        },
        dag=dag
    )

task_1 