from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime
import os

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020,8,1),
    'retries': 0
}

with DAG('dbt_models', default_args=default_args, schedule_interval='@once') as dag:
    init = BashOperator(
        task_id='0_install_dbt_packages',
        bash_command='cd /dbt && dbt deps --profiles-dir .',
        env={
            'dbt_user': '{{ var.value.dbt_user }}',
            'dbt_password': '{{ var.value.dbt_password }}',
            **os.environ
        },
        dag=dag
    )

    sources = BashOperator(
        task_id='1_sources',
        bash_command='cd /dbt && dbt run --no-version-check --models sources --profiles-dir .',
        env={
            'dbt_user': '{{ var.value.dbt_user }}',
            'dbt_password': '{{ var.value.dbt_password }}',
            **os.environ
        },
        dag=dag
    )

    staging = BashOperator(
        task_id='2_staging',
        bash_command='cd /dbt && dbt run --no-version-check --models staging --profiles-dir .',
        env={
            'dbt_user': '{{ var.value.dbt_user }}',
            'dbt_password': '{{ var.value.dbt_password }}',
            **os.environ
        },
        dag=dag
    )

    intermediate = BashOperator(
        task_id='3_intermediate',
        bash_command='cd /dbt && dbt run --no-version-check --models intermediate --profiles-dir .',
        env={
            'dbt_user': '{{ var.value.dbt_user }}',
            'dbt_password': '{{ var.value.dbt_password }}',
            **os.environ
        },
        dag=dag
    )

    marts = BashOperator(
        task_id='4_marts',
        bash_command='cd /dbt && dbt run --no-version-check --models marts --profiles-dir .',
        env={
            'dbt_user': '{{ var.value.dbt_user }}',
            'dbt_password': '{{ var.value.dbt_password }}',
            **os.environ
        },
        dag=dag
    )

    init >> sources >> staging >> intermediate >> marts