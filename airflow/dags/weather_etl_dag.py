from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
import fetch_data
from os import path, getcwd

def branch_function(**kwargs):
    result = fetch_data.check_raw_table_empty()
    if result == 1:
        return 'previous_processing_failed_group.run_dbt_models_task'
    else:
        return 'previous_processing_succeeded_group.load_data_to_staging_task'

with DAG(
    "Weather_ETL_DAG",
    default_args={
        "depends_on_past": False,
        "email": ["to_be_implemented@TBD.com"],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 0,
        "retry_delay": timedelta(minutes=5),
    },
    description="Weather data ETL DAG",
    schedule_interval=None,  # Set to None for manual trigger
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example"],
) as dag:

    check_task = BranchPythonOperator(
        task_id='check_raw_table_empty_task',
        python_callable=branch_function,
        provide_context=True,
    )

    with TaskGroup("previous_processing_failed_group") as previous_processing_failed_group:
        run_dbt_models_task = BashOperator(
            task_id='run_dbt_models_task',
            bash_command='cd ' + path.abspath(path.join(getcwd(), "../dbt_project")) + '/ && dbt run',
        )

        empty_raw_table_task = PythonOperator(
            task_id='empty_raw_table_task',
            python_callable=fetch_data.empty_raw_table,
        )

        load_data_to_staging_task = PythonOperator(
            task_id='load_data_to_staging_task',
            python_callable=fetch_data.load_data_to_raw_table,
        )

        run_dbt_models_again_task = BashOperator(
            task_id='run_dbt_models_again_task',
            bash_command='cd ' + path.abspath(path.join(getcwd(), "../dbt_project")) + '/ && dbt run',
        )

        empty_raw_table_again_task = PythonOperator(
            task_id='empty_raw_table_again_task',
            python_callable=fetch_data.empty_raw_table,
        )

        run_dbt_models_task >> empty_raw_table_task >> load_data_to_staging_task >> run_dbt_models_again_task >> empty_raw_table_again_task

    with TaskGroup("previous_processing_succeeded_group") as previous_processing_succeeded_group:
        load_data_to_staging_task = PythonOperator(
            task_id='load_data_to_staging_task',
            python_callable=fetch_data.load_data_to_raw_table,
        )

        run_dbt_models_task = BashOperator(
            task_id='run_dbt_models_task',
            bash_command='cd ' + path.abspath(path.join(getcwd(), "../dbt_project")) + '/ && dbt run',
        )

        empty_raw_table_task = PythonOperator(
            task_id='empty_raw_table_task',
            python_callable=fetch_data.empty_raw_table,
        )

        load_data_to_staging_task >> run_dbt_models_task >> empty_raw_table_task

    check_task >> previous_processing_failed_group
    check_task >> previous_processing_succeeded_group
