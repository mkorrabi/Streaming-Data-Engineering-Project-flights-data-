from datetime import datetime

from airflow import DAG
from airflow.operators.papermill_operator import PapermillOperator
from airflow.operators.python_operator import PythonOperator

from src.builder_airlines import update_airline_data
from src.builder_airports import update_airport_data

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 10, 1),
    "retries": 0,
}

dag_1 = DAG(
    "combined_dag",
    default_args=default_args,
    schedule_interval="0 */2 * * *",  # Run every 2 hours
    catchup=False,
)

# Add your existing PapermillOperator
notebook_task = PapermillOperator(
    task_id="run_example_notebook",
    input_nb="./notebook_viz.ipynb",
    output_nb="./notebook_viz_out_{{ execution_date }}.ipynb",
    parameters={"execution_date": "{{ execution_date }}"},
    dag=dag_1,
)

notebook_task


dag_2 = DAG(
    "combined_dag",
    default_args=default_args,
    schedule_interval="0 0 * * 0",  # Run every week
    catchup=False,
)

# Add your custom PythonOperator
run_my_function = PythonOperator(
    task_id="run_my_function",
    python_callable=update_airline_data,
    dag=dag_2,
)


# Add your custom PythonOperator
run_my_function = PythonOperator(
    task_id="run_my_function",
    python_callable=update_airport_data,
    dag=dag_2,
)

run_my_function >> run_my_function
