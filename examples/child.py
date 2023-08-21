from time import sleep

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator


def sleep_for(seconds: float):
    print("-" * 50)
    print(f"sleeping for {seconds}")
    sleep(seconds)
    print("-" * 50)


with DAG(
        dag_id="child_dag",
        catchup=False,
        start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
        schedule="@daily",
        tags=["trigger_multiple_dag_run"],
        render_template_as_native_obj=True,
) as dag1:
    sleep_task = PythonOperator(
        task_id="sleep",
        python_callable=sleep_for,
        op_args=[60],
    )
