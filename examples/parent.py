from dataclasses import dataclass

import pendulum
from airflow import DAG
from airflow_trigger_multiple_dagrun.models import XcomKeys, DagRunParams, BaseDagConf
from airflow_trigger_multiple_dagrun.operators import TriggerMultipleDagRunOperator
from airflow_trigger_multiple_dagrun.sensors import MultiDagRunSensor


@dataclass
class ChildDagConf(BaseDagConf):
    conf1: str
    conf2: str


def callable_returning_dag_run_params(run_id: str):
    return [
        DagRunParams(run_id, ChildDagConf("run_1", "run_1")),
        DagRunParams(run_id, ChildDagConf("run_2", "run_2")),
    ]


with DAG(
        dag_id="parent_dag",
        catchup=False,
        start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
        schedule="@daily",
        tags=["trigger_multiple_dag_run"],
        render_template_as_native_obj=True,
) as dag1:
    trigger_task = TriggerMultipleDagRunOperator(
        task_id="trigger",
        trigger_dag_id="child_dag",
        python_callable=callable_returning_dag_run_params,
        callable_kwargs={"run_id": "run_by_parent"},
        default_conf={"default": "defaults_for_all_run"},
    )

    monitor_task = MultiDagRunSensor(
        monitor_dag_id=f"{{{{ ti.xcom_pull(task_ids='trigger',key='{XcomKeys.TRIGGERED_DAG_ID}') }}}}",
        monitor_run_ids=f"{{{{ ti.xcom_pull(task_ids='trigger',key='{XcomKeys.TRIGGERED_RUN_IDS}') }}}}",
        task_id="sensor",
        poke_interval=10,
        mode="reschedule",
    )

    trigger_task >> monitor_task
