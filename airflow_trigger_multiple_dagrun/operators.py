from __future__ import annotations

from typing import Sequence, Any, Callable, List, Mapping

from airflow.api.common.trigger_dag import trigger_dag
from airflow.exceptions import AirflowConfigException
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator, TaskInstance
from airflow.utils import timezone
from airflow.utils.context import Context

from airflow_trigger_multiple_dagrun.models import DagRunParams, XcomKeys


class TriggerMultipleDagRunOperator(BaseOperator):
    ui_color = "#d9d9d9"
    template_fields: Sequence[str] = ("callable_kwargs", "trigger_dag_id")

    def __init__(
            self,
            *,
            trigger_dag_id: str,
            python_callable: Callable[..., List[DagRunParams]],
            callable_kwargs: Mapping[str, Any] | None = None,
            default_conf: Mapping[str, Any] | None = None,
            **kwargs,
    ):
        """
        Triggers dag runs for each DagRunParams generated from the python callable and xcom the ids
        :param trigger_dag_id: dag to trigger
        :param python_callable: user defined callable returning list of DagRunParams
        :param callable_kwargs: keywords args used by the callable
        :param default_conf: default conf that are common for all dag runs
        :param kwargs: other args to self and Base class
        """
        super().__init__(**kwargs)
        self.trigger_dag_id = trigger_dag_id
        self.default_conf = default_conf or {}
        self.python_callable = python_callable
        self.callable_kwargs = callable_kwargs or {}
        self._validate_input_values()

    def execute(self, context: Context) -> Any:
        xcom_dag_run_ids = []

        dag_runs: List[DagRunParams] = self.python_callable(**self.callable_kwargs)
        if not dag_runs:
            raise RuntimeError("No dags runs returned from the python_callable")

        for dag_run in dag_runs:
            parsed_execution_date = timezone.utcnow()
            dag_run_id = f"{dag_run.run_id}_{parsed_execution_date.isoformat()}"
            dag_run_conf = dag_run.get_conf_as_dict()

            try:
                dag_run = trigger_dag(
                    dag_id=self.trigger_dag_id,
                    run_id=dag_run_id,
                    conf={**self.default_conf, **dag_run_conf},
                    execution_date=parsed_execution_date,
                    replace_microseconds=False,
                )
            except AirflowException as exc:
                self.log.error(
                    "Failure while trying to trigger dag %s with run_id %s",
                    self.trigger_dag_id,
                    dag_run_id,
                )
                raise exc
            if not dag_run:
                raise RuntimeError(
                    f"dag_run of dag_id `{self.trigger_dag_id}` `{dag_run_id}` did not trigger"
                )
            xcom_dag_run_ids.append(dag_run.run_id)

        if xcom_dag_run_ids:
            task_instance = context["task_instance"]
            self._xcom_push(
                task_instance, XcomKeys.TRIGGERED_DAG_ID, self.trigger_dag_id
            )
            self._xcom_push(task_instance, XcomKeys.TRIGGERED_RUN_IDS, xcom_dag_run_ids)

    def _validate_input_values(self):
        if not callable(self.python_callable):
            raise AirflowException("`python_callable` param must be a callable")
        try:
            {**self.default_conf}
        except TypeError as exc:
            raise AirflowConfigException(
                "`default_conf` parameter should be a dictionary"
            ) from exc

    def _xcom_push(self, task_instance: TaskInstance, key, value):
        task_instance.xcom_push(key=key, value=value)
        self.log.info("xcom push with key: %s value: %s", key, value)
