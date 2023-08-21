from __future__ import annotations

from collections import Counter
from typing import Sequence, Collection, Any, List

from airflow.exceptions import AirflowException
from airflow.exceptions import AirflowFailException
from airflow.models import DagRun
from airflow.sensors.base import BaseSensorOperator, PokeReturnValue

from airflow.utils.context import Context
from airflow.utils.state import DagRunState

from airflow_trigger_multiple_dagrun.exceptions import NoDagRunsToMonitor

DAG_RUN_COMPLETED_STATES = {DagRunState.SUCCESS, DagRunState.FAILED}


# pylint: disable=too-many-ancestors
class MultiDagRunSensor(BaseSensorOperator):
    ui_color = "#f2f2f2"
    template_fields: Sequence[str] = (
        "monitor_dag_id",
        "monitor_run_ids",
        "poke_interval",
    )

    def __init__(
            self,
            *,
            monitor_dag_id: str,
            monitor_run_ids: Collection[str],
            poke_interval: str | float | None = None,
            **kwargs,
    ):
        """
        # todo:
        :param monitor_dag_id:
        :param monitor_run_ids:
        :param poke_interval:
        :param kwargs:
        """
        super().__init__(**kwargs)
        self.monitor_dag_id = monitor_dag_id
        self.monitor_run_ids = monitor_run_ids
        if poke_interval:
            self.poke_interval = poke_interval

    def poke(self, context: Context) -> bool | PokeReturnValue:

        dag_run_list: List[DagRun] = DagRun.find(
            dag_id=self.monitor_dag_id, run_id=self.monitor_run_ids
        )
        self.log.info(dag_run_list or "empty dag run list")

        if not dag_run_list:
            raise NoDagRunsToMonitor(
                f"Dag Runs {self.monitor_run_ids} not found for dag_id {self.monitor_dag_id}"
            )

        dag_run_statuses = {dag_run.state for dag_run in dag_run_list}

        isin_completed_state = self.apply_func(
            lambda state: state in DAG_RUN_COMPLETED_STATES, dag_run_statuses
        )
        self._log_dag_runs_summary(dag_run_list)

        if all(isin_completed_state):
            xcom_value = all(
                self.apply_func(
                    lambda state: state != DagRunState.FAILED, dag_run_statuses
                )
            )
            return PokeReturnValue(
                is_done=True,
                xcom_value=xcom_value,
            )

        return PokeReturnValue(is_done=False)

    def execute(self, context: Context) -> Any:
        self._validate_input_arguments()
        return_value = super().execute(context)
        if not return_value:
            self.log.error(
                "One or few run_ids have failed for dag %s", self.monitor_dag_id
            )
            raise AirflowFailException(
                "One or few run_ids have failed. Marking task as failed"
            )

    def _validate_input_arguments(self):
        if not isinstance(self.poke_interval, (int, float)) or self.poke_interval < 0:
            raise AirflowException("The poke_interval must be a non-negative number")
        if not self.monitor_run_ids:
            raise AirflowException(
                "`monitor_run_ids` must specify the run_ids to monitor"
            )

    def _log_dag_runs_summary(self, dag_run_list: List[DagRun]) -> None:
        for dag_run in dag_run_list:
            log = (
                self.log.error if dag_run.state == DagRunState.FAILED else self.log.info
            )
            log("Dag_run: '%s' state: '%s'", dag_run.run_id, dag_run.state)

        group_runs_on_state = Counter(dag_run.state for dag_run in dag_run_list)
        self.log.info("DagRun summary: %s", dict(group_runs_on_state))

    @staticmethod
    def apply_func(func, iterable):
        return [func(item) for item in iterable]
