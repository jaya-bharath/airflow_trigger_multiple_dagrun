import dataclasses
import unittest
from unittest import mock
from unittest.mock import patch, call, Mock

from airflow import DAG
from airflow.exceptions import AirflowException, AirflowConfigException
from airflow.utils import timezone

from airflow_trigger_multiple_dagrun.models import DagRunParams, BaseDagConf, XcomKeys
from airflow_trigger_multiple_dagrun.operators import TriggerMultipleDagRunOperator


@dataclasses.dataclass
class SampleDagConf(BaseDagConf):
    conf_key_1: str
    conf_key_2: str


def sample_python_callable(run_id):
    return [
        DagRunParams(run_id=f"{run_id}_1", conf=SampleDagConf("value_1", "value_2")),
        DagRunParams(run_id=f"{run_id}_2", conf=SampleDagConf("value_3", "value_4")),
    ]


class TestTriggerMultipleDagRunOperator(unittest.TestCase):
    task_id = "trigger_multiple_dag_run"
    dag_to_trigger = "dag_to_trigger"

    def setUp(self) -> None:
        args = {
            "owner": "airflow",
            "start_date": timezone.datetime(2023, 8, 1),
        }
        self.dag = DAG(dag_id="test_dag", default_args=args)
        self.python_callable = sample_python_callable
        self.task = TriggerMultipleDagRunOperator(
            task_id=self.task_id,
            trigger_dag_id=self.dag_to_trigger,
            default_conf={"default_version": "0.0.1"},
            python_callable=sample_python_callable,
            callable_kwargs={"run_id": "dag_run"},
            dag=self.dag,
        )
        self.mock_task_instance = mock.Mock()
        self.context = {"task_instance": self.mock_task_instance}
        self.trigger_dag_patcher = patch(
            "airflow_trigger_multiple_dagrun.operators.trigger_dag",
            Mock(return_value=Mock(run_id="dag_run")),
        )
        self.trigger_dag_patcher.start()
        super().setUp()

    def tearDown(self) -> None:
        self.trigger_dag_patcher.stop()
        super().tearDown()

    def test_execute_should_run_without_errors(self):
        self.task.execute(self.context)

    def test_operator_should_push_xcom_values(self):
        with mock.patch.object(
            self.mock_task_instance, "xcom_push", return_value=None
        ) as mock_xcom_push:
            self.task.execute(self.context)
            mock_xcom_push.assert_has_calls(
                [
                    call(key=XcomKeys.TRIGGERED_DAG_ID, value=self.task.trigger_dag_id),
                    call(key=XcomKeys.TRIGGERED_RUN_IDS, value=["dag_run", "dag_run"]),
                ]
            )

    def test_constructor_should_fail_with_invalid_callable(self):
        with self.assertRaises(AirflowException):
            _ = TriggerMultipleDagRunOperator(
                task_id="callable_validation",
                trigger_dag_id=self.dag_to_trigger,
                default_conf={"default_version": "0.0.1"},
                python_callable="<supposed_to_be_a_callable>",
                callable_kwargs={"run_id": "dag_run"},
                dag=self.dag,
            )

    def test_constructor_should_fail_with_invalid_default_conf(self):
        with self.assertRaises(AirflowConfigException):
            _ = TriggerMultipleDagRunOperator(
                task_id="default_conf_failure",
                trigger_dag_id=self.dag_to_trigger,
                default_conf="<supposed_to_be_mapping>",
                python_callable=sample_python_callable,
                callable_kwargs={"run_id": "dag_run"},
                dag=self.dag,
            )

    def test_failure_when_callable_returns_empty_dagrun_params(self):
        with mock.patch.object(self.task, "python_callable", new=lambda run_id: []):
            with self.assertRaises(RuntimeError):
                self.task.execute(self.context)

    def test_failure_when_trigger_dag_returns_none_or_exception(self):
        with patch(
            "airflow_trigger_multiple_dagrun.operators.trigger_dag"
        ) as mock_dag_run:
            mock_dag_run.side_effect = [AirflowException(), None]
            with self.assertRaises(AirflowException):
                self.task.execute(self.context)
            with self.assertRaises(RuntimeError):
                self.task.execute(self.context)


if __name__ == "__main__":
    unittest.main()
