import unittest
from unittest import mock

from airflow import DAG
from airflow.exceptions import AirflowFailException, AirflowException
from airflow.sensors.base import PokeReturnValue
from airflow.utils import timezone
from airflow.utils.state import DagRunState

import airflow_trigger_multiple_dagrun
from airflow_trigger_multiple_dagrun.exceptions import NoDagRunsToMonitor
from airflow_trigger_multiple_dagrun.sensors import MultiDagRunSensor


# pylint: disable=too-many-instance-attributes
class TestMultiDagRunSensor(unittest.TestCase):
    def setUp(self) -> None:
        args = {
            "owner": "airflow",
            "start_date": timezone.datetime(2023, 8, 1),
        }
        self.dag = DAG(dag_id="test_dag", default_args=args)
        self.monitor_dag_id = "monitor_dag"
        self.monitor_dag_runs = ["run_1", "run_2"]
        self.sensor = MultiDagRunSensor(
            task_id="dag_run_sensor",
            dag=self.dag,
            monitor_dag_id=self.monitor_dag_id,
            monitor_run_ids=self.monitor_dag_runs,
            poke_interval=300,
            mode="reschedule",
        )
        self.patch_base_sensor = mock.patch.object(
            airflow_trigger_multiple_dagrun.sensors.BaseSensorOperator,
            "execute",
            return_value=True,
        )
        self.mock_base_sensor_execute = self.patch_base_sensor.start()

        self.patch_dag_run = mock.patch.object(
            airflow_trigger_multiple_dagrun.sensors.DagRun,
            "find",
            return_value=[
                mock.Mock(state=DagRunState.SUCCESS),
                mock.Mock(state=DagRunState.SUCCESS),
            ],
        )
        self.mock_dag_run_find = self.patch_dag_run.start()
        super().setUp()

    def tearDown(self) -> None:
        self.patch_base_sensor.stop()
        self.patch_dag_run.stop()
        super().tearDown()

    def test_execute(self):
        self.sensor.execute({})

    def test_execute_should_fail_for_invalid_input_args(self):
        with mock.patch.object(self.sensor, "poke_interval", new="<not_a_number>"):
            with self.assertRaises(AirflowException):
                self.sensor.execute({})
        with mock.patch.object(self.sensor, "monitor_run_ids", new=[]):
            with self.assertRaises(AirflowException):
                self.sensor.execute({})

    def test_task_should_fail_when_returned_xcom_is_false(self):
        self.mock_base_sensor_execute.return_value = False
        with self.assertRaises(AirflowFailException):
            self.sensor.execute({})

    def test_poke_should_return_true_when_all_the_runs_are_in_success_states(self):
        result: PokeReturnValue = self.sensor.poke({})
        assert result.is_done is True
        assert result.xcom_value is True

    def test_poke_should_return_false_when_all_the_runs_are_not_in_finished_states(
            self,
    ):
        self.mock_dag_run_find.return_value = [
            mock.Mock(state=DagRunState.SUCCESS),
            mock.Mock(state=DagRunState.RUNNING),
        ]
        result: PokeReturnValue = self.sensor.poke({})
        assert result.is_done is False
        assert result.xcom_value is None

    def test_poke_should_return_xcom_as_false_when_any_of_the_runs_are_in_failed_state(
            self,
    ):
        self.mock_dag_run_find.return_value = [
            mock.Mock(state=DagRunState.SUCCESS),
            mock.Mock(state=DagRunState.FAILED),
        ]
        result: PokeReturnValue = self.sensor.poke({})
        assert result.is_done is True
        assert result.xcom_value is False

    def test_sensor_should_fail_when_no_dag_runs_found(self):
        with self.assertRaises(NoDagRunsToMonitor):
            self.mock_dag_run_find.return_value = []
            self.sensor.poke({})

    def test_apply_func(self):
        iterable = ["running", "failed"]

        actual = MultiDagRunSensor.apply_func(
            lambda state: state == "running", iterable
        )

        assert actual == [True, False]


if __name__ == "__main__":
    unittest.TestCase()
