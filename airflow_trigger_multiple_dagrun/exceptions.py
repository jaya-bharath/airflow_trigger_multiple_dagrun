"""Custom exceptions for airflow_trigger_multiple_dagrun plugin."""
from airflow.exceptions import AirflowException


class NoDagRunsToMonitor(AirflowException):
    """No DagRuns to monitor by the sensor"""
