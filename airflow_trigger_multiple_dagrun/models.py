from dataclasses import dataclass, asdict, field
from enum import Enum


@dataclass
class BaseDagConf:
    """common base dag conf for all dag runs"""


@dataclass
class DagRunParams:
    run_id: str
    conf: BaseDagConf = field(default_factory=BaseDagConf)

    def get_conf_as_dict(self):
        """Return conf as a dictionary"""
        return asdict(self.conf)


class XcomKeys(str, Enum):
    """xcom keys to be used in operator and sensor"""

    TRIGGERED_DAG_ID = "triggered_dag_id"
    TRIGGERED_RUN_IDS = "triggered_run_ids"

    def __str__(self):
        return self.value
