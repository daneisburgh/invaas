# Main task class

import logging
import os
import sys
import warnings

import numpy as np

from abc import ABC
from databricks.sdk import WorkspaceClient
from dotenv import load_dotenv, find_dotenv
from pyspark.sql import SparkSession
from typing import Union

warnings.filterwarnings("ignore")
load_dotenv(find_dotenv())


class Task(ABC):
    """
    This is an abstract class that provides handy interfaces to implement workloads.
    Create a child from this class and implement the abstract launch method.
    """

    def __init__(self, env: str):
        self.env = env
        self.spark = None if self.env == "local" else SparkSession.builder.getOrCreate()
        self.logger = self.__get_logger()
        self.dbutils = self.__get_dbutils(self.spark)

        if self.env != "local":
            self.job_id = self.dbutils.widgets.get("job_id")
            self.logger.info(f"Job ID: {self.job_id}")
            self.workspace_client = WorkspaceClient(
                host=f"https://{self.spark.conf.get('spark.databricks.workspaceUrl')}/",
                token=self.get_secret("DATABRICKS-TOKEN"),
            )

        self.logger.info(f"Initializing task for {self.env} environment")

    def __get_dbutils(self, spark: Union[SparkSession, None]):
        try:
            from pyspark.dbutils import DBUtils

            if "dbutils" not in locals():
                utils = DBUtils(spark)
            else:
                utils = locals().get("dbutils")
        except ImportError:
            utils = None

        if not utils:
            self.logger.warning("No DBUtils defined in the runtime")

        return utils

    def __get_logger(self):
        logging.basicConfig(
            force=True,
            level=logging.INFO,
            stream=sys.stdout,
            format="%(asctime)s %(levelname)s %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        logging.getLogger("asyncio.events").setLevel(logging.CRITICAL)
        logging.getLogger("azure.core.pipeline.policies.http_logging_policy").setLevel(logging.ERROR)
        logging.getLogger("mlflow.tracking.fluent").setLevel(logging.ERROR)
        logging.getLogger("py4j").setLevel(logging.ERROR)
        logging.getLogger("py4j.java_gateway").setLevel(logging.ERROR)
        logging.getLogger("pyspark").setLevel(logging.ERROR)
        return logging.getLogger(self.__class__.__name__)

    def get_secret(self, key: str):
        if self.env == "local":
            return os.environ[key]
        else:
            return self.dbutils.secrets.get(scope="kv-invaas", key=key)

    def floor_value(self, value: float, precision: int):
        return np.true_divide(np.floor(value * 10**precision), 10**precision)
