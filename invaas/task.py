# Abstract task class for use in workflow processes

import logging
import os
import sys
import warnings


from abc import ABC
from dotenv import load_dotenv, find_dotenv
from pyspark.sql import SparkSession


warnings.filterwarnings("ignore")


class Task(ABC):
    """
    This is an abstract class that provides handy interfaces to implement workloads.
    Create a child from this class and implement the abstract launch method.
    Class provides access to the following useful objects:
    * self.spark is a SparkSession
    * self.dbutils provides access to the DBUtils
    * self.logger provides access to the Spark-compatible logger
    * self.conf provides access to the parsed configuration of the job
    """

    def __init__(self, env: str = None):
        self.env = env
        self.logger = self.__get_logger()
        self.spark = self.__get_spark()
        self.dbutils = self.__get_dbutils()

    def get_secret(self, key: str):
        if self.env == "local":
            load_dotenv(find_dotenv())
            return os.environ[key]
        else:
            return self.dbutils.secrets.get(scope="kv-invaas", key=key)

    def __get_spark(self, spark: SparkSession = None):
        if not spark:
            return SparkSession.builder.getOrCreate()
        else:
            return spark

    def __get_dbutils(self):
        try:
            from pyspark.dbutils import DBUtils

            if "dbutils" not in locals():
                utils = DBUtils(self.spark)
            else:
                utils = locals().get("dbutils")
        except ImportError:
            utils = None

        if not utils:
            self.logger.warning("No DBUtils defined in the runtime")
        else:
            self.logger.info("DBUtils class initialized")

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
        logging.getLogger("azure.core.pipeline.policies.http_logging_policy").setLevel(logging.WARNING)
        logging.getLogger("mlflow.tracking.fluent").setLevel(logging.WARNING)
        logging.getLogger("py4j.java_gateway").setLevel(logging.WARNING)
        return logging.getLogger(self.__class__.__name__)
