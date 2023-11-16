"""
External Feature Store Service
"""
from typing import List, Optional, cast

import os
from dataclasses import dataclass

import pandas as pd
from databricks import sql as databricks_sql
from databricks_cli.clusters.api import ClusterApi
from databricks_cli.jobs.api import JobsApi
from databricks_cli.runs.api import RunsApi
from databricks_cli.sdk.api_client import ApiClient

from featurebyte.exception import (
    InvalidFeatureStoreArgumentError,
    InvalidFeatureStoreClusterNameError,
    InvalidFeatureStoreJobError,
)
from featurebyte.logging import get_logger

logger = get_logger(__name__)


@dataclass
class ExternalDatabricksFeatureStoreService:
    """
    ExternalDatabricksFeatureStoreService class
    """

    api_client = ApiClient(
        host=f"https://{os.environ['DATABRICKS_SERVER_HOSTNAME']}",
        token=os.environ["DATABRICKS_ACCESS_TOKEN"],
    )

    jobs_api = JobsApi(api_client)
    runs_api = RunsApi(api_client)
    clusters_api = ClusterApi(api_client)

    dbfs_prefix = "dbfs:/FileStore/featurebyte_scripts"

    def create_feature_table(
        self,
        table_name: str,
        primary_keys: List[str],
        column_names: List[str],
        column_types: Optional[List[str]] = None,
        timestamp_keys: Optional[List[str]] = None,
        feature_sql: Optional[str] = None,
    ) -> str:
        """
        Create a table in the external feature store

        Parameters
        ----------
        table_name: str
            fully-qualified name of table to create
        primary_keys: List[str]
            list of primary keys
        column_names: List[str]
            columns of table to create
        column_types: Optional[List[str]
            schema of table to create
        timestamp_keys: Optional[List[str]]
            list of timestamp keys
        feature_sql: Optional[str]
            feature sql query to create table

        Raises
        ------
        InvalidFeatureStoreArgumentError
            if neither column_types nor feature_sql is provided
        InvalidFeatureStoreJobError
            if job is currently running

        Returns
        -------
        str
            job id
        """
        if not column_types and not feature_sql:
            raise InvalidFeatureStoreArgumentError(
                "Either feature_column_types or sql must be provided"
            )

        create_job_file = os.getenv(
            "DATABRICKS_CREATE_TABLE_JOB_FILE",
            f"{self.dbfs_prefix}/create_table_fs.py",
        )
        logger.info(f"create_job_file: {create_job_file}")

        job_name = f"{table_name.replace('.', '_')}_CREATE"
        job_id = self._get_job_id(job_name=job_name, job_file_path=create_job_file)
        logger.info(f"job_id: {job_id}")

        if self.is_job_running(job_id=job_id):
            raise InvalidFeatureStoreJobError(f"Job {job_name} is currently running")

        primary_keys_str = ",".join(primary_keys) if primary_keys else ""
        timestamp_keys_str = ",".join(timestamp_keys) if timestamp_keys else ""

        column_names_str = ",".join(column_names) if column_names else ""
        column_types_str = ",".join(column_types) if column_types else ""
        feature_sql = feature_sql if feature_sql else ""

        job_params = [
            table_name.strip(),
            primary_keys_str,
            timestamp_keys_str,
            column_names_str,
            column_types_str,
            feature_sql,
        ]

        job_result = self.jobs_api.run_now(
            job_id=job_id,
            notebook_params=None,
            jar_params=None,
            python_params=job_params,
            spark_submit_params=None,
        )

        logger.info(f"job_result: {job_result}")
        return job_id

    def write_feature_table(
        self,
        table_name: str,
        feature_sql: str,
        primary_keys: List[str],
        feature_column_names: List[str],
        feature_column_types: List[str],
    ) -> str:
        """
        Write a table to the external feature store

        Parameters
        ----------
        table_name: str
            fully-qualified name of table to write
        feature_sql: str
            sql query to write to table
        primary_keys: List[str]
            list of primary keys
        feature_column_names: List[str]
            list of feature column names
        feature_column_types: List[str]
            list of feature column types

        Raises
        ------
        InvalidFeatureStoreJobError
            if job is currently running

        Returns
        -------
        str
            job id
        """
        write_job_file = os.getenv(
            "DATABRICKS_WRITE_TABLE_JOB_FILE",
            f"{self.dbfs_prefix}/write_table_fs.py",
        )
        logger.info(f"write_job_file: {write_job_file}")

        job_name = f"{table_name.replace('.', '_')}_WRITE"
        job_id = self._get_job_id(job_name=job_name, job_file_path=write_job_file)
        logger.info(f"job_id: {job_id}")

        if self.is_job_running(job_id=job_id):
            raise InvalidFeatureStoreJobError(f"Job {job_name} is currently running")

        primary_keys_str = ",".join(primary_keys) if primary_keys else ""
        feature_column_names_str = ",".join(feature_column_names) if feature_column_names else ""
        feature_column_types_str = ",".join(feature_column_types) if feature_column_types else ""

        job_params = [
            table_name.strip(),
            feature_sql,
            primary_keys_str,
            feature_column_names_str,
            feature_column_types_str,
        ]

        job_result = self.jobs_api.run_now(
            job_id=job_id,
            notebook_params=None,
            jar_params=None,
            python_params=job_params,
            spark_submit_params=None,
        )

        logger.info(f"job_result: {job_result}")
        return job_id

    def read_feature_table(
        self, table_name: Optional[str], sql: Optional[str] = None, limit: int = 100
    ) -> pd.DataFrame:
        """
        Read a table from the external feature store

        Parameters
        ----------
        table_name: str
            fully-qualified name of table to read
        sql: str
            sql query to read table
        limit: int
            limit of rows to read

        Returns
        -------
        pd.DataFrame

        Raises
        ------
        InvalidFeatureStoreArgumentError
            if neither table_name nor sql is provided
        """
        connection = databricks_sql.connect(
            server_hostname=os.environ["DATABRICKS_SERVER_HOSTNAME"],
            http_path=os.environ["DATABRICKS_UNITY_HTTP_PATH"],
            access_token=os.environ["DATABRICKS_ACCESS_TOKEN"],
        )

        with connection.cursor() as cursor:
            if table_name:
                cursor.execute(f"select * from {table_name} limit {limit}")
            elif sql:
                cursor.execute(f"{sql} limit {limit}")
            else:
                raise InvalidFeatureStoreArgumentError("Either name or sql must be provided")

            column_names = [desc[0] for desc in cursor.description]
            data = cursor.fetchall()
            dataframe = pd.DataFrame(data, columns=column_names)

            return dataframe

    def _get_cluster_id(self) -> str:
        """
        Get the cluster id for the cluster name

        Returns
        -------
        str
            cluster id

        Raises
        -------
        InvalidFeatureStoreClusterNameError
            if cluster name is not found
        """
        cluster_name = os.getenv("DATABRICKS_UNITY_CLUSTER", "Unity Cluster")

        cluster_id = None
        for cluster_info in self.clusters_api.list_clusters()["clusters"]:
            if cluster_info["cluster_name"] == cluster_name:
                cluster_id = cluster_info["cluster_id"]

        if not cluster_id:
            raise InvalidFeatureStoreClusterNameError(f"Cluster {cluster_name} not found")

        return cast(str, cluster_id)

    def _get_job_id(self, job_name: str, job_file_path: str) -> str:
        """
        Get the job id for a job name, or create a new job if it doesn't exist

        Parameters
        ----------
        job_name: str
            name of job
        job_file_path: str
            path to job file

        Returns
        -------
        str
            job id
        """
        result = self.jobs_api._list_jobs_by_name(name=job_name)  # pylint: disable=protected-access

        if len(result) > 0:
            job_id = result[0]["job_id"]
            logger.info(f"existing job_id: {result[0]['job_id']}")
        else:
            cluster_id = self._get_cluster_id()

            job_spec = {
                "name": job_name,
                "tasks": [
                    {
                        "task_key": f"{job_name}_task_1",
                        "existing_cluster_id": cluster_id,
                        "spark_python_task": {
                            "python_file": job_file_path,
                        },
                        "libraries": [
                            {
                                "pypi": {"package": "databricks-feature-engineering"},
                            }
                        ],
                    }
                ],
            }
            result = self.jobs_api.create_job(json=job_spec)
            job_id = result["job_id"]

            logger.info(f"created job_id: {job_id}")

        return cast(str, job_id)

    def is_job_running(self, job_id: str) -> bool:
        """
        Check if a job is currently running

        Parameters
        ----------
        job_id: str
            job id to check

        Returns
        -------
        bool
            True if job is running, False otherwise
        """
        job_runs = self.runs_api.list_runs(
            job_id=job_id, active_only=False, completed_only=False, offset=0, limit=1
        )
        if job_runs.get("runs", None):
            return "result_state" not in job_runs["runs"][0]["state"]

        return False
