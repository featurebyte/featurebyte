"""
Databricks Tile class
"""
from typing import Any, Optional, cast

import time

from databricks_cli.jobs.api import JobsApi
from databricks_cli.runs.api import RunsApi
from databricks_cli.sdk import ApiClient
from pydantic import BaseModel, PrivateAttr

from featurebyte.enum import InternalName
from featurebyte.logger import logger
from featurebyte.models.tile import TileSpec, TileType
from featurebyte.session.base import BaseSession
from featurebyte.session.databricks import DatabricksSession


class TileManagerDatabricks(BaseModel):
    """
    Databricks Tile class
    """

    _session: BaseSession = PrivateAttr()
    _jobs_api: JobsApi = PrivateAttr()
    _runs_api: RunsApi = PrivateAttr()

    def __init__(self, session: BaseSession, **kw: Any) -> None:
        """
        Custom constructor for TileManagerDatabricks to instantiate a datasource session

        Parameters
        ----------
        session: BaseSession
            input session for datasource
        kw: Any
            constructor arguments
        """
        super().__init__(**kw)
        self._session: DatabricksSession = cast(DatabricksSession, session)

        api_client = ApiClient(
            host=f"https://{self._session.server_hostname}", token=self._session.access_token
        )

        self._jobs_api = JobsApi(api_client)
        self._runs_api = RunsApi(api_client)

    async def update_tile_entity_tracker(self, tile_spec: TileSpec, temp_entity_table: str) -> str:
        """
        Update <tile_id>_entity_tracker table for last_tile_start_date

        Parameters
        ----------
        tile_spec: TileSpec
            the input TileSpec
        temp_entity_table: str
            temporary entity table to be merge into <tile_id>_entity_tracker

        Returns
        -------
            databricks job run detail
        """
        job_name = "tile_generate_entity_tracking"

        result = self._jobs_api._list_jobs_by_name(name=job_name)
        job_id = result[0]["job_id"]
        job_params = {
            "FEATUREBYTE_DATABASE": self._session.featurebyte_schema,
            "TILE_ID": tile_spec.aggregation_id,
            "ENTITY_COLUMN_NAMES": ",".join(tile_spec.entity_column_names),
            "ENTITY_TABLE": temp_entity_table,
            "TILE_LAST_START_DATE_COLUMN": InternalName.TILE_LAST_START_DATE.value,
        }

        job_run = self._jobs_api.run_now(
            job_id=job_id,
            notebook_params=job_params,
            jar_params=None,
            python_params=None,
            spark_submit_params=None,
        )
        self._wait_for_job_completion(job_run)

        return job_run

    async def generate_tiles(
        self,
        tile_spec: TileSpec,
        tile_type: TileType,
        start_ts_str: Optional[str],
        end_ts_str: Optional[str],
        last_tile_start_ts_str: Optional[str] = None,
    ) -> str:
        """
        Manually trigger tile generation

        Parameters
        ----------
        tile_spec: TileSpec
            the input TileSpec
        tile_type: TileType
            tile type. ONLINE or OFFLINE
        start_ts_str: str
            start_timestamp of tile. ie. 2022-06-20 15:00:00
        end_ts_str: str
            end_timestamp of tile. ie. 2022-06-21 15:00:00
        last_tile_start_ts_str: str
            start date string of last tile used to update the tile_registry table

        Returns
        -------
            databricks job run details
        """
        job_name = "tile_generate"
        result = self._jobs_api._list_jobs_by_name(name=job_name)
        job_id = result[0]["job_id"]

        if start_ts_str and end_ts_str:
            tile_sql = tile_spec.tile_sql.replace(
                InternalName.TILE_START_DATE_SQL_PLACEHOLDER, f"'{start_ts_str}'"
            ).replace(InternalName.TILE_END_DATE_SQL_PLACEHOLDER, f"'{end_ts_str}'")
        else:
            tile_sql = tile_spec.tile_sql

        logger.debug(f"tile_sql: {tile_sql}")

        job_params = {
            "FEATUREBYTE_DATABASE": self._session.featurebyte_schema,
            "SQL": tile_sql,
            "TILE_ID": tile_spec.tile_id,
            "TILE_TYPE": tile_type.value,
            "TILE_START_DATE_COLUMN": InternalName.TILE_START_DATE.value,
            "TILE_LAST_START_DATE_COLUMN": InternalName.TILE_LAST_START_DATE.value,
            "TIME_MODULO_FREQUENCY_SECOND": tile_spec.time_modulo_frequency_second,
            "BLIND_SPOT_SECOND": tile_spec.blind_spot_second,
            "FREQUENCY_MINUTE": tile_spec.frequency_minute,
            "ENTITY_COLUMN_NAMES": ",".join(tile_spec.entity_column_names),
            "VALUE_COLUMN_NAMES": ",".join(tile_spec.value_column_names),
            "LAST_TILE_START_STR": last_tile_start_ts_str or "",
        }

        job_run = self._jobs_api.run_now(
            job_id=job_id,
            notebook_params=job_params,
            jar_params=None,
            python_params=None,
            spark_submit_params=None,
        )
        self._wait_for_job_completion(job_run)

        return job_run

    def _wait_for_job_completion(self, job_run: Any, max_wait_seconds: int = 15) -> None:
        """
        Wait for Stored Proc(Job) to finish

        Parameters
        ----------
        job_run: Any
            job run details

        max_wait_seconds:
            max seconds to wait
        """
        for i in range(max_wait_seconds):
            time.sleep(1)
            run_details = self._runs_api.get_run(job_run["run_id"])
            if run_details["state"]["life_cycle_state"] == "TERMINATED":
                break
