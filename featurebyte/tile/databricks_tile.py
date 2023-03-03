"""
Databricks Tile class
"""
from typing import Any, Optional

import asyncio

from databricks_cli.jobs.api import JobsApi
from databricks_cli.runs.api import RunsApi
from databricks_cli.sdk import ApiClient
from pydantic import PrivateAttr

from featurebyte.enum import InternalName
from featurebyte.models.tile import TileSpec, TileType
from featurebyte.session.base import BaseSession
from featurebyte.session.databricks import DatabricksSession
from featurebyte.tile.base import BaseTileManager


class TileManagerDatabricks(BaseTileManager):
    """
    Databricks Tile class
    """

    _session: DatabricksSession = PrivateAttr()
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
        super().__init__(session=session, **kw)

        api_client = ApiClient(
            host=f"https://{self._session.server_hostname}", token=self._session.access_token
        )

        self._jobs_api = JobsApi(api_client)
        self._runs_api = RunsApi(api_client)

    async def tile_job_exists(self, tile_spec: TileSpec) -> bool:
        """
        Get existing tile jobs for the given tile_spec

        Parameters
        ----------
        tile_spec: TileSpec
            the input TileSpec

        Returns
        -------
            whether the tile jobs already exist
        """
        # TODO: implement this
        return True

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

        result = self._jobs_api._list_jobs_by_name(name=job_name)  # pylint: disable=W0212
        job_id = result[0]["job_id"]

        job_params = [
            self._session.featurebyte_schema,
            InternalName.TILE_LAST_START_DATE.value,
            ",".join(tile_spec.entity_column_names),
            tile_spec.aggregation_id,
            temp_entity_table,
        ]

        job_run = self._jobs_api.run_now(
            job_id=job_id,
            notebook_params=None,
            jar_params=None,
            python_params=job_params,
            spark_submit_params=None,
        )
        await self._wait_for_job_completion(job_run)

        return str(job_run)

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
        result = self._jobs_api._list_jobs_by_name(name=job_name)  # pylint: disable=W0212
        job_id = result[0]["job_id"]

        if start_ts_str and end_ts_str:
            tile_sql = tile_spec.tile_sql.replace(
                InternalName.TILE_START_DATE_SQL_PLACEHOLDER, f"'{start_ts_str}'"
            ).replace(InternalName.TILE_END_DATE_SQL_PLACEHOLDER, f"'{end_ts_str}'")
        else:
            tile_sql = tile_spec.tile_sql

        job_params = [
            self._session.featurebyte_schema,
            tile_sql,
            InternalName.TILE_START_DATE.value,
            InternalName.TILE_LAST_START_DATE.value,
            tile_spec.time_modulo_frequency_second,
            tile_spec.blind_spot_second,
            tile_spec.frequency_minute,
            ",".join(tile_spec.entity_column_names),
            ",".join(tile_spec.value_column_names),
            tile_spec.tile_id,
            tile_type.value,
            last_tile_start_ts_str or "",
        ]

        job_run = self._jobs_api.run_now(
            job_id=job_id,
            notebook_params=None,
            jar_params=None,
            python_params=job_params,
            spark_submit_params=None,
        )
        await self._wait_for_job_completion(job_run)

        return str(job_run)

    async def _wait_for_job_completion(self, job_run: Any, max_wait_seconds: int = 15) -> None:
        """
        Wait for Stored Proc(Job) to finish

        Parameters
        ----------
        job_run: Any
            job run details

        max_wait_seconds: int
            max seconds to wait
        """
        for _ in range(max_wait_seconds):
            await asyncio.sleep(1)
            run_details = self._runs_api.get_run(job_run["run_id"])
            if run_details["state"]["life_cycle_state"] == "TERMINATED":
                break
