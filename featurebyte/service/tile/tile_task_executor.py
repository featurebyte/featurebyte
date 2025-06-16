"""
Tile Generate Schedule script
"""

import time
import traceback
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import dateutil.parser

from featurebyte.common import date_util
from featurebyte.logging import get_logger
from featurebyte.models.feature_materialize_prerequisite import PrerequisiteTileTaskStatusType
from featurebyte.models.system_metrics import TileTaskMetrics
from featurebyte.models.tile import TileScheduledJobParameters, TileType
from featurebyte.models.tile_job_log import TileJobLogModel
from featurebyte.service.deployed_tile_table import DeployedTileTableService
from featurebyte.service.feature import FeatureService
from featurebyte.service.feature_materialize_sync import FeatureMaterializeSyncService
from featurebyte.service.online_store_compute_query_service import OnlineStoreComputeQueryService
from featurebyte.service.online_store_table_version import OnlineStoreTableVersionService
from featurebyte.service.system_metrics import SystemMetricsService
from featurebyte.service.tile_job_log import TileJobLogService
from featurebyte.service.tile_registry_service import TileRegistryService
from featurebyte.service.warehouse_table_service import WarehouseTableService
from featurebyte.session.base import BaseSession
from featurebyte.sql.tile_common import TileCommon
from featurebyte.sql.tile_generate import TileGenerate

logger = get_logger(__name__)
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


class TileTaskExecutor:
    """
    This implements the steps that are run in the scheduled task. This includes tiles calculation,
    tiles consistency monitoring and online store table updates.
    """

    def __init__(
        self,
        online_store_table_version_service: OnlineStoreTableVersionService,
        online_store_compute_query_service: OnlineStoreComputeQueryService,
        tile_registry_service: TileRegistryService,
        tile_job_log_service: TileJobLogService,
        feature_service: FeatureService,
        feature_materialize_sync_service: FeatureMaterializeSyncService,
        system_metrics_service: SystemMetricsService,
        warehouse_table_service: WarehouseTableService,
        deployed_tile_table_service: DeployedTileTableService,
    ):
        self.online_store_table_version_service = online_store_table_version_service
        self.online_store_compute_query_service = online_store_compute_query_service
        self.tile_registry_service = tile_registry_service
        self.tile_job_log_service = tile_job_log_service
        self.feature_service = feature_service
        self.feature_materialize_sync_service = feature_materialize_sync_service
        self.system_metrics_service = system_metrics_service
        self.warehouse_table_service = warehouse_table_service
        self.deployed_tile_table_service = deployed_tile_table_service

    async def execute(self, session: BaseSession, params: TileScheduledJobParameters) -> None:
        """
        Execute steps in the scheduled task

        Parameters
        ----------
        session: BaseSession
            Session object to be used for executing queries in data warehouse
        params: TileScheduledJobParameters
            Parameters for the scheduled task
        """
        used_job_schedule_ts = params.job_schedule_ts or datetime.utcnow().strftime(DATE_FORMAT)
        final_status: PrerequisiteTileTaskStatusType = "failure"
        try:
            await self._execute(session, params, used_job_schedule_ts)
            final_status = "success"
        finally:
            if params.tile_type.upper() == "ONLINE":
                if params.deployed_tile_table_id is None:
                    # legacy tile job only updates a specific aggregation_id
                    aggregation_ids = [params.aggregation_id]
                else:
                    # deployed tile job updates all aggregation_ids in the deployed tile table
                    aggregation_ids = (
                        await self.deployed_tile_table_service.get_document(
                            params.deployed_tile_table_id
                        )
                    ).aggregation_ids
                await self.feature_materialize_sync_service.update_tile_prerequisite(
                    tile_task_ts=dateutil.parser.isoparse(used_job_schedule_ts),
                    aggregation_ids=aggregation_ids,
                    status=final_status,
                )

    async def _execute(
        self,
        session: BaseSession,
        params: TileScheduledJobParameters,
        job_schedule_ts: str,
    ) -> None:
        candidate_last_tile_end_ts = dateutil.parser.isoparse(job_schedule_ts)

        # derive the correct job schedule ts based on input job schedule ts
        # the input job schedule ts might be between 2 intervals
        corrected_job_ts = self._derive_correct_job_ts(
            candidate_last_tile_end_ts, params.frequency_minute, params.time_modulo_frequency_second
        )
        logger.debug(
            "Tile end ts details",
            extra={
                "corrected_job_ts": corrected_job_ts,
                "candidate_last_tile_end_ts": candidate_last_tile_end_ts,
            },
        )

        tile_end_ts = corrected_job_ts - timedelta(seconds=params.blind_spot_second)
        tile_type = params.tile_type.upper()
        lookback_period = params.frequency_minute * (params.monitor_periods + 1)
        tile_id = params.tile_id.upper()

        if tile_type == "OFFLINE":
            lookback_period = params.offline_period_minute
            tile_end_ts = tile_end_ts - timedelta(minutes=lookback_period)

        tile_start_ts = tile_end_ts - timedelta(minutes=lookback_period)
        tile_start_ts_str = tile_start_ts.strftime(DATE_FORMAT)

        # use the last_tile_start_date from tile registry as tile_start_ts_str if it is earlier than tile_start_ts_str
        tile_model = await self.tile_registry_service.get_tile_model(
            params.tile_id, params.aggregation_id
        )
        if tile_model is not None and tile_model.last_run_metadata_online is not None:
            registry_last_tile_start_ts = tile_model.last_run_metadata_online.tile_end_date
            logger.info(f"Last tile start date from registry - {registry_last_tile_start_ts}")

            if registry_last_tile_start_ts.strftime(DATE_FORMAT) < tile_start_ts.strftime(
                DATE_FORMAT
            ):
                logger.info(
                    f"Use last tile start date from registry - {registry_last_tile_start_ts} instead of {tile_start_ts_str}"
                )
                tile_start_ts_str = registry_last_tile_start_ts.strftime(DATE_FORMAT)

        session_id = f"{tile_id}|{datetime.now()}"

        async def _add_log_entry(
            log_status: str,
            log_message: str,
            formatted_traceback: Optional[str] = None,
            duration: Optional[float] = None,
        ) -> None:
            document = TileJobLogModel(
                tile_id=tile_id,
                aggregation_id=params.aggregation_id,
                tile_type=TileType[tile_type],  # TODO: tile_type to be passed as enum
                session_id=session_id,
                status=log_status,
                message=log_message,
                traceback=formatted_traceback,
                duration=duration,
            )
            await self.tile_job_log_service.create_document(document)

        await _add_log_entry("STARTED", "")

        tile_end_ts_str = tile_end_ts.strftime(DATE_FORMAT)

        logger.info(
            "Tile Schedule information",
            extra={
                "tile_id": tile_id,
                "tile_start_ts_str": tile_start_ts_str,
                "tile_end_ts_str": tile_end_ts_str,
                "tile_type": tile_type,
            },
        )

        # Use deployed tile table as the tile table name if it is provided. Not strictly needed as
        # the tile_id of the params should have been set to the same value.
        if params.deployed_tile_table_id is not None:
            deployed_tile_table_model = await self.deployed_tile_table_service.get_document(
                params.deployed_tile_table_id
            )
            tile_table_name = deployed_tile_table_model.table_name
        else:
            deployed_tile_table_model = None
            tile_table_name = tile_id

        tile_generate_ins = TileGenerate(
            session=session,
            feature_store_id=params.feature_store_id,
            tile_id=tile_table_name,
            deployed_tile_table_id=params.deployed_tile_table_id,
            time_modulo_frequency_second=params.time_modulo_frequency_second,
            blind_spot_second=params.blind_spot_second,
            frequency_minute=params.frequency_minute,
            sql=params.sql,
            tile_compute_query=params.tile_compute_query,
            entity_column_names=params.entity_column_names,
            value_column_names=params.value_column_names,
            value_column_types=params.value_column_types,
            tile_type=params.tile_type,
            tile_start_ts_str=tile_start_ts_str,
            tile_end_ts_str=tile_end_ts_str,
            update_last_run_metadata=True,
            aggregation_id=params.aggregation_id,
            tile_registry_service=self.tile_registry_service,
            warehouse_table_service=self.warehouse_table_service,
            deployed_tile_table_service=self.deployed_tile_table_service,
            system_metrics_service=self.system_metrics_service,
        )

        step_specs: List[Dict[str, Any]] = [
            {
                "name": "tile_generate",
                "trigger": tile_generate_ins,
                "status": {
                    "fail": "GENERATED_FAILED",
                    "success": "GENERATED",
                },
                "metric_field_name": "tile_compute_seconds",
            },
        ]

        metrics_data = {}
        for spec in step_specs:
            try:
                logger.info(f"Calling {spec['name']}")
                tile_ins: TileCommon = spec["trigger"]
                tic = time.time()
                await tile_ins.execute()
                duration = time.time() - tic
                metrics_data[spec["metric_field_name"]] = duration
                logger.info(f"End of calling {spec['name']}")
            except Exception as exception:
                message = str(exception).replace("'", "")
                fail_code = spec["status"]["fail"]
                formatted_traceback = traceback.format_exc()

                logger.exception(f"fail_insert_sql exception: {exception}")
                await _add_log_entry(fail_code, message, formatted_traceback)
                raise exception

            success_code = spec["status"]["success"]
            await _add_log_entry(success_code, "", duration=duration)

        logger.debug("Start updating feature last updated date")
        if deployed_tile_table_model is None:
            aggregation_ids = [params.aggregation_id]
        else:
            aggregation_ids = deployed_tile_table_model.aggregation_ids
        await self.feature_service.update_last_updated_by_scheduled_task_at(
            aggregation_ids=aggregation_ids,
            last_updated_by_scheduled_task_at=datetime.utcnow(),
        )
        await self.system_metrics_service.create_metrics(
            TileTaskMetrics(tile_table_id=params.tile_id, **metrics_data)
        )
        logger.info("End of TileSchedule.execute")

    def _derive_correct_job_ts(
        self, input_dt: datetime, frequency_minutes: int, time_modulo_frequency_seconds: int
    ) -> datetime:
        """
        Derive correct job schedule datetime

        Parameters
        ----------
        input_dt: datetime
            input job schedule datetime
        frequency_minutes: int
            frequency in minutes
        time_modulo_frequency_seconds: int
            time modulo frequency in seconds

        Returns
        -------
        datetime
        """
        input_dt = input_dt.replace(tzinfo=None)

        next_job_time = date_util.get_next_job_datetime(
            input_dt=input_dt,
            frequency_minutes=frequency_minutes,
            time_modulo_frequency_seconds=time_modulo_frequency_seconds,
        )

        logger.debug(
            "Inside derive_correct_job_ts",
            extra={"next_job_time": next_job_time, "input_dt": input_dt},
        )

        if next_job_time == input_dt:
            # if next_job_time is same as input_dt, then return next_job_time
            return next_job_time

        # if next_job_time is not same as input_dt, then return (next_job_time - frequency_minutes)
        return next_job_time - timedelta(minutes=frequency_minutes)
