"""
TileJobStatusService class
"""

from __future__ import annotations

import datetime
from collections import defaultdict
from typing import Any, List

import pandas as pd

from featurebyte.common.date_util import get_next_job_datetime
from featurebyte.common.utils import dataframe_to_json
from featurebyte.feature_manager.model import ExtendedFeatureModel
from featurebyte.models.tile import TileType
from featurebyte.models.tile_job_log import TileJobLogModel
from featurebyte.schema.common.base import BaseDocumentServiceUpdateSchema
from featurebyte.service.base_document import BaseDocumentService


class TileJobLogService(
    BaseDocumentService[TileJobLogModel, TileJobLogModel, BaseDocumentServiceUpdateSchema]
):
    """
    TileJobLogService class

    This service is used by TileTaskExecutor and downstream steps to update the status of the tile
    jobs. It also supports reporting of feature jobs summary to users.
    """

    document_class = TileJobLogModel
    document_update_class = BaseDocumentServiceUpdateSchema

    async def get_logs_dataframe(self, aggregation_ids: list[str], hour_limit: int) -> pd.DataFrame:
        """
        Retrieve tile job logs for a list of aggregation_ids as a pandas DataFrame

        Parameters
        ----------
        aggregation_ids: list[str]
            List of aggregation_ids to fetch
        hour_limit: int
            Limit in hours on the job history to fetch

        Returns
        -------
        pd.DataFrame
        """
        datetime_now = datetime.datetime.utcnow()
        min_created_at = datetime_now - datetime.timedelta(hours=hour_limit)
        query_filter = {
            "created_at": {"$gte": min_created_at, "$lt": datetime_now},
            "aggregation_id": {"$in": aggregation_ids},
            "tile_type": TileType.ONLINE.value,
        }
        columns = [
            "session_id",
            "created_at",
            "aggregation_id",
            "status",
            "message",
        ]
        data = defaultdict(list)
        async for doc in self.list_documents_as_dict_iterator(query_filter=query_filter):
            for col in columns:
                data[col.upper()].append(doc[col])
        result = pd.DataFrame(data=data)
        if result.shape[0] == 0:
            return pd.DataFrame(columns=[col.upper() for col in columns])
        return result

    async def get_feature_job_logs(
        self,
        features: List[ExtendedFeatureModel],
        hour_limit: int,
    ) -> dict[str, Any]:
        """
        Retrieve feature job logs for a list of features

        Parameters
        ----------
        features: List[ExtendedFeatureModel]
            List of features
        hour_limit: int
            Limit in hours on the job history to fetch

        Returns
        -------
        dict[str, Any]
            Dataframe converted to json string
        """
        # compile list of aggregation_ids to filter logs
        aggregation_ids = []
        for feature in features:
            for tile_spec in feature.tile_specs:
                aggregation_ids.append(tile_spec.aggregation_id)

        result = await self.get_logs_dataframe(
            aggregation_ids=aggregation_ids, hour_limit=hour_limit
        )

        return dataframe_to_json(self._summarize_logs(result, features))

    @staticmethod
    def _summarize_logs(logs: pd.DataFrame, features: List[ExtendedFeatureModel]) -> pd.DataFrame:
        """
        Summarize logs by session

        Parameters
        ----------
        logs: pd.DataFrame
            Logs records
        features: List[ExtendedFeatureModel]
            List of features

        Returns
        -------
        pd.DataFrame
        """

        def _summarize_session(session_logs: pd.DataFrame) -> pd.DataFrame:
            """
            Compute session durations

            Parameters
            ----------
            session_logs: pd.DataFrame
                Log records in a single session

            Returns
            -------
            pd.DataFrame
            """
            session_logs.index = session_logs["STATUS"]
            timestamps = session_logs["CREATED_AT"].to_dict()

            # extract timestamps for key steps
            standard_statuses = ["STARTED", "MONITORED", "GENERATED", "COMPLETED"]
            summarized_logs = pd.DataFrame({
                status: [timestamps.get(status, pd.NaT)] for status in standard_statuses
            })

            # extract error message if any
            summarized_logs["ERROR"] = None
            error_logs = session_logs[~session_logs["STATUS"].isin(standard_statuses)]
            if error_logs.shape[0] > 0:
                summarized_logs["ERROR"] = error_logs["MESSAGE"].iloc[0]
            return summarized_logs

        tile_specs = []
        tile_specs_cols = ["aggregation_id", "frequency_minute", "time_modulo_frequency_second"]
        for feature in features:
            if feature.tile_specs:
                _tile_specs = pd.DataFrame.from_dict([
                    tile_spec.model_dump() for tile_spec in feature.tile_specs
                ])
                tile_specs.append(_tile_specs[tile_specs_cols])
        feature_tile_specs = (
            pd.concat(tile_specs).drop_duplicates()
            if tile_specs
            else pd.DataFrame(columns=tile_specs_cols)
        )

        # summarize logs by session
        sessions = logs.groupby(["SESSION_ID", "AGGREGATION_ID"], group_keys=True).apply(
            _summarize_session
        )
        output_columns = [
            "SESSION_ID",
            "AGGREGATION_ID",
            "SCHEDULED",
            "STARTED",
            "COMPLETED",
            "QUEUE_DURATION",
            "COMPUTE_DURATION",
            "TOTAL_DURATION",
            "ERROR",
        ]
        if sessions.shape[0] > 0:
            # exclude sessions that started before the range
            sessions = sessions[~sessions["STARTED"].isnull()].reset_index()
            sessions = sessions.merge(
                feature_tile_specs, left_on="AGGREGATION_ID", right_on="aggregation_id"
            )
            if sessions.shape[0] > 0:
                sessions["SCHEDULED"] = sessions.apply(
                    lambda row: get_next_job_datetime(
                        row.STARTED, row.frequency_minute, row.time_modulo_frequency_second
                    ),
                    axis=1,
                ) - pd.to_timedelta(sessions.frequency_minute, unit="minute")
                sessions["COMPUTE_DURATION"] = (
                    sessions["COMPLETED"] - sessions["STARTED"]
                ).dt.total_seconds()
                sessions["QUEUE_DURATION"] = (
                    sessions["STARTED"] - sessions["SCHEDULED"]
                ).dt.total_seconds()
                sessions["TOTAL_DURATION"] = (
                    sessions["COMPLETED"] - sessions["SCHEDULED"]
                ).dt.total_seconds()
                return sessions[output_columns]
        return pd.DataFrame(columns=output_columns)
