"""
Service for interacting with the data warehouse for queries around the feature store.

We split this into a separate service, as these typically require a session object that is created.
"""
from __future__ import annotations

from typing import Any, List

import datetime

import pandas as pd
from bson.objectid import ObjectId
from sqlglot import expressions

from featurebyte.common.date_util import get_next_job_datetime
from featurebyte.common.utils import dataframe_to_json
from featurebyte.feature_manager.model import ExtendedFeatureModel
from featurebyte.models.feature_store import FeatureStoreModel
from featurebyte.persistent import Persistent
from featurebyte.query_graph.node.schema import ColumnSpec
from featurebyte.query_graph.sql.ast.literal import make_literal_value
from featurebyte.query_graph.sql.common import quoted_identifier, sql_to_string
from featurebyte.service.base_service import BaseService
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.service.session_manager import SessionManagerService


class FeatureStoreWarehouseService(BaseService):
    """
    FeatureStoreWarehouseService is responsible for interacting with the data warehouse.
    """

    def __init__(
        self,
        user: Any,
        persistent: Persistent,
        catalog_id: ObjectId,
        session_manager_service: SessionManagerService,
        feature_store_service: FeatureStoreService,
    ):
        super().__init__(user, persistent, catalog_id)
        self.session_manager_service = session_manager_service
        self.feature_store_service = feature_store_service

    async def list_databases(
        self, feature_store: FeatureStoreModel, get_credential: Any
    ) -> List[str]:
        """
        List databases in feature store

        Parameters
        ----------
        feature_store: FeatureStoreModel
            FeatureStoreModel object
        get_credential: Any
            Get credential handler function

        Returns
        -------
        List[str]
            List of database names
        """
        db_session = await self.session_manager_service.get_feature_store_session(
            feature_store=feature_store, get_credential=get_credential
        )
        return await db_session.list_databases()

    async def list_schemas(
        self,
        feature_store: FeatureStoreModel,
        database_name: str,
        get_credential: Any,
    ) -> List[str]:
        """
        List schemas in feature store

        Parameters
        ----------
        feature_store: FeatureStoreModel
            FeatureStoreModel object
        database_name: str
            Name of database to use
        get_credential: Any
            Get credential handler function

        Returns
        -------
        List[str]
            List of schema names
        """
        db_session = await self.session_manager_service.get_feature_store_session(
            feature_store=feature_store, get_credential=get_credential
        )
        return await db_session.list_schemas(database_name=database_name)

    async def list_tables(
        self,
        feature_store: FeatureStoreModel,
        database_name: str,
        schema_name: str,
        get_credential: Any,
    ) -> List[str]:
        """
        List tables in feature store

        Parameters
        ----------
        feature_store: FeatureStoreModel
            FeatureStoreModel object
        database_name: str
            Name of database to use
        schema_name: str
            Name of schema to use
        get_credential: Any
            Get credential handler function

        Returns
        -------
        List[str]
            List of table names
        """
        db_session = await self.session_manager_service.get_feature_store_session(
            feature_store=feature_store, get_credential=get_credential
        )
        return await db_session.list_tables(database_name=database_name, schema_name=schema_name)

    async def list_columns(
        self,
        feature_store: FeatureStoreModel,
        database_name: str,
        schema_name: str,
        table_name: str,
        get_credential: Any,
    ) -> List[ColumnSpec]:
        """
        List columns in database table

        Parameters
        ----------
        feature_store: FeatureStoreModel
            FeatureStoreModel object
        database_name: str
            Name of database to use
        schema_name: str
            Name of schema to use
        table_name: str
            Name of table to use
        get_credential: Any
            Get credential handler function

        Returns
        -------
        List[ColumnSpec]
            List of ColumnSpec object
        """
        db_session = await self.session_manager_service.get_feature_store_session(
            feature_store=feature_store, get_credential=get_credential
        )
        table_schema = await db_session.list_table_schema(
            database_name=database_name, schema_name=schema_name, table_name=table_name
        )
        return [ColumnSpec(name=name, dtype=dtype) for name, dtype in table_schema.items()]

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
            summarized_logs = pd.DataFrame(
                {status: [timestamps.get(status, pd.NaT)] for status in standard_statuses}
            )

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
                _tile_specs = pd.DataFrame.from_dict(
                    [tile_spec.dict() for tile_spec in feature.tile_specs]
                )
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

    async def get_feature_job_logs(
        self,
        feature_store_id: ObjectId,
        features: List[ExtendedFeatureModel],
        hour_limit: int,
        get_credential: Any,
    ) -> dict[str, Any]:
        """
        Retrieve data preview for query graph node

        Parameters
        ----------
        feature_store_id: ObjectId
            Feature List
        features: List[ExtendedFeatureModel]
            List of features
        hour_limit: int
            Limit in hours on the job history to fetch
        get_credential: Any
            Get credential handler function

        Returns
        -------
        dict[str, Any]
            Dataframe converted to json string
        """
        feature_store = await self.feature_store_service.get_document(feature_store_id)
        db_session = await self.session_manager_service.get_feature_store_session(
            feature_store=feature_store, get_credential=get_credential
        )
        utcnow = datetime.datetime.utcnow()

        # compile list of aggregation_ids to filter logs
        aggregation_ids = []
        for feature in features:
            for tile_spec in feature.tile_specs:
                aggregation_ids.append(tile_spec.aggregation_id)

        sql_expr = (
            expressions.select(
                quoted_identifier("SESSION_ID"),
                quoted_identifier("CREATED_AT"),
                quoted_identifier("AGGREGATION_ID"),
                quoted_identifier("STATUS"),
                quoted_identifier("MESSAGE"),
            )
            .from_("TILE_JOB_MONITOR")
            .where(
                expressions.And(
                    expressions=[
                        expressions.GTE(
                            this=quoted_identifier("CREATED_AT"),
                            expression=make_literal_value(
                                utcnow - datetime.timedelta(hours=hour_limit),
                                cast_as_timestamp=True,
                            ),
                        ),
                        expressions.LT(
                            this=quoted_identifier("CREATED_AT"),
                            expression=make_literal_value(utcnow, cast_as_timestamp=True),
                        ),
                        expressions.In(
                            this=quoted_identifier("AGGREGATION_ID"),
                            expressions=[
                                make_literal_value(aggregation_id)
                                for aggregation_id in set(aggregation_ids)
                            ],
                        ),
                        expressions.EQ(
                            this=quoted_identifier("TILE_TYPE"),
                            expression=make_literal_value("ONLINE"),
                        ),
                    ]
                )
            )
        )
        sql = sql_to_string(sql_expr, source_type=feature_store.type)
        result = await db_session.execute_query(sql)
        return dataframe_to_json(self._summarize_logs(result, features))
