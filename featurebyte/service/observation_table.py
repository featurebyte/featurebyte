"""
ObservationTableService class
"""
from __future__ import annotations

from typing import Any, Dict, Optional, cast

import pandas as pd
from bson import ObjectId

from featurebyte.enum import DBVarType, MaterializedTableNamePrefix, SpecialColumnName
from featurebyte.exception import (
    MissingPointInTimeColumnError,
    UnsupportedPointInTimeColumnTypeError,
)
from featurebyte.models.base import FeatureByteBaseDocumentModel
from featurebyte.models.observation_table import ObservationTableModel
from featurebyte.persistent import Persistent
from featurebyte.query_graph.node.schema import TableDetails
from featurebyte.query_graph.sql.materialisation import get_most_recent_point_in_time_sql
from featurebyte.schema.observation_table import ObservationTableCreate
from featurebyte.schema.worker.task.observation_table import ObservationTableTaskPayload
from featurebyte.service.context import ContextService
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.service.materialized_table import BaseMaterializedTableService
from featurebyte.session.base import BaseSession


class ObservationTableService(
    BaseMaterializedTableService[ObservationTableModel, ObservationTableModel]
):
    """
    ObservationTableService class
    """

    document_class = ObservationTableModel
    materialized_table_name_prefix = MaterializedTableNamePrefix.OBSERVATION_TABLE

    def __init__(
        self,
        user: Any,
        persistent: Persistent,
        catalog_id: Optional[ObjectId],
        feature_store_service: FeatureStoreService,
        context_service: ContextService,
    ):
        super().__init__(user, persistent, catalog_id, feature_store_service)
        self.context_service = context_service

    @property
    def class_name(self) -> str:
        return "ObservationTable"

    async def get_observation_table_task_payload(
        self, data: ObservationTableCreate
    ) -> ObservationTableTaskPayload:
        """
        Validate and convert a ObservationTableCreate schema to a ObservationTableTaskPayload schema
        which will be used to initiate the ObservationTable creation task.

        Parameters
        ----------
        data: ObservationTableCreate
            ObservationTable creation payload

        Returns
        -------
        ObservationTableTaskPayload
        """

        # Check any conflict with existing documents
        output_document_id = data.id or ObjectId()
        await self._check_document_unique_constraints(
            document=FeatureByteBaseDocumentModel(_id=output_document_id, name=data.name),
        )

        if data.context_id is not None:
            # Check if the context document exists when provided. This should perform additional
            # validation once additional information such as request schema are available in the
            # context.
            await self.context_service.get_document(document_id=data.context_id)

        return ObservationTableTaskPayload(
            **data.dict(by_alias=True),
            user_id=self.user.id,
            catalog_id=self.catalog_id,
            output_document_id=output_document_id,
        )

    @staticmethod
    async def get_most_recent_point_in_time(
        db_session: BaseSession,
        destination: TableDetails,
    ) -> str:
        """
        Get the most recent point in time from the destination table.

        Parameters
        ----------
        db_session: BaseSession
            Database session
        destination: TableDetails
            Table details of the materialized table

        Returns
        -------
        str
        """
        res = await db_session.execute_query(
            get_most_recent_point_in_time_sql(
                destination=destination,
                source_type=db_session.source_type,
            )
        )
        most_recent_point_in_time = res.iloc[0, 0]  # type: ignore[union-attr]
        most_recent_point_in_time = pd.Timestamp(most_recent_point_in_time)
        if most_recent_point_in_time.tzinfo is not None:
            most_recent_point_in_time = most_recent_point_in_time.tz_convert("UTC").tz_localize(
                None
            )
        most_recent_point_in_time = most_recent_point_in_time.isoformat()

        return cast(str, most_recent_point_in_time)

    @staticmethod
    async def validate_materialized_table_and_get_metadata(
        db_session: BaseSession, table_details: TableDetails
    ) -> Dict[str, Any]:
        """
        Validate and get additional metadata for the materialized observation table.

        Parameters
        ----------
        db_session: BaseSession
            Database session
        table_details: TableDetails
            Table details of the materialized table

        Returns
        -------
        dict[str, Any]

        Raises
        ------
        MissingPointInTimeColumnError
            If the point in time column is missing.
        UnsupportedPointInTimeColumnTypeError
            If the point in time column is not of timestamp type.
        """
        columns_info, num_rows = await ObservationTableService.get_columns_info_and_num_rows(
            db_session=db_session,
            table_details=table_details,
        )
        columns_info_mapping = {info.name: info for info in columns_info}

        if SpecialColumnName.POINT_IN_TIME not in columns_info_mapping:
            raise MissingPointInTimeColumnError(
                f"Point in time column not provided: {SpecialColumnName.POINT_IN_TIME}"
            )

        point_in_time_dtype = columns_info_mapping[SpecialColumnName.POINT_IN_TIME].dtype
        if point_in_time_dtype not in {
            DBVarType.TIMESTAMP,
            DBVarType.TIMESTAMP_TZ,
        }:
            raise UnsupportedPointInTimeColumnTypeError(
                f"Point in time column should have timestamp type; got {point_in_time_dtype}"
            )

        most_recent_point_in_time = await ObservationTableService.get_most_recent_point_in_time(
            db_session=db_session,
            destination=table_details,
        )
        return {
            "columns_info": columns_info,
            "num_rows": num_rows,
            "most_recent_point_in_time": most_recent_point_in_time,
        }
