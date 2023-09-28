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
    ObservationTableInvalidContextError,
    UnsupportedPointInTimeColumnTypeError,
)
from featurebyte.models.base import FeatureByteBaseDocumentModel
from featurebyte.models.observation_table import ObservationTableModel
from featurebyte.persistent import Persistent
from featurebyte.query_graph.node.schema import TableDetails
from featurebyte.query_graph.sql.materialisation import get_most_recent_point_in_time_sql
from featurebyte.schema.observation_table import ObservationTableCreate, ObservationTableUpdate
from featurebyte.schema.worker.task.observation_table import ObservationTableTaskPayload
from featurebyte.service.context import ContextService
from featurebyte.service.entity import EntityService
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.service.materialized_table import BaseMaterializedTableService
from featurebyte.service.session_manager import SessionManagerService
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
        session_manager_service: SessionManagerService,
        feature_store_service: FeatureStoreService,
        entity_service: EntityService,
        context_service: ContextService,
    ):
        super().__init__(
            user,
            persistent,
            catalog_id,
            session_manager_service,
            feature_store_service,
            entity_service,
        )
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

    async def validate_materialized_table_and_get_metadata(
        self,
        db_session: BaseSession,
        table_details: TableDetails,
        serving_names_remapping: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        """
        Validate and get additional metadata for the materialized observation table.

        Parameters
        ----------
        db_session: BaseSession
            Database session
        table_details: TableDetails
            Table details of the materialized table
        serving_names_remapping: Dict[str, str]
            Remapping of serving names

        Returns
        -------
        dict[str, Any]

        Raises
        ------
        MissingPointInTimeColumnError
            If the point in time column is missing.
        UnsupportedPointInTimeColumnTypeError
            If the point in time column is not of timestamp type.
        ValueError
            If no entity column is provided.
        """
        columns_info, num_rows = await self.get_columns_info_and_num_rows(
            db_session=db_session,
            table_details=table_details,
            serving_names_remapping=serving_names_remapping,
        )
        columns_info_mapping = {info.name: info for info in columns_info}

        # Check that there's at least one entity mapped in the columns info
        if not any(info.entity_id is not None for info in columns_info):
            raise ValueError("At least one entity column should be provided.")

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

    async def update_observation_table(
        self, observation_table_id: ObjectId, data: ObservationTableUpdate
    ) -> Optional[ObservationTableModel]:
        """
        Update ObservationTable

        Parameters
        ----------
        observation_table_id: ObjectId
            ObservationTable document_id
        data: ObservationTableUpdate
            ObservationTable update payload

        Returns
        -------
        Optional[ObservationTableModel]

        Raises
        ------
        ObservationTableInvalidContextError
            If the entity ids are different from the existing Context
        """

        # validate that the context exists
        await self.context_service.get_document(document_id=data.context_id)

        exist_observation_table = await self.get_document(document_id=observation_table_id)
        if (
            exist_observation_table.context_id
            and exist_observation_table.context_id != data.context_id
        ):
            exist_context = await self.context_service.get_document(
                document_id=exist_observation_table.context_id
            )
            new_context = await self.context_service.get_document(document_id=data.context_id)
            if set(exist_context.entity_ids) != set(new_context.entity_ids):
                raise ObservationTableInvalidContextError(
                    "Cannot update Context as the entities are different from the existing Context."
                )

        observation_table: Optional[ObservationTableModel] = await self.update_document(
            document_id=observation_table_id, data=data, return_document=True
        )

        return observation_table
