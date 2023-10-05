"""
ObservationTableService class
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional, cast

from dataclasses import dataclass

import pandas as pd
from bson import ObjectId

from featurebyte import SourceTable
from featurebyte.common.utils import dataframe_from_json
from featurebyte.enum import DBVarType, MaterializedTableNamePrefix, SpecialColumnName
from featurebyte.exception import (
    MissingPointInTimeColumnError,
    ObservationTableInvalidContextError,
    UnsupportedPointInTimeColumnTypeError,
)
from featurebyte.models import FeatureStoreModel
from featurebyte.models.base import FeatureByteBaseDocumentModel
from featurebyte.models.materialized_table import ColumnSpecWithEntityId
from featurebyte.models.observation_table import ObservationTableModel
from featurebyte.persistent import Persistent
from featurebyte.query_graph.model.common_table import TabularSource
from featurebyte.query_graph.node.schema import TableDetails
from featurebyte.query_graph.sql.materialisation import (
    get_earliest_and_most_recent_point_in_time_sql,
)
from featurebyte.schema.feature_store import FeatureStoreSample
from featurebyte.schema.observation_table import ObservationTableCreate, ObservationTableUpdate
from featurebyte.schema.worker.task.observation_table import ObservationTableTaskPayload
from featurebyte.service.context import ContextService
from featurebyte.service.entity import EntityService
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.service.materialized_table import BaseMaterializedTableService
from featurebyte.service.preview import PreviewService
from featurebyte.service.session_manager import SessionManagerService
from featurebyte.session.base import BaseSession


@dataclass
class PointInTimeStats:
    """
    Point in time stats
    """

    earliest: str
    most_recent: str


async def get_point_in_time_stats(
    db_session: BaseSession,
    destination: TableDetails,
) -> PointInTimeStats:
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
        get_earliest_and_most_recent_point_in_time_sql(
            destination=destination,
            source_type=db_session.source_type,
        )
    )
    earliest_point_in_time = res.iloc[0, 0]  # type: ignore[union-attr]
    most_recent_point_in_time = res.iloc[0, 1]  # type: ignore[union-attr]

    def _convert_ts_to_str(timestamp_str: str) -> str:
        current_timestamp = pd.Timestamp(timestamp_str)
        if current_timestamp.tzinfo is not None:
            current_timestamp = current_timestamp.tz_convert("UTC").tz_localize(None)
        current_timestamp = current_timestamp.isoformat()
        return cast(str, current_timestamp)

    most_recent_point_in_time_str = _convert_ts_to_str(most_recent_point_in_time)
    earliest_str = _convert_ts_to_str(earliest_point_in_time)
    return PointInTimeStats(most_recent=most_recent_point_in_time_str, earliest=earliest_str)


def validate_columns_info(
    columns_info: List[ColumnSpecWithEntityId], skip_entity_validation_checks: bool = False
) -> None:
    """
    Validate column info.

    Parameters
    ----------
    columns_info: List[ColumnSpecWithEntityId]
        List of column specs with entity id
    skip_entity_validation_checks: bool
        Whether to skip entity validation checks

    Raises
    ------
    MissingPointInTimeColumnError
        If the point in time column is missing.
    UnsupportedPointInTimeColumnTypeError
        If the point in time column is not of timestamp type.
    ValueError
        If no entity column is provided.
    """
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

    if not skip_entity_validation_checks:
        # Check that there's at least one entity mapped in the columns info
        if not any(info.entity_id is not None for info in columns_info):
            raise ValueError("At least one entity column should be provided.")


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
        preview_service: PreviewService,
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
        self.preview_service = preview_service

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

    async def _get_column_name_to_entity_count(
        self,
        feature_store: FeatureStoreModel,
        table_details: TableDetails,
        columns_info: List[ColumnSpecWithEntityId],
    ) -> Dict[str, int]:
        """
        Get the entity column name to unique entity count mapping.

        Parameters
        ----------
        feature_store: FeatureStoreModel
            Feature store document
        table_details: TableDetails
            Table details of the materialized table
        columns_info: List[ColumnSpecWithEntityId]
            List of column specs with entity id

        Returns
        -------
        Dict[str, int]
        """
        # Get describe statistics
        source_table = SourceTable(
            feature_store=feature_store,
            tabular_source=TabularSource(
                feature_store_id=feature_store.id,
                table_details=TableDetails(
                    database_name=table_details.database_name,
                    schema_name=table_details.schema_name,
                    table_name=table_details.table_name,
                ),
            ),
            columns_info=columns_info,
        )
        graph, node = source_table.frame.extract_pruned_graph_and_node()
        sample = FeatureStoreSample(
            feature_store_name=feature_store.name,
            graph=graph,
            node_name=node.name,
            stats_names=["unique"],
        )
        describe_stats_json = await self.preview_service.describe(sample, 0, 1234)
        describe_stats_dataframe = dataframe_from_json(describe_stats_json)
        entity_cols = [col for col in columns_info if col.entity_id is not None]
        column_name_to_count = {}
        for col in entity_cols:
            col_name = col.name
            column_name_to_count[
                col_name
            ] = describe_stats_dataframe.loc[  # pylint: disable=no-member
                "unique", col_name
            ]
        return column_name_to_count

    async def validate_materialized_table_and_get_metadata(
        self,
        db_session: BaseSession,
        table_details: TableDetails,
        feature_store: FeatureStoreModel,
        serving_names_remapping: Optional[Dict[str, str]] = None,
        skip_entity_validation_checks: bool = False,
    ) -> Dict[str, Any]:
        """
        Validate and get additional metadata for the materialized observation table.

        Parameters
        ----------
        db_session: BaseSession
            Database session
        table_details: TableDetails
            Table details of the materialized table
        feature_store: FeatureStoreModel
            Feature store document
        serving_names_remapping: Dict[str, str]
            Remapping of serving names
        skip_entity_validation_checks: bool
            Whether to skip entity validation checks

        Returns
        -------
        dict[str, Any]
        """
        # Get column info and number of row metadata
        columns_info, num_rows = await self.get_columns_info_and_num_rows(
            db_session=db_session,
            table_details=table_details,
            serving_names_remapping=serving_names_remapping,
        )
        # Perform validation on column info
        validate_columns_info(columns_info, skip_entity_validation_checks)
        # Get point in time metadata
        point_in_time_stats = await get_point_in_time_stats(
            db_session=db_session,
            destination=table_details,
        )
        # Get entity statistics metadata
        column_name_to_count = await self._get_column_name_to_entity_count(
            feature_store, table_details, columns_info
        )
        return {
            "columns_info": columns_info,
            "num_rows": num_rows,
            "most_recent_point_in_time": point_in_time_stats.most_recent,
            "earliest_point_in_time": point_in_time_stats.earliest,
            "entity_column_name_to_count": column_name_to_count,
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
            if set(exist_context.primary_entity_ids) != set(new_context.primary_entity_ids):
                raise ObservationTableInvalidContextError(
                    "Cannot update Context as the entities are different from the existing Context."
                )

        observation_table: Optional[ObservationTableModel] = await self.update_document(
            document_id=observation_table_id, data=data, return_document=True
        )

        return observation_table
