"""
ObservationTableService class
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple, cast

from dataclasses import dataclass
from pathlib import Path

import pandas as pd
from bson import ObjectId
from sqlglot import Expression, expressions

from featurebyte.api.source_table import SourceTable
from featurebyte.common.utils import dataframe_from_json
from featurebyte.enum import DBVarType, MaterializedTableNamePrefix, SpecialColumnName, SourceType
from featurebyte.exception import (
    MissingPointInTimeColumnError,
    ObservationTableInvalidContextError,
    ObservationTableInvalidUseCaseError,
    UnsupportedPointInTimeColumnTypeError,
)
from featurebyte.models.base import FeatureByteBaseDocumentModel, PydanticObjectId
from featurebyte.models.feature_store import FeatureStoreModel
from featurebyte.models.materialized_table import ColumnSpecWithEntityId
from featurebyte.models.observation_table import ObservationTableModel, TargetInput
from featurebyte.persistent import Persistent
from featurebyte.query_graph.model.common_table import TabularSource
from featurebyte.query_graph.node.schema import TableDetails
from featurebyte.query_graph.sql.adapter import get_sql_adapter
from featurebyte.query_graph.sql.ast.literal import make_literal_value
from featurebyte.query_graph.sql.common import (
    get_fully_qualified_table_name,
    quoted_identifier,
    sql_to_string,
)
from featurebyte.routes.block_modification_handler import BlockModificationHandler
from featurebyte.routes.common.primary_entity_validator import PrimaryEntityValidator
from featurebyte.schema.feature_store import FeatureStoreSample
from featurebyte.schema.observation_table import (
    ObservationTableCreate,
    ObservationTableUpdate,
    ObservationTableUpload,
)
from featurebyte.schema.worker.task.observation_table import ObservationTableTaskPayload
from featurebyte.schema.worker.task.observation_table_upload import (
    ObservationTableUploadTaskPayload,
)
from featurebyte.service.context import ContextService
from featurebyte.service.entity import EntityService
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.service.materialized_table import BaseMaterializedTableService
from featurebyte.service.preview import PreviewService
from featurebyte.service.session_manager import SessionManagerService
from featurebyte.service.use_case import UseCaseService
from featurebyte.session.base import BaseSession
from featurebyte.storage import Storage


@dataclass
class PointInTimeStats:
    """
    Point in time stats
    """

    least_recent: str
    most_recent: str


def _convert_ts_to_str(timestamp_str: str) -> str:
    timestamp_obj = pd.Timestamp(timestamp_str)
    if timestamp_obj.tzinfo is not None:
        timestamp_obj = timestamp_obj.tz_convert("UTC").tz_localize(None)
    return cast(str, timestamp_obj.isoformat())


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

    def __init__(  # pylint: disable=too-many-arguments
        self,
        user: Any,
        persistent: Persistent,
        catalog_id: Optional[ObjectId],
        session_manager_service: SessionManagerService,
        feature_store_service: FeatureStoreService,
        entity_service: EntityService,
        context_service: ContextService,
        preview_service: PreviewService,
        temp_storage: Storage,
        block_modification_handler: BlockModificationHandler,
        primary_entity_validator: PrimaryEntityValidator,
        use_case_service: UseCaseService,
    ):
        super().__init__(
            user,
            persistent,
            catalog_id,
            session_manager_service,
            feature_store_service,
            entity_service,
            block_modification_handler,
        )
        self.context_service = context_service
        self.preview_service = preview_service
        self.temp_storage = temp_storage
        self.primary_entity_validator = primary_entity_validator
        self.use_case_service = use_case_service

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

    async def get_observation_table_upload_task_payload(
        self,
        data: ObservationTableUpload,
        observation_set_dataframe: pd.DataFrame,
    ) -> ObservationTableUploadTaskPayload:
        """
        Validate and convert a ObservationTableUpload schema to a ObservationTableUploadTaskPayload schema
        which will be used to initiate the ObservationTable upload task.

        Parameters
        ----------
        data: ObservationTableUpload
            ObservationTable upload payload
        observation_set_dataframe: pd.DataFrame
            Observation set DataFrame

        Returns
        -------
        ObservationTableUploadTaskPayload
        """

        # Check any conflict with existing documents
        output_document_id = data.id or ObjectId()
        await self._check_document_unique_constraints(
            document=FeatureByteBaseDocumentModel(_id=output_document_id, name=data.name),
        )

        # Persist dataframe to parquet file that can be read by the task later
        observation_set_storage_path = f"observation_table/{output_document_id}.parquet"
        await self.temp_storage.put_dataframe(
            observation_set_dataframe, Path(observation_set_storage_path)
        )

        return ObservationTableUploadTaskPayload(
            **data.dict(by_alias=True),
            user_id=self.user.id,
            catalog_id=self.catalog_id,
            output_document_id=output_document_id,
            observation_set_storage_path=observation_set_storage_path,
        )

    @staticmethod
    def get_minimum_iet_sql_expr(
        entity_column_names: List[str], table_details: TableDetails, source_type: SourceType
    ) -> Expression:
        """
        Get the SQL expression to compute the minimum interval in seconds for each entity.

        Parameters
        ----------
        entity_column_names: List[str]
            List of entity column names
        table_details: TableDetails
            Table details of the materialized table
        source_type: SourceType
            Source type

        Returns
        -------
        str
        """
        adapter = get_sql_adapter(source_type)

        window_expression = expressions.Window(
            this=expressions.Anonymous(
                this="LAG", expressions=[make_literal_value(SpecialColumnName.POINT_IN_TIME)]
            ),
            partition_by=[make_literal_value(col_name) for col_name in entity_column_names],
            order=expressions.Order(
                expressions=[
                    expressions.Ordered(this=make_literal_value(SpecialColumnName.POINT_IN_TIME))
                ]
            ),
        )
        aliased_window = expressions.Alias(
            this=window_expression,
            alias=quoted_identifier("PREVIOUS_POINT_IN_TIME"),
        )
        inner_query = expressions.select(aliased_window, SpecialColumnName.POINT_IN_TIME).from_(
            get_fully_qualified_table_name(table_details.dict())
        )

        # TODO: convert to seconds
        datediff_expr = adapter.datediff_microsecond(quoted_identifier("PREVIOUS_POINT_IN_TIME"), quoted_identifier(SpecialColumnName.POINT_IN_TIME))
        difference_expr = expressions.Alias(
            this=datediff_expr,
            alias=quoted_identifier("interval")
        )
        iet_expr = expressions.select(difference_expr).from_(inner_query.subquery())
        return expressions.select(expressions.Min(this=quoted_identifier("interval"))).from_(iet_expr.subquery())

    async def _get_entity_col_name_to_minimum_interval(
        self,
        db_session: BaseSession,
        columns_info: List[ColumnSpecWithEntityId],
        table_details: TableDetails,
    ) -> Dict[str, int]:
        """
        Get the entity column name to minimum interval mapping.

        Parameters
        ----------
        db_session: BaseSession
            Database session
        columns_info: List[ColumnSpecWithEntityId]
            List of column specs with entity id
        table_details: TableDetails
            Table details of the materialized table

        Returns
        -------
        Dict[str, int]
            column name to minimum interval in seconds
        """
        entity_col_names = [col.name for col in columns_info if col.entity_id is not None]
        # Construct SQL
        sql_expr = self.get_minimum_iet_sql_expr(entity_col_names, table_details, db_session.source_type)

        # Execute SQL
        sql_string = sql_to_string(sql_expr, db_session.source_type)
        df_row_count = await db_session.execute_query(sql_string)
        assert df_row_count is not None

        # Build output for entity columns
        column_name_to_minimum_interval = {}
        for col_name in entity_col_names:
            # TODO: get the results from the dataframe
            column_name_to_minimum_interval[col_name] = 1
        return column_name_to_minimum_interval

    async def _get_column_name_to_entity_count(
        self,
        feature_store: FeatureStoreModel,
        table_details: TableDetails,
        columns_info: List[ColumnSpecWithEntityId],
    ) -> Tuple[Dict[str, int], PointInTimeStats]:
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
        Tuple[Dict[str, int], PointInTimeStats]
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
            stats_names=["unique", "max", "min"],
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
        least_recent_time_str = describe_stats_dataframe.loc[  # pylint: disable=no-member
            "min", SpecialColumnName.POINT_IN_TIME
        ]
        least_recent_time_str = _convert_ts_to_str(least_recent_time_str)
        most_recent_time_str = describe_stats_dataframe.loc[  # pylint: disable=no-member
            "max", SpecialColumnName.POINT_IN_TIME
        ]
        most_recent_time_str = _convert_ts_to_str(most_recent_time_str)
        return column_name_to_count, PointInTimeStats(
            least_recent=least_recent_time_str, most_recent=most_recent_time_str
        )

    async def validate_materialized_table_and_get_metadata(
        self,
        db_session: BaseSession,
        table_details: TableDetails,
        feature_store: FeatureStoreModel,
        serving_names_remapping: Optional[Dict[str, str]] = None,
        skip_entity_validation_checks: bool = False,
        primary_entity_ids: Optional[List[PydanticObjectId]] = None,
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
        primary_entity_ids: Optional[List[PydanticObjectId]]
            List of primary entity IDs

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
        # Perform validation on primary entity IDs. We always perform this check if the primary entity IDs exist.
        if primary_entity_ids is not None:
            await self.primary_entity_validator.validate_entities_are_primary_entities(
                primary_entity_ids
            )

        # Perform validation on column info
        validate_columns_info(columns_info, skip_entity_validation_checks)
        # Get entity column name to minimum interval mapping
        column_name_to_minimum_interval = await self._get_entity_col_name_to_minimum_interval(
            db_session, columns_info, table_details
        )
        # Get entity statistics metadata
        column_name_to_count, point_in_time_stats = await self._get_column_name_to_entity_count(
            feature_store, table_details, columns_info
        )

        return {
            "columns_info": columns_info,
            "num_rows": num_rows,
            "most_recent_point_in_time": point_in_time_stats.most_recent,
            "least_recent_point_in_time": point_in_time_stats.least_recent,
            "entity_column_name_to_count": column_name_to_count,
            "entity_column_name_to_min_interval_secs": column_name_to_minimum_interval,
        }

    async def update_observation_table(  # pylint: disable=too-many-branches
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
        ObservationTableInvalidUseCaseError
            If the use case is invalid
        """

        if data.use_case_ids:
            raise ObservationTableInvalidUseCaseError("use_case_ids is not a valid field to update")

        observation_table = await self.get_document(document_id=observation_table_id)
        if data.use_case_id_to_add or data.use_case_id_to_remove:
            if not observation_table.context_id:
                raise ObservationTableInvalidUseCaseError(
                    f"Cannot add/remove UseCase as the ObservationTable {observation_table_id} is not associated with any Context."
                )

            use_case_ids = observation_table.use_case_ids
            # validate use case id to add
            if data.use_case_id_to_add:
                if data.use_case_id_to_add in use_case_ids:
                    raise ObservationTableInvalidUseCaseError(
                        f"Cannot add UseCase {data.use_case_id_to_add} as it is already associated with the ObservationTable."
                    )
                use_case = await self.use_case_service.get_document(
                    document_id=data.use_case_id_to_add
                )
                if use_case.context_id != observation_table.context_id:
                    raise ObservationTableInvalidUseCaseError(
                        f"Cannot add UseCase {data.use_case_id_to_add} as its context_id is different from the existing context_id."
                    )

                # validate request_input and target_id
                if not isinstance(observation_table.request_input, TargetInput):
                    raise ObservationTableInvalidUseCaseError(
                        "observation table request_input is not TargetInput"
                    )

                if not use_case.target_id or (
                    isinstance(observation_table.request_input, TargetInput)
                    and observation_table.request_input.target_id != use_case.target_id
                ):
                    raise ObservationTableInvalidUseCaseError(
                        f"Cannot add UseCase {data.use_case_id_to_add} as its target_id is different from the existing target_id."
                    )

                use_case_ids.append(data.use_case_id_to_add)

            # validate use case id to remove
            if data.use_case_id_to_remove:
                if data.use_case_id_to_remove not in use_case_ids:
                    raise ObservationTableInvalidUseCaseError(
                        f"Cannot remove UseCase {data.use_case_id_to_remove} as it is not associated with the ObservationTable."
                    )
                use_case_ids.remove(data.use_case_id_to_remove)

            # update use case ids
            data.use_case_ids = sorted(use_case_ids)

        if data.context_id:
            # validate context_id
            new_context = await self.context_service.get_document(document_id=data.context_id)

            if observation_table.context_id and observation_table.context_id != data.context_id:
                exist_context = await self.context_service.get_document(
                    document_id=observation_table.context_id
                )
                if set(exist_context.primary_entity_ids) != set(new_context.primary_entity_ids):
                    raise ObservationTableInvalidContextError(
                        "Cannot update Context as the entities are different from the existing Context."
                    )

        result: Optional[ObservationTableModel] = await self.update_document(
            document_id=observation_table_id, data=data, return_document=True
        )

        return result
