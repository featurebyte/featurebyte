"""
ObservationTableService class
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union, cast

import pandas as pd
import pytz
from bson import ObjectId
from redis import Redis
from sqlglot import expressions
from sqlglot.expressions import Expression

from featurebyte import TargetType
from featurebyte.api.source_table import SourceTable
from featurebyte.common.model_util import validate_timezone_offset_string
from featurebyte.enum import (
    DBVarType,
    MaterializedTableNamePrefix,
    SpecialColumnName,
    UploadFileFormat,
)
from featurebyte.exception import (
    InvalidForecastTimezoneColumnTypeError,
    InvalidForecastTimezoneValueError,
    MissingForecastPointColumnError,
    MissingForecastTimezoneColumnError,
    MissingPointInTimeColumnError,
    ObservationTableInvalidContextError,
    ObservationTableInvalidSamplingError,
    ObservationTableInvalidTargetNameError,
    ObservationTableInvalidTreatmentNameError,
    ObservationTableInvalidUseCaseError,
    ObservationTableMissingColumnsError,
    ObservationTableTargetDefinitionExistsError,
    UnsupportedForecastPointColumnTypeError,
    UnsupportedPointInTimeColumnTypeError,
)
from featurebyte.models.base import FeatureByteBaseDocumentModel, PydanticObjectId
from featurebyte.models.context import ContextModel
from featurebyte.models.feature_store import FeatureStoreModel
from featurebyte.models.materialized_table import ColumnSpecWithEntityId
from featurebyte.models.observation_table import (
    ManagedViewObservationInput,
    ObservationTableModel,
    ObservationTableObservationInput,
    Purpose,
    SourceTableObservationInput,
    TargetInput,
)
from featurebyte.models.request_input import BaseRequestInput
from featurebyte.models.target_namespace import TargetNamespaceModel
from featurebyte.models.treatment import TreatmentModel
from featurebyte.models.use_case import UseCaseModel
from featurebyte.persistent import Persistent
from featurebyte.query_graph.graph import QueryGraph
from featurebyte.query_graph.model.common_table import TabularSource
from featurebyte.query_graph.model.timestamp_schema import TimeZoneColumn
from featurebyte.query_graph.node.schema import TableDetails
from featurebyte.query_graph.sql.adapter import BaseAdapter
from featurebyte.query_graph.sql.ast.literal import make_literal_value
from featurebyte.query_graph.sql.common import (
    get_fully_qualified_table_name,
    quoted_identifier,
    sql_to_string,
)
from featurebyte.routes.block_modification_handler import BlockModificationHandler
from featurebyte.routes.common.primary_entity_validator import PrimaryEntityValidator
from featurebyte.schema.context import ContextUpdate
from featurebyte.schema.feature_store import FeatureStoreSample
from featurebyte.schema.observation_table import (
    ObservationTableCreate,
    ObservationTableServiceUpdate,
    ObservationTableUpload,
)
from featurebyte.schema.use_case import UseCaseUpdate
from featurebyte.schema.worker.task.observation_table import ObservationTableTaskPayload
from featurebyte.schema.worker.task.observation_table_upload import (
    ObservationTableUploadTaskPayload,
)
from featurebyte.service.context import ContextService
from featurebyte.service.entity import EntityService
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.service.managed_view import ManagedViewService
from featurebyte.service.materialized_table import BaseMaterializedTableService
from featurebyte.service.preview import PreviewService
from featurebyte.service.session_manager import SessionManagerService
from featurebyte.service.target import TargetService
from featurebyte.service.target_namespace import TargetNamespaceService
from featurebyte.service.treatment import TreatmentService
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
    percentage_missing: float


@dataclass
class EntityColumnStats:
    """
    Entity column stats
    """

    entity_id: ObjectId
    column_name: str
    count: int
    percentage_missing: float


@dataclass
class ObservationTableStats:
    """
    Statistics of special columns in observation table
    """

    point_in_time_stats: PointInTimeStats
    entity_columns_stats: List[EntityColumnStats]

    @property
    def column_name_to_count(self) -> dict[str, int]:
        """
        Returns a mapping from entity column name to count

        Returns
        -------
        dict[str, int]
        """
        return {
            entity_stat.column_name: entity_stat.count for entity_stat in self.entity_columns_stats
        }

    def get_columns_with_missing_values(
        self, entity_ids_to_check: Optional[list[PydanticObjectId]]
    ) -> list[str]:
        """
        Return list of column names with missing values

        Parameters
        ----------
        entity_ids_to_check: Optional[list[PydanticObjectId]]
            If provided, only check for missing values in these entities

        Returns
        -------
        list[str]
        """
        out = []
        if self.point_in_time_stats.percentage_missing > 0:
            out.append(SpecialColumnName.POINT_IN_TIME.value)
        for entity_stats in self.entity_columns_stats:
            if (
                entity_ids_to_check is not None
                and entity_stats.entity_id not in entity_ids_to_check
            ):
                continue
            if entity_stats.percentage_missing > 0:
                out.append(entity_stats.column_name)
        return out


def _convert_ts_to_str(timestamp_str: str) -> str:
    timestamp_obj = pd.Timestamp(timestamp_str)
    if timestamp_obj.tzinfo is not None:
        timestamp_obj = timestamp_obj.tz_convert("UTC").tz_localize(None)
    return cast(str, timestamp_obj.isoformat())


def validate_columns_info(
    columns_info: List[ColumnSpecWithEntityId],
    primary_entity_ids: Optional[List[PydanticObjectId]] = None,
    skip_entity_validation_checks: bool = False,
    target_namespace: Optional[TargetNamespaceModel] = None,
    treatment: Optional[TreatmentModel] = None,
) -> None:
    """
    Validate column info.

    Parameters
    ----------
    columns_info: List[ColumnSpecWithEntityId]
        List of column specs with entity id
    primary_entity_ids: Optional[List[PydanticObjectId]]
        List of primary entity IDs
    skip_entity_validation_checks: bool
        Whether to skip entity validation checks
    target_namespace: Optional[TargetNamespaceModel]
        Target namespace document
    treatment: Optional[TreatmentModel]
        Treatment document

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
        entity_ids_from_cols = {
            info.entity_id for info in columns_info if info.entity_id is not None
        }

        # Check that there's at least one entity mapped in the columns info
        if len(entity_ids_from_cols) == 0:
            raise ValueError("At least one entity column should be provided.")

        # Check that primary entity IDs passed in are present in the column info
        if primary_entity_ids is not None:
            if not entity_ids_from_cols.issuperset(set(primary_entity_ids)):
                raise ValueError(
                    f"Primary entity IDs passed in are not present in the column info: {primary_entity_ids}"
                )

        # Check that target name is present in the column info
        if target_namespace is not None:
            if target_namespace.name not in columns_info_mapping:
                raise ValueError(f'Target column "{target_namespace.name}" not found.')
            if (
                target_namespace.dtype is not None
                and columns_info_mapping[target_namespace.name].dtype != target_namespace.dtype
            ):
                raise ValueError(
                    f'Target column "{target_namespace.name}" should have dtype "{target_namespace.dtype}"'
                )

        # Check that treatment is present in the column info
        if treatment is not None:
            if treatment.name not in columns_info_mapping:
                raise ValueError(f'Treatment column "{treatment.name}" not found.')
            if (
                treatment.dtype is not None
                and columns_info_mapping[treatment.name].dtype != treatment.dtype
            ):
                raise ValueError(
                    f'Treatment column "{treatment.name}" should have dtype "{treatment.dtype}"'
                )


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
        temp_storage: Storage,
        block_modification_handler: BlockModificationHandler,
        primary_entity_validator: PrimaryEntityValidator,
        use_case_service: UseCaseService,
        storage: Storage,
        redis: Redis[Any],
        target_namespace_service: TargetNamespaceService,
        target_service: TargetService,
        treatment_service: TreatmentService,
        managed_view_service: ManagedViewService,
    ):
        super().__init__(
            user,
            persistent,
            catalog_id,
            session_manager_service,
            feature_store_service,
            entity_service,
            block_modification_handler,
            storage,
            redis,
        )
        self.context_service = context_service
        self.preview_service = preview_service
        self.temp_storage = temp_storage
        self.primary_entity_validator = primary_entity_validator
        self.use_case_service = use_case_service
        self.target_namespace_service = target_namespace_service
        self.target_service = target_service
        self.treatment_service = treatment_service
        self.managed_view_service = managed_view_service

    @property
    def class_name(self) -> str:
        return "ObservationTable"

    async def create_document(self, data: ObservationTableModel) -> ObservationTableModel:
        observation_table = await super().create_document(data)

        # Update default eda and preview tables in the context if applicable
        if observation_table.context_id is not None:
            context = await self.context_service.get_document(
                document_id=observation_table.context_id
            )
            if observation_table.purpose == Purpose.EDA:
                if (
                    context.default_eda_table_id is None
                    and observation_table.treatment_id == context.treatment_id
                ):
                    # context does not have default EDA table set, set it to this table
                    await self.context_service.update_document(
                        document_id=context.id,
                        data=ContextUpdate(default_eda_table_id=observation_table.id),
                    )
            if observation_table.purpose == Purpose.PREVIEW:
                if context.default_preview_table_id is None:
                    # context does not have default preview table set, set it to this table
                    await self.context_service.update_document(
                        document_id=context.id,
                        data=ContextUpdate(default_preview_table_id=observation_table.id),
                    )

        # Update default eda and preview tables in the use case if applicable
        if observation_table.purpose == Purpose.EDA:
            for use_case_id in observation_table.use_case_ids:
                use_case = await self.use_case_service.get_document(document_id=use_case_id)
                if use_case.default_eda_table_id is None:
                    # use case does not have default EDA table set, set it to this table
                    if observation_table.treatment_id:
                        context = await self.context_service.get_document(
                            document_id=use_case.context_id
                        )
                        if observation_table.treatment_id != context.treatment_id:
                            continue
                    if observation_table.target_namespace_id == use_case.target_namespace_id:
                        await self.use_case_service.update_use_case(
                            document_id=use_case_id,
                            data=UseCaseUpdate(default_eda_table_id=observation_table.id),
                        )
        if observation_table.purpose == Purpose.PREVIEW:
            for use_case_id in observation_table.use_case_ids:
                use_case = await self.use_case_service.get_document(document_id=use_case_id)
                if use_case.default_preview_table_id is None:
                    # use case does not have default preview table set, set it to this table
                    await self.use_case_service.update_use_case(
                        document_id=use_case_id,
                        data=UseCaseUpdate(default_preview_table_id=observation_table.id),
                    )

        return observation_table

    async def _validate_context_use_case(
        self,
        data: Union[ObservationTableCreate, ObservationTableUpload],
        target_namespace_id: Optional[ObjectId] = None,
        treatment_id: Optional[ObjectId] = None,
    ) -> None:
        """
        Validate that the context and use case are compatible.

        Parameters
        ----------
        data: Union[ObservationTableCreate, ObservationTableUpload]
            Data to validate
        target_namespace_id: Optional[ObjectId]
            Target namespace ID, if applicable
        treatment_id: Optional[ObjectId]
            Treatment ID, if applicable

        Raises
        ------
        ObservationTableInvalidContextError
            If the context is not valid.
        ObservationTableInvalidUseCaseError
            If the use case is not valid.
        """
        use_case: Optional[UseCaseModel] = None

        entity_id_to_name_mapping = {}
        async for entity in self.entity_service.list_documents_as_dict_iterator(
            projection={"_id": 1, "name": 1}
        ):
            entity_id_to_name_mapping[entity["_id"]] = entity["name"]

        def _get_entity_names(entity_ids: List[PydanticObjectId]) -> str:
            return ", ".join([
                entity_id_to_name_mapping.get(entity_id, "Unknown Entity")
                for entity_id in entity_ids
            ])

        if data.use_case_id is not None:
            # Check if the use case document exists when provided.
            use_case = await self.use_case_service.get_document(document_id=data.use_case_id)
            if data.context_id is not None:
                raise ObservationTableInvalidContextError(
                    "Context should not be specified if use case is specified."
                )
            data.context_id = use_case.context_id

        context: Optional[ContextModel] = None
        if data.context_id is not None:
            # Check if the context document exists when provided. This should perform additional
            # validation once additional information such as request schema are available in the
            # context.
            context = await self.context_service.get_document(document_id=data.context_id)
            if data.primary_entity_ids and data.primary_entity_ids != context.primary_entity_ids:
                raise ObservationTableInvalidContextError(
                    f"Specified primary entity ({_get_entity_names(data.primary_entity_ids)}) does not match "
                    f"context primary entity ({_get_entity_names(context.primary_entity_ids)})."
                )
            data.primary_entity_ids = context.primary_entity_ids

        if treatment_id and context:
            # Check if the treatment matches the context
            if treatment_id != context.treatment_id:
                treatment = await self.treatment_service.get_document(document_id=treatment_id)
                if context.treatment_id:
                    context_treatment = await self.treatment_service.get_document(
                        document_id=context.treatment_id
                    )
                    raise ObservationTableInvalidContextError(
                        f'Treatment "{treatment.name}" does not match context treatment "{context_treatment.name}".'
                    )
                else:
                    raise ObservationTableInvalidContextError(
                        f'Context "{context.name}" is not associated with any treatment. '
                        f'Ensure it is associated with "{treatment.name}".'
                    )

        if target_namespace_id and use_case:
            # Check if the target namespace matches the use case
            if target_namespace_id != use_case.target_namespace_id:
                target_namespace = await self.target_namespace_service.get_document(
                    document_id=target_namespace_id
                )
                use_case_target_namespace = await self.target_namespace_service.get_document(
                    document_id=use_case.target_namespace_id
                )
                raise ObservationTableInvalidUseCaseError(
                    f'Target "{target_namespace.name}" does not match use case target "{use_case_target_namespace.name}".'
                )

    async def _validate_columns(
        self,
        available_columns: List[str],
        primary_entity_ids: Optional[List[PydanticObjectId]],
        target_column: Optional[str],
        treatment_column: Optional[str],
        context: Optional[ContextModel] = None,
        column_dtypes: Optional[Dict[str, DBVarType]] = None,
    ) -> Tuple[Optional[ObjectId], Optional[ObjectId]]:
        # Check if required column names are provided
        missing_columns = []

        # Check if point in time column is present
        if SpecialColumnName.POINT_IN_TIME not in available_columns:
            missing_columns.append(str(SpecialColumnName.POINT_IN_TIME))

        # Check if forecast point column is required and present
        if context is not None and context.forecast_point_schema is not None:
            forecast_point_schema = context.forecast_point_schema
            if SpecialColumnName.FORECAST_POINT not in available_columns:
                raise MissingForecastPointColumnError(
                    f"Forecast point column not provided: {SpecialColumnName.FORECAST_POINT}. "
                    f"This column is required for forecast context '{context.name}'."
                )

            # Validate forecast point column dtype matches schema
            if column_dtypes is not None:
                forecast_point_dtype = column_dtypes.get(SpecialColumnName.FORECAST_POINT)
                expected_dtype = forecast_point_schema.dtype
                if forecast_point_dtype is not None and forecast_point_dtype != expected_dtype:
                    raise UnsupportedForecastPointColumnTypeError(
                        f"Forecast point column has type '{forecast_point_dtype}' but expected "
                        f"'{expected_dtype}' as specified in the forecast point schema for context '{context.name}'."
                    )

            # Check if forecast timezone column is required and present
            if forecast_point_schema.has_timezone_column:
                timezone_column_name = forecast_point_schema.timezone_column_name
                if timezone_column_name and timezone_column_name not in available_columns:
                    raise MissingForecastTimezoneColumnError(
                        f"Forecast timezone column not provided: {timezone_column_name}. "
                        f"This column is required by the forecast point schema for context '{context.name}'."
                    )

                # Validate timezone column dtype is VARCHAR
                if column_dtypes is not None and timezone_column_name:
                    timezone_dtype = column_dtypes.get(timezone_column_name)
                    if timezone_dtype is not None and timezone_dtype != DBVarType.VARCHAR:
                        raise InvalidForecastTimezoneColumnTypeError(
                            f"Forecast timezone column '{timezone_column_name}' has type '{timezone_dtype}' "
                            f"but expected 'VARCHAR' for context '{context.name}'."
                        )

        # Check if entity columns are present
        if primary_entity_ids:
            for entity_id in primary_entity_ids:
                entity = await self.entity_service.get_document(document_id=entity_id)
                if not set(entity.serving_names).intersection(available_columns):
                    missing_columns.append("/".join(entity.serving_names))

        # Check if target column is present
        if target_column:
            if target_column not in available_columns:
                missing_columns.append(target_column)
            target_namespaces = await self.target_namespace_service.list_documents_as_dict(
                query_filter={"name": target_column}
            )
            if target_namespaces["total"] == 0:
                raise ObservationTableInvalidTargetNameError(
                    f"Target name not found: {target_column}"
                )

            # validate target namespace has same primary entity ids
            target_namespace = target_namespaces["data"][0]
            target_namespace_id = ObjectId(target_namespace["_id"])
            if primary_entity_ids:
                if target_namespace["entity_ids"] != primary_entity_ids:
                    raise ObservationTableInvalidTargetNameError(
                        f'Target "{target_column}" does not have matching primary entity ids.'
                    )

            # check if target namespace already has a definition
            if target_namespace["target_ids"]:
                raise ObservationTableTargetDefinitionExistsError(
                    f'Target "{target_column}" already has a definition.'
                )

        else:
            target_namespace_id = None

        # Check if treatment column is present
        if treatment_column:
            if treatment_column not in available_columns:
                missing_columns.append(treatment_column)
            treatments = await self.treatment_service.list_documents_as_dict(
                query_filter={"name": treatment_column}
            )
            if treatments["total"] == 0:
                raise ObservationTableInvalidTreatmentNameError(
                    f"Treatment name not found: {treatment_column}"
                )
            treatment = treatments["data"][0]
            treatment_id = ObjectId(treatment["_id"])
        else:
            treatment_id = None

        if missing_columns:
            raise ObservationTableMissingColumnsError(
                f"Required column(s) not found: {', '.join(missing_columns)}"
            )

        return target_namespace_id, treatment_id

    async def get_observation_table_task_payload(
        self, data: ObservationTableCreate, to_add_row_index: bool = True
    ) -> ObservationTableTaskPayload:
        """
        Validate and convert a ObservationTableCreate schema to a ObservationTableTaskPayload schema
        which will be used to initiate the ObservationTable creation task.

        Parameters
        ----------
        data: ObservationTableCreate
            ObservationTable creation payload
        to_add_row_index: bool
            Whether to add row index to the observation table

        Raises
        ------
        ObservationTableInvalidSamplingError
            If the sampling rate per target value is provided but the target namespace is not available
            or target is not of classification type.

        Returns
        -------
        ObservationTableTaskPayload
        """

        # Check any conflict with existing documents
        output_document_id = data.id or ObjectId()
        await self._check_document_unique_constraints(
            document=FeatureByteBaseDocumentModel(_id=output_document_id, name=data.name),
        )

        if isinstance(data.request_input, ObservationTableObservationInput):
            source_observation_table = await self.get_document(
                document_id=data.request_input.observation_table_id
            )
            # set primary entity and target namespace ids in the payload using values from source observation table
            data.primary_entity_ids = source_observation_table.primary_entity_ids
            target_namespace_id = source_observation_table.target_namespace_id
            treatment_id = source_observation_table.treatment_id
            # context and use case ids will be populated in the task and can be set to None in the payload
            data.context_id = None
            data.use_case_id = None
            # no need to perform entity validation checks since we are copying from existing observation table
            data.skip_entity_validation_checks = True

            if (
                data.request_input.downsampling_info is not None
                and data.request_input.downsampling_info.sampling_rate_per_target_value
            ):
                # do not allow both sampling by target value and sample rows
                if data.sample_rows is not None:
                    raise ObservationTableInvalidSamplingError(
                        "Downsampling by both target value and sample rows is not supported."
                    )

                # ensure target namespace is available if sampling rate per target value is provided
                if target_namespace_id is None:
                    raise ObservationTableInvalidSamplingError(
                        "Downsampling by target value requires the source observation table to have a target column."
                    )

                # ensure target is of classification type
                if target_namespace_id is not None:
                    target_namespace = await self.target_namespace_service.get_document(
                        document_id=target_namespace_id
                    )
                    if target_namespace.target_type not in TargetType.classification_types():
                        raise ObservationTableInvalidSamplingError(
                            "Downsampling by target value is only supported for classification targets."
                        )

        elif isinstance(data.request_input, BaseRequestInput):
            request_input = data.request_input
            if isinstance(data.request_input, ManagedViewObservationInput):
                # for managed view, convert to source table request input for column validation
                managed_view = await self.managed_view_service.get_document(
                    document_id=data.request_input.managed_view_id
                )
                request_input = SourceTableObservationInput(
                    source=managed_view.tabular_source,
                    **data.request_input.model_dump(by_alias=True, exclude={"type"}),
                )

            feature_store = await self.feature_store_service.get_document(
                document_id=data.feature_store_id
            )
            db_session = await self.session_manager_service.get_feature_store_session(feature_store)
            # validate columns
            column_names_and_dtypes = await request_input.get_column_names_and_dtypes(db_session)
            available_columns = list(column_names_and_dtypes.keys())
            column_dtypes: Dict[str, DBVarType] = dict(column_names_and_dtypes)
            columns_rename_mapping = data.request_input.columns_rename_mapping
            if columns_rename_mapping:
                available_columns = [
                    columns_rename_mapping.get(col, col) for col in available_columns
                ]
                # Apply rename mapping to dtypes dict
                column_dtypes = {
                    columns_rename_mapping.get(col, col): dtype
                    for col, dtype in column_dtypes.items()
                }

            # Fetch context for forecast point validation
            context: Optional[ContextModel] = None
            context_id = data.context_id
            if data.use_case_id is not None:
                use_case = await self.use_case_service.get_document(document_id=data.use_case_id)
                context_id = use_case.context_id
            if context_id is not None:
                context = await self.context_service.get_document(document_id=context_id)

            target_namespace_id, treatment_id = await self._validate_columns(
                available_columns=available_columns,
                primary_entity_ids=data.primary_entity_ids,
                target_column=data.target_column,
                treatment_column=data.treatment_column,
                context=context,
                column_dtypes=column_dtypes,
            )
        elif (
            isinstance(data.request_input, TargetInput) and data.request_input.target_id is not None
        ):
            target = await self.target_service.get_document(
                document_id=data.request_input.target_id
            )
            target_namespace_id = target.target_namespace_id
            if data.request_input.observation_table_id is not None:
                observation_table_input = await self.get_document(
                    data.request_input.observation_table_id
                )
                treatment_id = observation_table_input.treatment_id
            else:
                treatment_id = None
        else:
            target_namespace_id = None
            treatment_id = None

        # Validate context and use case
        await self._validate_context_use_case(data, target_namespace_id, treatment_id)

        return ObservationTableTaskPayload(
            **data.model_dump(by_alias=True),
            to_add_row_index=to_add_row_index,
            user_id=self.user.id,
            catalog_id=self.catalog_id,
            output_document_id=output_document_id,
            target_namespace_id=target_namespace_id,
            treatment_id=treatment_id,
        )

    async def get_observation_table_upload_task_payload(
        self,
        data: ObservationTableUpload,
        observation_set_dataframe: pd.DataFrame,
        file_format: UploadFileFormat,
        uploaded_file_name: str,
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
        file_format: UploadFileFormat
            File format of the uploaded file
        uploaded_file_name: str
            Name of the uploaded file

        Returns
        -------
        ObservationTableUploadTaskPayload
        """

        # Check any conflict with existing documents
        output_document_id = data.id or ObjectId()
        await self._check_document_unique_constraints(
            document=FeatureByteBaseDocumentModel(_id=output_document_id, name=data.name),
        )

        # Fetch context for forecast point validation
        context: Optional[ContextModel] = None
        context_id = data.context_id
        if data.use_case_id is not None:
            use_case = await self.use_case_service.get_document(document_id=data.use_case_id)
            context_id = use_case.context_id
        if context_id is not None:
            context = await self.context_service.get_document(document_id=context_id)

        # Check if required column names are provided
        target_namespace_id, treatment_id = await self._validate_columns(
            available_columns=observation_set_dataframe.columns.tolist(),
            primary_entity_ids=data.primary_entity_ids,
            target_column=data.target_column,
            treatment_column=data.treatment_column,
            context=context,
        )

        # Validate context and use case
        await self._validate_context_use_case(data, target_namespace_id, treatment_id)

        # Persist dataframe to parquet file that can be read by the task later
        observation_set_storage_path = f"observation_table/{output_document_id}.parquet"
        await self.temp_storage.put_dataframe(
            observation_set_dataframe, Path(observation_set_storage_path)
        )

        return ObservationTableUploadTaskPayload(
            **data.model_dump(by_alias=True),
            user_id=self.user.id,
            catalog_id=self.catalog_id,
            output_document_id=output_document_id,
            observation_set_storage_path=observation_set_storage_path,
            file_format=file_format,
            uploaded_file_name=uploaded_file_name,
            target_namespace_id=target_namespace_id,
            treatment_id=treatment_id,
        )

    @staticmethod
    def get_minimum_iet_sql_expr(
        entity_column_names: List[str], table_details: TableDetails, adapter: BaseAdapter
    ) -> Expression:
        """
        Get the SQL expression to compute the minimum interval in seconds for each entity.

        Parameters
        ----------
        entity_column_names: List[str]
            List of entity column names
        table_details: TableDetails
            Table details of the materialized table
        adapter: BaseAdapter
            Instance of the SQL adapter

        Returns
        -------
        str
        """
        point_in_time_quoted = quoted_identifier(SpecialColumnName.POINT_IN_TIME)
        previous_point_in_time_quoted = quoted_identifier("PREVIOUS_POINT_IN_TIME")

        window_expression = expressions.Window(
            this=expressions.Anonymous(this="LAG", expressions=[point_in_time_quoted]),
            partition_by=[quoted_identifier(col_name) for col_name in entity_column_names],
            order=expressions.Order(expressions=[expressions.Ordered(this=point_in_time_quoted)]),
        )
        aliased_window = expressions.Alias(
            this=window_expression,
            alias=previous_point_in_time_quoted,
        )
        inner_query = expressions.select(aliased_window, point_in_time_quoted).from_(
            get_fully_qualified_table_name(table_details.model_dump())
        )

        datediff_expr = adapter.datediff_microsecond(
            previous_point_in_time_quoted, point_in_time_quoted
        )
        # Convert microseconds to seconds
        quoted_interval_identifier = quoted_identifier("INTERVAL")
        interval_secs_expr = expressions.Div(
            this=datediff_expr, expression=make_literal_value(1000000)
        )
        difference_expr = expressions.Alias(
            this=interval_secs_expr, alias=quoted_interval_identifier
        )
        iet_expr = expressions.select(difference_expr).from_(inner_query.subquery())
        aliased_min = expressions.Alias(
            this=expressions.Min(this=quoted_interval_identifier),
            alias=quoted_identifier("MIN_INTERVAL"),
        )
        return (
            expressions.select(aliased_min)
            .from_(iet_expr.subquery())
            .where(
                expressions.Not(
                    this=expressions.Is(
                        this=quoted_interval_identifier, expression=expressions.Null()
                    )
                )
            )
        )

    async def _get_min_interval_secs_between_entities(
        self,
        db_session: BaseSession,
        columns_info: List[ColumnSpecWithEntityId],
        table_details: TableDetails,
    ) -> Optional[float]:
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
        Optional[float]
            minimum interval in seconds, None if there's only one row
        """
        entity_col_names = [col.name for col in columns_info if col.entity_id is not None]
        # Construct SQL
        sql_expr = self.get_minimum_iet_sql_expr(
            entity_col_names, table_details, db_session.adapter
        )

        # Execute SQL
        sql_string = sql_to_string(sql_expr, db_session.source_type)
        min_interval_df = await db_session.execute_query_long_running(sql_string)
        assert min_interval_df is not None
        value = min_interval_df.iloc[0]["MIN_INTERVAL"]
        if value is None or pd.isna(value):
            return None
        return float(value)

    async def _get_observation_table_stats(
        self,
        feature_store: FeatureStoreModel,
        table_details: TableDetails,
        columns_info: List[ColumnSpecWithEntityId],
    ) -> ObservationTableStats:
        """
        Get statistics of point in time and entity columns

        Extract information such as entity column name to unique entity count mapping, point in time
        stats, and missing value percentage for validation.

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
        ObservationTableStats
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
            graph=QueryGraph(**graph.model_dump(by_alias=True)),
            node_name=node.name,
            stats_names=["unique", "max", "min", "%missing"],
            feature_store_id=feature_store.id,
            enable_query_cache=False,  # query is one off and not expected to be reused
        )
        describe_stats_dataframe = await self.preview_service.describe(sample, 0, 1234)
        entity_columns_stats = []
        for col in columns_info:
            if col.entity_id is None:
                continue
            entity_columns_stats.append(
                EntityColumnStats(
                    entity_id=col.entity_id,
                    column_name=col.name,
                    count=describe_stats_dataframe.loc["unique", col.name],
                    percentage_missing=describe_stats_dataframe.loc["%missing", col.name],
                )
            )
        least_recent_time_str = describe_stats_dataframe.loc["min", SpecialColumnName.POINT_IN_TIME]
        least_recent_time_str = _convert_ts_to_str(least_recent_time_str)
        most_recent_time_str = describe_stats_dataframe.loc["max", SpecialColumnName.POINT_IN_TIME]
        most_recent_time_str = _convert_ts_to_str(most_recent_time_str)
        percentage_missing = describe_stats_dataframe.loc[
            "%missing", SpecialColumnName.POINT_IN_TIME
        ]
        point_in_time_stats = PointInTimeStats(
            least_recent=least_recent_time_str,
            most_recent=most_recent_time_str,
            percentage_missing=percentage_missing,
        )
        return ObservationTableStats(
            point_in_time_stats=point_in_time_stats,
            entity_columns_stats=entity_columns_stats,
        )

    async def _validate_forecast_timezone_values(
        self,
        db_session: BaseSession,
        table_details: TableDetails,
        context: ContextModel,
    ) -> None:
        """
        Validate timezone column values against the forecast point schema.

        Parameters
        ----------
        db_session: BaseSession
            Database session
        table_details: TableDetails
            Table details of the materialized table
        context: ContextModel
            Context with forecast point schema

        Raises
        ------
        InvalidForecastTimezoneValueError
            If any timezone value is invalid
        """
        forecast_point_schema = context.forecast_point_schema
        if forecast_point_schema is None or not forecast_point_schema.has_timezone_column:
            return

        timezone_column = forecast_point_schema.timezone
        assert isinstance(timezone_column, TimeZoneColumn)
        timezone_column_name = timezone_column.column_name
        timezone_type = timezone_column.type

        # Query distinct timezone values from the table
        query = sql_to_string(
            expressions.select(
                expressions.Distinct(expressions=[quoted_identifier(timezone_column_name)])
            ).from_(get_fully_qualified_table_name(table_details.model_dump())),
            db_session.source_type,
        )
        result_df = await db_session.execute_query(query)
        if result_df is None or result_df.empty:
            return

        # Validate each timezone value
        invalid_values = []
        for value in result_df[timezone_column_name].dropna().unique():
            value_str = str(value)
            try:
                if timezone_type == "timezone":
                    # Validate IANA timezone name
                    pytz.timezone(value_str)
                elif timezone_type == "offset":
                    # Validate UTC offset format
                    validate_timezone_offset_string(value_str)
            except (pytz.UnknownTimeZoneError, ValueError):
                invalid_values.append(value_str)

        if invalid_values:
            expected_format = (
                "IANA timezone names (e.g., 'America/New_York')"
                if timezone_type == "timezone"
                else "UTC offsets (e.g., '+05:30', '-03:00')"
            )
            raise InvalidForecastTimezoneValueError(
                f"Forecast timezone column '{timezone_column_name}' contains invalid values: "
                f"{invalid_values[:5]}{'...' if len(invalid_values) > 5 else ''}. "
                f"Expected {expected_format} for context '{context.name}'."
            )

    async def validate_materialized_table_and_get_metadata(
        self,
        db_session: BaseSession,
        table_details: TableDetails,
        feature_store: FeatureStoreModel,
        serving_names_remapping: Optional[Dict[str, str]] = None,
        skip_entity_validation_checks: bool = False,
        primary_entity_ids: Optional[List[PydanticObjectId]] = None,
        target_namespace_id: Optional[PydanticObjectId] = None,
        treatment_id: Optional[PydanticObjectId] = None,
        context_id: Optional[PydanticObjectId] = None,
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
        target_namespace_id: Optional[PydanticObjectId]
            Target namespace ID
        treatment_id: Optional[PydanticObjectId]
            Treatment ID
        context_id: Optional[PydanticObjectId]
            Context ID for forecast point schema validation

        Returns
        -------
        dict[str, Any]

        Raises
        ------
        ValueError
            If the observation table fails any of the validation
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

        # Validate forecast timezone values if context has forecast point schema
        if context_id is not None:
            context = await self.context_service.get_document(document_id=context_id)
            await self._validate_forecast_timezone_values(db_session, table_details, context)

        # Perform validation on column info
        if target_namespace_id is not None:
            target_namespace = await self.target_namespace_service.get_document(
                document_id=target_namespace_id
            )
        else:
            target_namespace = None

        if treatment_id is not None:
            treatment = await self.treatment_service.get_document(document_id=treatment_id)
        else:
            treatment = None

        validate_columns_info(
            columns_info,
            primary_entity_ids=primary_entity_ids,
            skip_entity_validation_checks=skip_entity_validation_checks,
            target_namespace=target_namespace,
            treatment=treatment,
        )
        # Get entity column name to minimum interval mapping
        min_interval_secs_between_entities = await self._get_min_interval_secs_between_entities(
            db_session, columns_info, table_details
        )
        # Get entity statistics metadata
        observation_table_stats = await self._get_observation_table_stats(
            feature_store, table_details, columns_info
        )
        point_in_time_stats = observation_table_stats.point_in_time_stats
        columns_with_missing_values = observation_table_stats.get_columns_with_missing_values(
            primary_entity_ids
        )
        if columns_with_missing_values:
            raise ValueError(
                "These columns in the observation table must not contain any missing values: "
                f"{', '.join(columns_with_missing_values)}"
            )

        return {
            "columns_info": columns_info,
            "num_rows": num_rows,
            "most_recent_point_in_time": point_in_time_stats.most_recent,
            "least_recent_point_in_time": point_in_time_stats.least_recent,
            "entity_column_name_to_count": observation_table_stats.column_name_to_count,
            "min_interval_secs_between_entities": min_interval_secs_between_entities,
        }

    async def update_observation_table(
        self, observation_table_id: ObjectId, data: ObservationTableServiceUpdate
    ) -> Optional[ObservationTableModel]:
        """
        Update ObservationTable

        Parameters
        ----------
        observation_table_id: ObjectId
            ObservationTable document_id
        data: ObservationTableServiceUpdate
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
                        f"Cannot add UseCase {data.use_case_id_to_add} due to mismatched contexts."
                    )

                if (
                    isinstance(observation_table.request_input, TargetInput)
                    and observation_table.request_input.target_id != use_case.target_id
                ):
                    raise ObservationTableInvalidUseCaseError(
                        f"Cannot add UseCase {data.use_case_id_to_add} due to mismatched targets."
                    )

                if (
                    observation_table.target_namespace_id
                    and use_case.target_namespace_id != observation_table.target_namespace_id
                ):
                    raise ObservationTableInvalidUseCaseError(
                        f"Cannot add UseCase {data.use_case_id_to_add} due to mismatched targets."
                    )

                if observation_table.treatment_id:
                    context = await self.context_service.get_document(
                        document_id=use_case.context_id
                    )
                    if context.treatment_id != observation_table.treatment_id:
                        raise ObservationTableInvalidUseCaseError(
                            f"Cannot add UseCase {data.use_case_id_to_add} due to mismatched treatments."
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
            # validate and replace context_id
            new_context = await self.context_service.get_document(document_id=data.context_id)

            if observation_table.context_id:
                if observation_table.context_id != data.context_id:
                    exist_context = await self.context_service.get_document(
                        document_id=observation_table.context_id
                    )
                    if set(exist_context.primary_entity_ids) != set(new_context.primary_entity_ids):
                        raise ObservationTableInvalidContextError(
                            "Cannot update Context as the entities are different from the existing Context."
                        )
            elif new_context.primary_entity_ids != observation_table.primary_entity_ids:
                raise ObservationTableInvalidContextError(
                    "Cannot update Context as the primary_entity_ids are different from existing primary_entity_ids."
                )
            elif new_context.treatment_id != observation_table.treatment_id:
                raise ObservationTableInvalidContextError(
                    "Cannot update Context as the treatments are different."
                )

        if data.context_id_to_remove:
            # validate and replace context_id
            updated_count = await self.update_documents(
                query_filter={"_id": observation_table_id, "context_id": data.context_id_to_remove},
                update={"$set": {"context_id": None}},
            )
            if updated_count == 0:
                raise ObservationTableInvalidContextError(
                    f"Cannot remove Context {data.context_id_to_remove} as it is not associated with ObservationTable {observation_table_id}."
                )

        result: Optional[ObservationTableModel] = await self.update_document(
            document_id=observation_table_id, data=data, return_document=True
        )

        return result
