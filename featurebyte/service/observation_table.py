"""
ObservationTableService class
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from pathlib import Path

import pandas as pd
from bson import ObjectId

from featurebyte.enum import (
    DBVarType,
    MaterializedTableNamePrefix,
    SpecialColumnName,
    UploadFileFormat,
)
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
from featurebyte.query_graph.node.schema import TableDetails
from featurebyte.routes.block_modification_handler import BlockModificationHandler
from featurebyte.routes.common.primary_entity_validator import PrimaryEntityValidator
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
from featurebyte.service.materialized_table_metadata_extractor import (
    MaterializedTableMetadataExtractor,
)
from featurebyte.service.preview import PreviewService
from featurebyte.service.session_manager import SessionManagerService
from featurebyte.service.use_case import UseCaseService
from featurebyte.session.base import BaseSession
from featurebyte.storage import Storage


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
        materialized_table_metadata_extractor: MaterializedTableMetadataExtractor,
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
        self.materialized_table_metadata_extractor = materialized_table_metadata_extractor

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
        file_format: UploadFileFormat,
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
            file_format=file_format,
        )

    # TODO: probably move this out for reuse with historical_feature_table
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
        min_interval_secs_between_entities = (
            await self.materialized_table_metadata_extractor.get_min_interval_secs_between_entities(
                db_session, columns_info, table_details
            )
        )
        # Get entity statistics metadata
        (
            column_name_to_count,
            point_in_time_stats,
        ) = await self.materialized_table_metadata_extractor.get_column_name_to_entity_count(
            feature_store, table_details, columns_info
        )

        return {
            "columns_info": columns_info,
            "num_rows": num_rows,
            "most_recent_point_in_time": point_in_time_stats.most_recent,
            "least_recent_point_in_time": point_in_time_stats.least_recent,
            "entity_column_name_to_count": column_name_to_count,
            "min_interval_secs_between_entities": min_interval_secs_between_entities,
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
