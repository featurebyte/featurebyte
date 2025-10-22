"""
ObservationTable creation task
"""

from __future__ import annotations

import copy
from datetime import datetime, timedelta
from typing import Any, Optional

from bson import ObjectId
from dateutil import tz

from featurebyte.enum import InternalName, SpecialColumnName
from featurebyte.logging import get_logger
from featurebyte.models import FeatureStoreModel
from featurebyte.models.observation_table import (
    ManagedViewObservationInput,
    ObservationInput,
    ObservationTableModel,
    ObservationTableObservationInput,
    SourceTableObservationInput,
    TargetInput,
)
from featurebyte.query_graph.model.common_table import TabularSource
from featurebyte.query_graph.node.schema import TableDetails
from featurebyte.query_graph.sql.adapter.base import DownSamplingInfo
from featurebyte.query_graph.sql.common import sql_to_string
from featurebyte.query_graph.sql.feature_historical import (
    HISTORICAL_REQUESTS_POINT_IN_TIME_RECENCY_HOUR,
)
from featurebyte.query_graph.sql.materialisation import get_source_count_expr
from featurebyte.schema.target import ComputeTargetRequest
from featurebyte.schema.worker.task.observation_table import ObservationTableTaskPayload
from featurebyte.service.entity import EntityService
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.service.managed_view import ManagedViewService
from featurebyte.service.observation_table import ObservationTableService
from featurebyte.service.session_manager import SessionManagerService
from featurebyte.service.target import TargetService
from featurebyte.service.target_helper.compute_target import TargetComputer
from featurebyte.service.target_namespace import TargetNamespaceService
from featurebyte.service.task_manager import TaskManager
from featurebyte.service.use_case import UseCaseService
from featurebyte.session.base import BaseSession
from featurebyte.worker.task.base import BaseTask
from featurebyte.worker.task.mixin import DataWarehouseMixin

logger = get_logger(__name__)


class ObservationTableTask(DataWarehouseMixin, BaseTask[ObservationTableTaskPayload]):
    """
    ObservationTable Task
    """

    payload_class = ObservationTableTaskPayload

    def __init__(
        self,
        task_manager: TaskManager,
        feature_store_service: FeatureStoreService,
        session_manager_service: SessionManagerService,
        observation_table_service: ObservationTableService,
        managed_view_service: ManagedViewService,
        target_namespace_service: TargetNamespaceService,
        entity_service: EntityService,
        use_case_service: UseCaseService,
        target_service: TargetService,
        target_computer: TargetComputer,
    ):
        super().__init__(task_manager=task_manager)
        self.feature_store_service = feature_store_service
        self.session_manager_service = session_manager_service
        self.observation_table_service = observation_table_service
        self.managed_view_service = managed_view_service
        self.target_namespace_service = target_namespace_service
        self.entity_service = entity_service
        self.use_case_service = use_case_service
        self.target_service = target_service
        self.target_computer = target_computer

    async def get_task_description(self, payload: ObservationTableTaskPayload) -> str:
        return f'Save observation table "{payload.name}" from source table.'

    @staticmethod
    async def get_table_with_missing_data(
        db_session: BaseSession, missing_data_table_details: TableDetails
    ) -> Optional[TableDetails]:
        """
        Get the table with missing data

        Parameters
        ----------
        db_session: BaseSession
            Database session
        missing_data_table_details: TableDetails
            Table details of the table with missing data

        Returns
        -------
        Optional[TableDetails]
            The table with missing data
        """
        # check missing data table whether it has data
        missing_data_table_row_count = await db_session.execute_query_long_running(
            sql_to_string(
                get_source_count_expr(missing_data_table_details),
                db_session.source_type,
            )
        )

        table_with_missing_data = None
        if (
            missing_data_table_row_count is None
            or missing_data_table_row_count.iloc[0]["row_count"] == 0
        ):
            logger.debug(
                "Drop the table with missing data as it has no data",
                extra=missing_data_table_details.model_dump(),
            )

            # drop the table if it has no data & set it to None
            await db_session.drop_table(
                table_name=missing_data_table_details.table_name,
                schema_name=missing_data_table_details.schema_name,  # type: ignore
                database_name=missing_data_table_details.database_name,  # type: ignore
                if_exists=True,
            )
        else:
            table_with_missing_data = missing_data_table_details
        return table_with_missing_data

    async def materialize_request_input_with_target(
        self,
        feature_store: FeatureStoreModel,
        db_session: BaseSession,
        payload: ObservationTableTaskPayload,
        request_input: ObservationInput,
        location: TabularSource,
        target_id: ObjectId,
        context_id: Optional[ObjectId],
        use_case_ids: list[ObjectId],
        sample_rows: Optional[int],
        sample_from_timestamp: Optional[datetime],
        sample_to_timestamp: Optional[datetime],
        columns_to_exclude_missing_values: list[str],
        missing_data_table_details: TableDetails,
        has_row_index: bool,
    ) -> tuple[ObjectId, bool]:
        """
        Materialize the requested input with target column computed

        Parameters
        ----------
        feature_store: FeatureStoreModel
            Feature store model object
        db_session: BaseSession
            Database session
        payload: ObservationTableTaskPayload
            Observation table task payload
        request_input: ObservationInput
            Observation table task payload
        location: TabularSource
            Location to create observation table
        target_id: ObjectId
            Id of target to compute for the observation table
        context_id: ObjectId
            Id of context to tag the observation table
        use_case_ids: list[ObjectId]
            Id of use cases to tag the observation table
        sample_rows: Optional[int]
            The number of rows to sample. If None, no sampling is performed
        sample_from_timestamp: Optional[datetime]
            The timestamp to sample from
        sample_to_timestamp: Optional[datetime]
            The timestamp to sample to
        columns_to_exclude_missing_values: Optional[List[str]
            The columns to exclude missing values from
        missing_data_table_details: TableDetails
            Table details of the table with missing data
        has_row_index: bool
            Whether the observation table has row index column

        Returns
        -------
        tuple[ObjectId, bool]
            Target namespace id and whether the observation table is created as a view instead of a table
        """

        target = await self.target_service.get_document(document_id=target_id)
        target_namespace_id = target.target_namespace_id
        graph = target.graph
        node_names = [target.node_name]

        # ObservationTableObservationInput does not support materialize
        assert not isinstance(request_input, ObservationTableObservationInput)

        temp_location = copy.deepcopy(location)
        temp_location.table_details.table_name = f"__TEMP_{location.table_details.table_name}"
        async with self.drop_table_on_error(
            db_session=db_session,
            list_of_table_details=[temp_location.table_details, missing_data_table_details],
            payload=payload,
        ):
            # compute observation table without target column first
            await request_input.materialize(
                session=db_session,
                destination=temp_location.table_details,
                sample_rows=sample_rows,
                sample_from_timestamp=sample_from_timestamp,
                sample_to_timestamp=sample_to_timestamp,
                columns_to_exclude_missing_values=columns_to_exclude_missing_values,
                missing_data_table_details=missing_data_table_details,
            )
            try:
                additional_metadata = await self.observation_table_service.validate_materialized_table_and_get_metadata(
                    db_session,
                    temp_location.table_details,
                    feature_store=feature_store,
                    skip_entity_validation_checks=payload.skip_entity_validation_checks,
                    primary_entity_ids=payload.primary_entity_ids,
                )

                # compute observation table with target column
                observation_set = ObservationTableModel(
                    user_id=payload.user_id,
                    name=payload.name,
                    location=temp_location,
                    context_id=context_id,
                    use_case_ids=use_case_ids,
                    request_input=payload.request_input,
                    purpose=payload.purpose,
                    primary_entity_ids=payload.primary_entity_ids or [],
                    has_row_index=has_row_index,
                    **additional_metadata,
                )

                result = await self.target_computer.compute(
                    observation_set=observation_set,
                    compute_request=ComputeTargetRequest(
                        feature_store_id=payload.feature_store_id,
                        graph=graph,
                        node_names=node_names,
                        target_id=target_id,
                    ),
                    output_table_details=location.table_details,
                )
                return target_namespace_id, result.is_output_view
            finally:
                # drop temporary table
                await db_session.drop_table(
                    table_name=temp_location.table_details.table_name,
                    schema_name=temp_location.table_details.schema_name or db_session.schema_name,
                    database_name=temp_location.table_details.database_name
                    or db_session.database_name,
                    if_exists=True,
                )

    async def create_observation_table(
        self,
        payload: ObservationTableTaskPayload,
        override_model_params: Optional[dict[str, Any]] = None,
    ) -> None:
        """
        Create an ObservationTable from the given payload

        Parameters
        ----------
        payload: ObservationTableTaskPayload
            ObservationTable creation payload
        override_model_params: Optional[dict[str, Any]]
            Override model parameters
        """
        feature_store = await self.feature_store_service.get_document(
            document_id=payload.feature_store_id
        )
        db_session = await self.session_manager_service.get_feature_store_session(feature_store)
        location = await self.observation_table_service.generate_materialized_table_location(
            payload.feature_store_id,
        )
        missing_data_table_details = location.table_details.model_copy(
            update={"table_name": f"missing_data_{location.table_details.table_name}"}
        )

        # exclude rows with null values in key columns (POINT_IN_TIME, entity columns, target_column)
        columns_to_exclude_missing_values = [str(SpecialColumnName.POINT_IN_TIME)]
        if payload.primary_entity_ids is not None:
            for entity_id in payload.primary_entity_ids:
                entity = await self.entity_service.get_document(document_id=entity_id)
                columns_to_exclude_missing_values.extend(entity.serving_names)
        if payload.target_column is not None:
            columns_to_exclude_missing_values.append(payload.target_column)

        # limit POINT_IN_TIME to a certain recency to avoid historical request failures
        max_timestamp = datetime.utcnow() - timedelta(
            hours=HISTORICAL_REQUESTS_POINT_IN_TIME_RECENCY_HOUR
        )
        if payload.sample_to_timestamp is not None:
            if payload.sample_to_timestamp.tzinfo is not None:
                sample_to_timestamp = payload.sample_to_timestamp.astimezone(tz.UTC).replace(
                    tzinfo=None
                )
            else:
                sample_to_timestamp = payload.sample_to_timestamp

            sample_to_timestamp = min(sample_to_timestamp, max_timestamp)
        else:
            sample_to_timestamp = max_timestamp

        request_input: ObservationInput
        if isinstance(payload.request_input, ObservationTableObservationInput):
            # for observation input, materialize using source table request input without column filters or remapping
            source_observation_table = await self.observation_table_service.get_document(
                document_id=payload.request_input.observation_table_id
            )
            request_input = SourceTableObservationInput(
                source=source_observation_table.location,
            )
            # exclude row index from source table
            column_names_and_dtypes = await request_input.get_column_names_and_dtypes(
                session=db_session
            )
            request_input.columns = [
                column_name
                for column_name in column_names_and_dtypes.keys()
                if column_name not in [InternalName.TABLE_ROW_INDEX]
            ]
            context_id = source_observation_table.context_id
            use_case_ids = source_observation_table.use_case_ids
            sampling_rate_per_target_value = payload.request_input.sampling_rate_per_target_value
            # sample rate column will be inherited from source observation table
            output_table_has_row_weights = source_observation_table.has_row_weights
        else:
            if isinstance(payload.request_input, ManagedViewObservationInput):
                # for managed view input, materialize using source table request input
                managed_view = await self.managed_view_service.get_document(
                    document_id=payload.request_input.managed_view_id
                )
                request_input = SourceTableObservationInput(
                    source=managed_view.tabular_source,
                    **payload.request_input.model_dump(by_alias=True, exclude={"type"}),
                )
            else:
                request_input = payload.request_input
            context_id = payload.context_id
            use_case_ids = [payload.use_case_id] if payload.use_case_id else []
            sampling_rate_per_target_value = None
            output_table_has_row_weights = False

        # if use case is specified and observation table does not contain target,
        # try to compute the target column
        target_id_to_compute = None
        target_namespace_id = payload.target_namespace_id
        if target_namespace_id is None and use_case_ids:
            # check if use case has target definition
            use_case = await self.use_case_service.get_document(document_id=use_case_ids[0])
            if use_case.target_id is not None:
                target_id_to_compute = use_case.target_id

        if target_id_to_compute is not None:
            target_namespace_id, is_view = await self.materialize_request_input_with_target(
                feature_store=feature_store,
                db_session=db_session,
                payload=payload,
                request_input=request_input,
                location=location,
                target_id=target_id_to_compute,
                context_id=context_id,
                use_case_ids=use_case_ids,
                sample_rows=payload.sample_rows,
                sample_from_timestamp=payload.sample_from_timestamp,
                sample_to_timestamp=sample_to_timestamp,
                columns_to_exclude_missing_values=columns_to_exclude_missing_values,
                missing_data_table_details=missing_data_table_details,
                has_row_index=not payload.to_add_row_index,
            )
        else:
            # apply downsampling if target is available and sampling rates are provided
            if target_namespace_id is not None and sampling_rate_per_target_value:
                target_namespace = await self.target_namespace_service.get_document(
                    document_id=target_namespace_id,
                )
                downsampling_info = DownSamplingInfo(
                    target_column=target_namespace.name,
                    sampling_rate_per_target_value=sampling_rate_per_target_value,
                )
                # sample rate column will be added / updated in the output table
                output_table_has_row_weights = True
            else:
                downsampling_info = None

            await request_input.materialize(
                session=db_session,
                destination=location.table_details,
                sample_rows=payload.sample_rows,
                sample_from_timestamp=payload.sample_from_timestamp,
                sample_to_timestamp=sample_to_timestamp,
                downsampling_info=downsampling_info,
                columns_to_exclude_missing_values=columns_to_exclude_missing_values,
                missing_data_table_details=missing_data_table_details,
            )
            is_view = False

        # check if the table has row index column
        if payload.to_add_row_index:
            await self.observation_table_service.add_row_index_column(
                session=db_session, table_details=location.table_details
            )
            if missing_data_table_details:
                await self.observation_table_service.add_row_index_column(
                    session=db_session, table_details=missing_data_table_details
                )

        # get the table with missing data if it has data
        table_with_missing_data = await self.get_table_with_missing_data(
            db_session=db_session,
            missing_data_table_details=missing_data_table_details,
        )

        list_of_table_details = [location.table_details]
        if table_with_missing_data:
            list_of_table_details.append(table_with_missing_data)

        async with self.drop_table_on_error(
            db_session=db_session,
            list_of_table_details=list_of_table_details,
            payload=payload,
        ):
            payload_input = payload.request_input
            assert not isinstance(payload_input, TargetInput)
            additional_metadata = (
                await self.observation_table_service.validate_materialized_table_and_get_metadata(
                    db_session,
                    location.table_details,
                    feature_store=feature_store,
                    skip_entity_validation_checks=payload.skip_entity_validation_checks,
                    primary_entity_ids=payload.primary_entity_ids,
                    target_namespace_id=target_namespace_id,
                )
            )

            logger.debug(
                "Creating a new ObservationTable", extra=location.table_details.model_dump()
            )
            primary_entity_ids = payload.primary_entity_ids or []
            override_model_params = override_model_params or {}
            observation_table_params = {
                "_id": payload.output_document_id,
                "user_id": payload.user_id,
                "name": payload.name,
                "location": location,
                "context_id": context_id,
                "use_case_ids": use_case_ids,
                "request_input": payload.request_input,
                "purpose": payload.purpose,
                "primary_entity_ids": primary_entity_ids,
                "has_row_index": True,
                "has_row_weights": output_table_has_row_weights,
                "target_namespace_id": target_namespace_id,
                "sample_rows": payload.sample_rows,
                "sample_from_timestamp": payload.sample_from_timestamp,
                "sample_to_timestamp": payload.sample_to_timestamp,
                "table_with_missing_data": table_with_missing_data,
                "is_view": is_view,
                **additional_metadata,
                **override_model_params,
            }
            observation_table = ObservationTableModel(**observation_table_params)
            observation_table = await self.observation_table_service.create_document(
                observation_table
            )
            if target_namespace_id:
                # update the target namespace with the unique target values if applicable
                await self.target_namespace_service.update_target_namespace_classification_metadata(
                    target_namespace_id=target_namespace_id,
                    observation_table=observation_table,
                    db_session=db_session,
                )

    async def execute(self, payload: ObservationTableTaskPayload) -> Any:
        await self.create_observation_table(payload)
