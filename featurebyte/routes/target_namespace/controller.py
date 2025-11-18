"""
Target namespace controller
"""

from typing import Any, List, Tuple, cast

from bson import ObjectId

from featurebyte.common.validator import validate_target_type
from featurebyte.enum import TargetType
from featurebyte.exception import DocumentUpdateError
from featurebyte.models.persistent import QueryFilter
from featurebyte.models.target_namespace import TargetNamespaceModel
from featurebyte.routes.common.base import BaseDocumentController
from featurebyte.schema.target_namespace import (
    TargetNamespaceClassificationMetadataUpdate,
    TargetNamespaceCreate,
    TargetNamespaceInfo,
    TargetNamespaceList,
    TargetNamespaceServiceUpdate,
    TargetNamespaceUpdate,
)
from featurebyte.schema.task import Task
from featurebyte.service.observation_table import ObservationTableService
from featurebyte.service.target import TargetService
from featurebyte.service.target_namespace import TargetNamespaceService
from featurebyte.service.task_manager import TaskManager
from featurebyte.service.use_case import UseCaseService


class TargetNamespaceController(
    BaseDocumentController[TargetNamespaceModel, TargetNamespaceService, TargetNamespaceList],
):
    """
    TargetNamespace controller
    """

    paginated_document_class = TargetNamespaceList

    def __init__(
        self,
        target_namespace_service: TargetNamespaceService,
        target_service: TargetService,
        use_case_service: UseCaseService,
        observation_table_service: ObservationTableService,
        task_manager: TaskManager,
    ):
        super().__init__(target_namespace_service)
        self.target_service = target_service
        self.use_case_service = use_case_service
        self.observation_table_service = observation_table_service
        self.task_manager = task_manager

    async def create_target_namespace(
        self,
        data: TargetNamespaceCreate,
    ) -> TargetNamespaceModel:
        """
        Create TargetNamespace at persistent

        Parameters
        ----------
        data: TargetNamespaceCreate
            Target namespace creation payload

        Returns
        -------
        TargetNamespaceModel
            Newly created TargetNamespace object
        """
        return await self.service.create_document(data)

    async def create_target_namespace_classification_metadata_update_task(
        self,
        target_namespace_id: ObjectId,
        data: TargetNamespaceClassificationMetadataUpdate,
    ) -> Task:
        """
        Create a task to update classification metadata for a target namespace.

        Parameters
        ----------
        target_namespace_id: ObjectId
            Target namespace ID
        data: TargetNamespaceClassificationMetadataUpdate
            Data for updating classification metadata

        Returns
        -------
        Task
            Task created to update classification metadata

        Raises
        ------
        DocumentUpdateError
            If the target namespace is not of classification type or does not associate with the observation table
        """
        target_namespace = await self.service.get_document(document_id=target_namespace_id)
        observation_table = await self.observation_table_service.get_document(
            document_id=data.observation_table_id
        )

        if target_namespace.target_type is None:
            raise DocumentUpdateError(
                f"Target namespace [{target_namespace.name}] has not been set to classification type."
            )
        elif target_namespace.target_type != TargetType.CLASSIFICATION:
            raise DocumentUpdateError(
                f"Target namespace [{target_namespace.name}] is not of classification type, "
                f"it is {target_namespace.target_type} type."
            )

        if observation_table.target_namespace_id is None:
            raise DocumentUpdateError(
                f"Observation table [{observation_table.name}] does not associate with any target namespace."
            )
        elif observation_table.target_namespace_id != target_namespace.id:
            raise DocumentUpdateError(
                f"Observation table [{observation_table.name}] does not associate with "
                f"target namespace [{target_namespace.name}]."
            )

        task_payload = (
            await self.service.get_target_namespace_classification_metadata_update_task_payload(
                target_namespace_id=target_namespace.id,
                observation_table_id=observation_table.id,
            )
        )
        task_id = await self.task_manager.submit(payload=task_payload)
        task = await self.task_manager.get_task(task_id=task_id)
        assert task is not None, "Task should not be None"
        return task

    async def service_and_query_pairs_for_checking_reference(
        self, document_id: ObjectId
    ) -> List[Tuple[Any, QueryFilter]]:
        return [
            (self.target_service, {"target_namespace_id": document_id}),
            (self.use_case_service, {"target_namespace_id": document_id}),
            (self.observation_table_service, {"target_namespace_id": document_id}),
        ]

    async def get_info(self, document_id: ObjectId, verbose: bool) -> TargetNamespaceInfo:
        """
        Get target namespace info given document_id

        Parameters
        ----------
        document_id: ObjectId
            Document ID
        verbose: bool
            Flag to control verbose level

        Returns
        -------
        TargetNamespaceInfo
        """
        _ = verbose
        target_namespace = await self.service.get_document(document_id=document_id)
        return TargetNamespaceInfo(
            name=target_namespace.name,
            default_version_mode=target_namespace.default_version_mode,
            default_target_id=target_namespace.default_target_id,
            target_type=target_namespace.target_type,
            created_at=target_namespace.created_at,
            updated_at=target_namespace.updated_at,
        )

    async def _validate_positive_label_immutability(
        self, target_namespace: TargetNamespaceModel
    ) -> None:
        """
        Validate that positive label is immutable once set.

        Parameters
        ----------
        target_namespace: TargetNamespaceModel
            Target namespace model

        Raises
        ------
        DocumentUpdateError
            If positive label is already set
        """
        # check whether positive label has been used in any observation table
        related_obs_table_res = await self.observation_table_service.list_documents_as_dict(
            query_filter={"target_namespace_id": target_namespace.id}
        )
        if related_obs_table_res["total"] == 0:
            # no observation table is associated with this target namespace yet,
            # positive label can still be updated
            return

        if target_namespace.positive_label is not None:
            raise DocumentUpdateError(
                "Positive label is immutable and cannot be updated once set. "
                f"Current positive label value: {target_namespace.positive_label}."
            )

    async def _validate_positive_label_candidate(
        self,
        target_namespace: TargetNamespaceModel,
        data: TargetNamespaceUpdate,
    ) -> Any:
        """
        Validate and extract positive label value from candidates.

        Parameters
        ----------
        target_namespace: TargetNamespaceModel
            Target namespace model
        data: TargetNamespaceUpdate
            Update payload containing positive label information

        Returns
        -------
        Any
            Validated positive label value (str, float, or bool)

        Raises
        ------
        DocumentUpdateError
            If no matching candidate found or value is not in valid candidates
        """
        assert data.positive_label is not None

        if data.positive_label.observation_table_id:
            # validate against specific observation table candidates

            matched_candidate = None
            for candidate in target_namespace.positive_label_candidates:
                if candidate.observation_table_id == data.positive_label.observation_table_id:
                    matched_candidate = candidate
                    break

            if matched_candidate is None:
                raise DocumentUpdateError(
                    "Please run target namespace classification metadata update task "
                    "to extract positive label candidates before setting the positive label."
                )

            if data.positive_label.value not in matched_candidate.positive_label_candidates:
                raise DocumentUpdateError(
                    f'Value "{data.positive_label.value}" is not a valid candidate for '
                    f"observation table (ID: {matched_candidate.observation_table_id}). "
                    f"Valid candidates are: {matched_candidate.positive_label_candidates}."
                )

        return data.positive_label.value

    async def _validate_and_process_positive_label(
        self,
        target_namespace: TargetNamespaceModel,
        data: TargetNamespaceUpdate,
    ) -> Any:
        """
        Validate and process positive label update.

        This method orchestrates the validation logic for positive label updates.
        Override this method to customize the overall validation flow.

        Parameters
        ----------
        target_namespace: TargetNamespaceModel
            Target namespace model
        data: TargetNamespaceUpdate
            Update payload

        Returns
        -------
        Any
            Validated positive label value (str, float, or bool) or None if not provided
        """
        if data.positive_label is None:
            return None

        # Validate target type
        data.validate_positive_label_target_type(target_namespace)

        # Validate immutability
        await self._validate_positive_label_immutability(target_namespace)

        # Validate and extract positive label from candidates
        pos_label = await self._validate_positive_label_candidate(target_namespace, data)
        return pos_label

    async def update_target_namespace(
        self, target_namespace_id: ObjectId, data: TargetNamespaceUpdate
    ) -> TargetNamespaceModel:
        """
        Update TargetNamespace

        Parameters
        ----------
        target_namespace_id: ObjectId
            TargetNamespace ID
        data: TargetNamespaceUpdate
            TargetNamespace update payload

        Returns
        -------
        TargetNamespaceModel
            Updated TargetNamespace object

        Raises
        ------
        DocumentUpdateError
            If updating target type after setting it is not supported
        """
        target_namespace = await self.service.get_document(document_id=target_namespace_id)
        if (
            data.target_type
            and target_namespace.target_type
            and data.target_type != target_namespace.target_type
        ):
            raise DocumentUpdateError("Updating target type after setting it is not supported.")

        validate_target_type(target_type=data.target_type, dtype=target_namespace.dtype)

        # Validate and process positive label
        positive_label = await self._validate_and_process_positive_label(target_namespace, data)

        data = TargetNamespaceServiceUpdate(**{
            **data.model_dump(by_alias=True, exclude={"positive_label": True}),
            "positive_label": positive_label,
        })
        updated_namespace = await self.service.update_document(
            document_id=target_namespace_id, data=data
        )
        return cast(TargetNamespaceModel, updated_namespace)
