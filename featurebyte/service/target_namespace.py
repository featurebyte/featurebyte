"""
TargetNamespaceService class
"""

from __future__ import annotations

from typing import List, Union

from sqlglot import expressions

from featurebyte.enum import TargetType
from featurebyte.exception import DocumentUpdateError
from featurebyte.models.base import PydanticObjectId
from featurebyte.models.observation_table import ObservationTableModel
from featurebyte.models.target_namespace import PositiveLabelCandidatesItem, TargetNamespaceModel
from featurebyte.query_graph.sql.common import (
    get_fully_qualified_table_name,
    quoted_identifier,
    sql_to_string,
)
from featurebyte.schema.target_namespace import TargetNamespaceCreate, TargetNamespaceServiceUpdate
from featurebyte.schema.worker.task.target_namespace_classification_metadata_update import (
    TargetNamespaceClassificationMetadataUpdateTaskPayload,
)
from featurebyte.service.base_document import BaseDocumentService
from featurebyte.session.base import BaseSession


class TargetNamespaceService(
    BaseDocumentService[TargetNamespaceModel, TargetNamespaceCreate, TargetNamespaceServiceUpdate],
):
    """
    TargetNamespaceService class
    """

    document_class = TargetNamespaceModel

    async def get_target_namespace_classification_metadata_update_task_payload(
        self,
        target_namespace_id: PydanticObjectId,
        observation_table_id: PydanticObjectId,
    ) -> TargetNamespaceClassificationMetadataUpdateTaskPayload:
        """
        Get task payload to update classification metadata for a target namespace.

        Parameters
        ----------
        target_namespace_id: PydanticObjectId
            ID of the target namespace to update
        observation_table_id: PydanticObjectId
            ID of the observation table with target values

        Returns
        -------
        TargetNamespaceClassificationMetadataUpdateTaskPayload
            Task payload for updating classification metadata
        """
        return TargetNamespaceClassificationMetadataUpdateTaskPayload(
            target_namespace_id=target_namespace_id,
            observation_table_id=observation_table_id,
            user_id=self.user.id,
            catalog_id=self.catalog_id,
        )

    async def _add_positive_label_candidate(
        self, document_id: PydanticObjectId, new_candidate: PositiveLabelCandidatesItem
    ) -> None:
        namespace: TargetNamespaceModel = await self.get_document(document_id=document_id)
        updated_candidates = [
            candidate
            if candidate.observation_table_id != new_candidate.observation_table_id
            else new_candidate
            for candidate in namespace.positive_label_candidates
        ]

        if not any(
            candidate.observation_table_id == new_candidate.observation_table_id
            for candidate in namespace.positive_label_candidates
        ):
            updated_candidates.append(new_candidate)

        update_payload = TargetNamespaceServiceUpdate(positive_label_candidates=updated_candidates)
        await self.update_document(document_id=document_id, data=update_payload)

    @staticmethod
    async def _get_unique_target_values(
        observation_table: ObservationTableModel, target_name: str, db_session: BaseSession
    ) -> List[Union[str, int]]:
        query = expressions.select(
            expressions.Distinct(expressions=[quoted_identifier(target_name)])
        ).from_(
            get_fully_qualified_table_name(observation_table.location.table_details.model_dump())
        )
        query_str = sql_to_string(query, source_type=db_session.source_type)
        results = await db_session.execute_query_long_running(query_str)
        return list(results[target_name].tolist()) if results is not None else []

    async def update_target_namespace_classification_metadata(
        self,
        target_namespace_id: PydanticObjectId,
        observation_table: ObservationTableModel,
        db_session: BaseSession,
    ) -> None:
        """
        Update the target namespace with the target unique values when the target type is classification.

        Parameters
        ----------
        target_namespace_id: PydanticObjectId
            ID of the target namespace to update
        observation_table: ObservationTableModel
            Newly created observation table with target values
        db_session: BaseSession
            Database session to execute queries

        Raises
        ------
        DocumentUpdateError
            If the positive label is not found in the target values
        """
        target_namespace = await self.get_document(document_id=target_namespace_id)
        assert isinstance(target_namespace, TargetNamespaceModel)
        if target_namespace.target_type == TargetType.CLASSIFICATION:
            assert target_namespace.name is not None, "Target namespace name should not be None"
            unique_targets = await self._get_unique_target_values(
                observation_table=observation_table,
                target_name=target_namespace.name,
                db_session=db_session,
            )

            if (
                target_namespace.positive_label
                and target_namespace.positive_label not in unique_targets
            ):
                raise DocumentUpdateError(
                    f"Positive label {target_namespace.positive_label} not found in target values "
                    f"{unique_targets}. Please either update the positive label of the target "
                    "or ensure the positive label exists in the observation table."
                )

            positive_label_candidate = PositiveLabelCandidatesItem(
                observation_table_id=observation_table.id,
                positive_label_candidates=unique_targets,
            )

            await self._add_positive_label_candidate(
                document_id=target_namespace_id,
                new_candidate=positive_label_candidate,
            )
