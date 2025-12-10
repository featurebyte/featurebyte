"""
TreatmentService class
"""

from __future__ import annotations

from typing import List, Union

from sqlglot import expressions

from featurebyte.enum import TreatmentType
from featurebyte.exception import DocumentUpdateError
from featurebyte.models.base import PydanticObjectId
from featurebyte.models.observation_table import ObservationTableModel
from featurebyte.models.treatment import TreatmentModel
from featurebyte.query_graph.sql.common import (
    get_fully_qualified_table_name,
    quoted_identifier,
    sql_to_string,
)
from featurebyte.schema.treatment import TreatmentCreate, TreatmentUpdate
from featurebyte.schema.worker.task.treatment_labels_validate import (
    TreatmentLabelsValidateTaskPayload,
)
from featurebyte.service.base_document import BaseDocumentService
from featurebyte.session.base import BaseSession


class TreatmentService(
    BaseDocumentService[TreatmentModel, TreatmentCreate, TreatmentUpdate],
):
    """
    TreatmentService class
    """

    document_class = TreatmentModel

    async def get_treatment_labels_validate_task_payload(
        self,
        treatment_id: PydanticObjectId,
        observation_table_id: PydanticObjectId,
    ) -> TreatmentLabelsValidateTaskPayload:
        """
        Get task payload to validate treatment labeld.

        Parameters
        ----------
        treatment_id: PydanticObjectId
            ID of the treatment to validate
        observation_table_id: PydanticObjectId
            ID of the observation table with treatment values

        Returns
        -------
        TreatmentLabelsValidateTaskPayload
            Task payload for validating treatment labels
        """
        return TreatmentLabelsValidateTaskPayload(
            treatment_id=treatment_id,
            observation_table_id=observation_table_id,
            user_id=self.user.id,
            catalog_id=self.catalog_id,
        )

    @staticmethod
    async def _get_unique_treatment_values(
        observation_table: ObservationTableModel, treatment_name: str, db_session: BaseSession
    ) -> List[Union[str, int]]:
        query = expressions.select(
            expressions.Distinct(expressions=[quoted_identifier(treatment_name)])
        ).from_(
            get_fully_qualified_table_name(observation_table.location.table_details.model_dump())
        )
        query_str = sql_to_string(query, source_type=db_session.source_type)
        results = await db_session.execute_query_long_running(query_str)
        return list(results[treatment_name].tolist()) if results is not None else []

    async def validate_treatment_labels(
        self,
        treatment_id: PydanticObjectId,
        observation_table: ObservationTableModel,
        db_session: BaseSession,
    ) -> None:
        """
        Validate the control label with the treatment unique values.

        Parameters
        ----------
        treatment_id: PydanticObjectId
            ID of the treatment to update
        observation_table: ObservationTableModel
            Newly created observation table with treatment values
        db_session: BaseSession
            Database session to execute queries

        Raises
        ------
        DocumentUpdateError
            If the control label is not found in the treatment values
        """
        treatment = await self.get_document(document_id=treatment_id)
        assert isinstance(treatment, TreatmentModel)
        if treatment.treatment_type in {TreatmentType.BINARY, TreatmentType.MULTI_ARM}:
            assert treatment.name is not None, "Treatment name should not be None"
            assert treatment.treatment_labels is not None, "Treatment labels should not be None"

            unique_treatments = await self._get_unique_treatment_values(
                observation_table=observation_table,
                treatment_name=treatment.name,
                db_session=db_session,
            )

            if set(treatment.treatment_labels) != set(unique_treatments):
                raise DocumentUpdateError(
                    f"Treatment labels {treatment.treatment_labels} are different from treatment values "
                    f"{unique_treatments}. Ensure the control label exists in the observation table."
                )
