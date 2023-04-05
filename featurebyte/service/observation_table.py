"""
ObservationTableService class
"""
from __future__ import annotations

from featurebyte.models.observation_table import ObservationTableModel
from featurebyte.schema.common.base import BaseDocumentServiceUpdateSchema
from featurebyte.schema.observation_table import ObservationTableCreate
from featurebyte.service.base_table_document import BaseDocumentService


class ObservationTableService(
    BaseDocumentService[
        ObservationTableModel, ObservationTableModel, BaseDocumentServiceUpdateSchema
    ]
):
    """
    ObservationTableService class
    """

    document_class = ObservationTableModel

    @property
    def class_name(self) -> str:
        return "ObservationTable"
