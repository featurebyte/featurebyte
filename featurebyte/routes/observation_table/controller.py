"""
ObservationTable API route controller
"""
from __future__ import annotations

from featurebyte.models.observation_table import ObservationTableModel
from featurebyte.routes.common.base import BaseDocumentController
from featurebyte.schema.observation_table import ObservationTableList
from featurebyte.service.observation_table import ObservationTableService


class ObservationTableController(
    BaseDocumentController[ObservationTableModel, ObservationTableService, ObservationTableList],
):
    """
    ObservationTable Controller
    """

    paginated_document_class = ObservationTableList
