"""
TabularData API route controller
"""
from __future__ import annotations

from featurebyte.models.tabular_data import TabularDataModel
from featurebyte.routes.common.base import BaseDocumentController
from featurebyte.schema.tabular_data import TabularDataList
from featurebyte.service.tabular_data import DataService


class TabularDataController(BaseDocumentController[TabularDataModel, DataService, TabularDataList]):
    """
    TabularData controller
    """

    paginated_document_class = TabularDataList
