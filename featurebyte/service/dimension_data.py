"""
DimensionDataService class
"""
from __future__ import annotations

from featurebyte.models.dimension_data import DimensionDataModel
from featurebyte.routes.app_container import register_service_constructor
from featurebyte.schema.dimension_data import DimensionDataCreate, DimensionDataUpdate
from featurebyte.service.base_data_document import BaseDataDocumentService


class DimensionDataService(
    BaseDataDocumentService[DimensionDataModel, DimensionDataCreate, DimensionDataUpdate]
):
    """
    DimensionDataService class
    """

    document_class = DimensionDataModel


register_service_constructor(DimensionDataService)
