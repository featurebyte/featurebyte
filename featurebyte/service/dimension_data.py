"""
DimensionDataService class
"""
from __future__ import annotations

from featurebyte.models.dimension_data import DimensionDataModel
from featurebyte.schema.dimension_data import DimensionDataCreate, DimensionDataUpdate
from featurebyte.service.base_data_document import BaseDataDocumentService


class DimensionDataService(
    BaseDataDocumentService[DimensionDataModel, DimensionDataCreate, DimensionDataUpdate]
):
    """
    DimensionDataService class
    """

    document_class = DimensionDataModel
