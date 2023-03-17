"""
DimensionDataService class
"""
from __future__ import annotations

from featurebyte.models.dimension_data import DimensionDataModel
from featurebyte.schema.dimension_data import DimensionDataCreate, DimensionDataServiceUpdate
from featurebyte.service.base_data_document import BaseDataDocumentService


class DimensionDataService(
    BaseDataDocumentService[DimensionDataModel, DimensionDataCreate, DimensionDataServiceUpdate]
):
    """
    DimensionDataService class
    """

    document_class = DimensionDataModel
