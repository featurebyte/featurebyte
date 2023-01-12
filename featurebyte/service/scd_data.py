"""
SCDDataService class
"""
from __future__ import annotations

from featurebyte.models.scd_data import SCDDataModel
from featurebyte.schema.scd_data import SCDDataCreate, SCDDataServiceUpdate
from featurebyte.service.base_data_document import BaseDataDocumentService


class SCDDataService(BaseDataDocumentService[SCDDataModel, SCDDataCreate, SCDDataServiceUpdate]):
    """
    SCDDataService class
    """

    document_class = SCDDataModel

    @property
    def class_name(self) -> str:
        return "SCDData"
