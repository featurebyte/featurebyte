"""
SCDDataService class
"""
from __future__ import annotations

from featurebyte.models.scd_data import SCDDataModel
from featurebyte.schema.scd_data import SCDDataCreate, SCDDataUpdate
from featurebyte.service.base_data_document import BaseDataDocumentService


class SCDDataService(BaseDataDocumentService[SCDDataModel, SCDDataCreate, SCDDataUpdate]):
    """
    SCDDataService class
    """

    document_class = SCDDataModel
