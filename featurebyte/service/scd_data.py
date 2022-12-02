"""
SCDDataService class
"""
from __future__ import annotations

from featurebyte.models.scd_data import SCDDataModel
from featurebyte.routes.app_container import register_service_constructor
from featurebyte.schema.scd_data import SCDDataCreate, SCDDataUpdate
from featurebyte.service.base_data_document import BaseDataDocumentService


class SCDDataService(BaseDataDocumentService[SCDDataModel, SCDDataCreate, SCDDataUpdate]):
    """
    SCDDataService class
    """

    document_class = SCDDataModel

    @property
    def class_name(self) -> str:
        return "SCDData"


register_service_constructor(SCDDataService)
