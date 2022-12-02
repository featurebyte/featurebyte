"""
SCDData API route controller
"""
from __future__ import annotations

from typing import Any

from featurebyte.enum import SemanticType
from featurebyte.models.scd_data import SCDDataModel
from featurebyte.routes.app_container import register_controller_constructor
from featurebyte.routes.common.base_data import BaseDataDocumentController
from featurebyte.schema.scd_data import SCDDataList, SCDDataUpdate
from featurebyte.service.data_update import DataUpdateService
from featurebyte.service.info import InfoService
from featurebyte.service.scd_data import SCDDataService
from featurebyte.service.semantic import SemanticService


class SCDDataController(BaseDataDocumentController[SCDDataModel, SCDDataService, SCDDataList]):
    """
    SCDData controller
    """

    paginated_document_class = SCDDataList
    document_update_schema_class = SCDDataUpdate

    async def _get_column_semantic_map(self, document: SCDDataModel) -> dict[str, Any]:
        scd_natural_key_id = await self.semantic_service.get_or_create_document(
            name=SemanticType.SCD_NATURAL_KEY_ID
        )
        scd_surrogate_key_id = await self.semantic_service.get_or_create_document(
            name=SemanticType.SCD_SURROGATE_KEY_ID
        )
        column_semantic_map = {
            document.natural_key_column: scd_natural_key_id,
        }
        if document.surrogate_key_column:
            column_semantic_map.update(
                {
                    document.surrogate_key_column: scd_surrogate_key_id,
                }
            )
        return column_semantic_map


register_controller_constructor(
    SCDDataController, [SCDDataService, DataUpdateService, SemanticService, InfoService]
)
