"""
EventData API route controller
"""
from __future__ import annotations

from typing import Any

from featurebyte.enum import SemanticType
from featurebyte.models.dimension_data import DimensionDataModel
from featurebyte.routes.app_container import register_controller_constructor
from featurebyte.routes.common.base_data import BaseDataDocumentController
from featurebyte.schema.dimension_data import DimensionDataList, DimensionDataUpdate
from featurebyte.service.data_update import DataUpdateService
from featurebyte.service.dimension_data import DimensionDataService
from featurebyte.service.info import InfoService
from featurebyte.service.semantic import SemanticService


class DimensionDataController(
    BaseDataDocumentController[DimensionDataModel, DimensionDataService, DimensionDataList]
):
    """
    DimensionData controller
    """

    paginated_document_class = DimensionDataList
    document_update_schema_class = DimensionDataUpdate

    async def _get_column_semantic_map(self, document: DimensionDataModel) -> dict[str, Any]:
        dimension_data_id = await self.semantic_service.get_or_create_document(
            name=SemanticType.DIMENSION_ID
        )
        return {
            document.dimension_data_id_column: dimension_data_id,
        }


register_controller_constructor(
    DimensionDataController, [DimensionDataService, DataUpdateService, SemanticService, InfoService]
)
