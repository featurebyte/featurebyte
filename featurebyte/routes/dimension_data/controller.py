"""
EventData API route controller
"""
from __future__ import annotations

from typing import Any

from bson import ObjectId

from featurebyte.enum import SemanticType
from featurebyte.models.dimension_data import DimensionDataModel
from featurebyte.routes.common.base_data import BaseDataDocumentController
from featurebyte.schema.dimension_data import DimensionDataList, DimensionDataServiceUpdate
from featurebyte.schema.info import DimensionDataInfo
from featurebyte.service.dimension_data import DimensionDataService


class DimensionDataController(
    BaseDataDocumentController[DimensionDataModel, DimensionDataService, DimensionDataList]
):
    """
    DimensionData controller
    """

    paginated_document_class = DimensionDataList
    document_update_schema_class = DimensionDataServiceUpdate

    async def _get_column_semantic_map(self, document: DimensionDataModel) -> dict[str, Any]:
        dimension_data_id = await self.semantic_service.get_or_create_document(
            name=SemanticType.DIMENSION_ID
        )
        return {
            document.dimension_id_column: dimension_data_id,
        }

    async def get_info(self, document_id: ObjectId, verbose: bool) -> DimensionDataInfo:
        """
        Get document info given document ID

        Parameters
        ----------
        document_id: ObjectId
            Document ID
        verbose: bool
            Flag to control verbose level

        Returns
        -------
        DimensionDataInfo
        """
        info_document = await self.info_service.get_dimension_data_info(
            document_id=document_id, verbose=verbose
        )
        return info_document
