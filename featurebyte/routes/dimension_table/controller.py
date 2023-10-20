"""
DimensionTable API route controller
"""
from __future__ import annotations

from typing import Any, List, Tuple

from bson import ObjectId

from featurebyte import Feature
from featurebyte.enum import SemanticType
from featurebyte.models.dimension_table import DimensionTableModel
from featurebyte.models.persistent import QueryFilter
from featurebyte.routes.common.base_table import BaseTableDocumentController
from featurebyte.schema.dimension_table import DimensionTableList, DimensionTableServiceUpdate
from featurebyte.schema.info import DimensionTableInfo
from featurebyte.service.dimension_table import DimensionTableService
from featurebyte.service.entity import EntityService
from featurebyte.service.feature import FeatureService
from featurebyte.service.semantic import SemanticService
from featurebyte.service.table_columns_info import TableDocumentService
from featurebyte.service.table_facade import TableFacadeService
from featurebyte.service.table_info import TableInfoService


class DimensionTableController(
    BaseTableDocumentController[DimensionTableModel, DimensionTableService, DimensionTableList]
):
    """
    DimensionTable controller
    """

    paginated_document_class = DimensionTableList
    document_update_schema_class = DimensionTableServiceUpdate
    semantic_tag_rules = {
        **BaseTableDocumentController.semantic_tag_rules,
        "dimension_id_column": SemanticType.DIMENSION_ID,
    }

    def __init__(
        self,
        dimension_table_service: TableDocumentService,
        table_facade_service: TableFacadeService,
        semantic_service: SemanticService,
        table_info_service: TableInfoService,
        entity_service: EntityService,
        feature_service: FeatureService,
    ):
        super().__init__(dimension_table_service, table_facade_service, semantic_service)
        self.table_info_service = table_info_service
        self.entity_service = entity_service
        self.feature_service = feature_service

    async def get_info(self, document_id: ObjectId, verbose: bool) -> DimensionTableInfo:
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
        DimensionTableInfo
        """
        dimension_table = await self.service.get_document(document_id=document_id)
        table_dict = await self.table_info_service.get_table_info(
            data_document=dimension_table, verbose=verbose
        )
        return DimensionTableInfo(
            **table_dict,
            dimension_id_column=dimension_table.dimension_id_column,
        )

    async def service_and_query_pairs_for_checking_reference(
        self, document_id: ObjectId
    ) -> List[Tuple[Any, QueryFilter]]:
        return [
            (self.entity_service, {"primary_table_ids": document_id}),
            (self.feature_service, {"table_ids": document_id}),
        ]
