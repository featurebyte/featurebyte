"""
Table info service
"""
from typing import Any, Dict

from bson import ObjectId

from featurebyte.models.feature_store import TableModel
from featurebyte.schema.info import EntityBriefInfoList
from featurebyte.service.catalog import CatalogService
from featurebyte.service.entity import EntityService
from featurebyte.service.semantic import SemanticService


class TableInfoService:
    """
    Table info service
    """

    def __init__(
        self,
        entity_service: EntityService,
        semantic_service: SemanticService,
        catalog_service: CatalogService,
    ):
        self.entity_service = entity_service
        self.semantic_service = semantic_service
        self.catalog_service = catalog_service

    async def get_table_info(self, data_document: TableModel, verbose: bool) -> Dict[str, Any]:
        """
        Get table info

        Parameters
        ----------
        data_document: TableModel
            Data document (could be event table, SCD table, item table, dimension table, etc)
        verbose: bool
            Verbose or not

        Returns
        -------
        Dict[str, Any]
        """
        entities = await self.entity_service.list_documents(
            page=1, page_size=0, query_filter={"_id": {"$in": data_document.entity_ids}}
        )
        semantics = await self.semantic_service.list_documents(
            page=1, page_size=0, query_filter={"_id": {"$in": data_document.semantic_ids}}
        )
        columns_info = None
        if verbose:
            columns_info = []
            entity_map = {ObjectId(entity["_id"]): entity["name"] for entity in entities["data"]}
            semantic_map = {
                ObjectId(semantic["_id"]): semantic["name"] for semantic in semantics["data"]
            }
            for column_info in data_document.columns_info:
                columns_info.append(
                    {
                        **column_info.dict(),
                        "entity": entity_map.get(column_info.entity_id),  # type: ignore[arg-type]
                        "semantic": semantic_map.get(column_info.semantic_id),  # type: ignore[arg-type]
                        "critical_data_info": column_info.critical_data_info,
                    }
                )

        # get catalog info
        catalog = await self.catalog_service.get_document(data_document.catalog_id)
        for entity in entities["data"]:
            assert entity["catalog_id"] == catalog.id
            entity["catalog_name"] = catalog.name

        return {
            "name": data_document.name,
            "created_at": data_document.created_at,
            "updated_at": data_document.updated_at,
            "record_creation_timestamp_column": data_document.record_creation_timestamp_column,
            "table_details": data_document.tabular_source.table_details,
            "status": data_document.status,
            "entities": EntityBriefInfoList.from_paginated_data(entities),
            "semantics": [semantic["name"] for semantic in semantics["data"]],
            "column_count": len(data_document.columns_info),
            "columns_info": columns_info,
            "catalog_name": catalog.name,
        }
