"""
Feature metadata extractor
"""
from __future__ import annotations

from typing import Any, Optional, Type, TypeVar

from bson import ObjectId

from featurebyte.models.base import PydanticObjectId
from featurebyte.query_graph.node.metadata.operation import GroupOperationStructure
from featurebyte.schema.semantic import SemanticList
from featurebyte.schema.table import TableList
from featurebyte.service.base_document import BaseDocumentService, DocumentUpdateSchema
from featurebyte.service.mixin import Document, DocumentCreateSchema
from featurebyte.service.semantic import SemanticService
from featurebyte.service.table import TableService

ObjectT = TypeVar("ObjectT")


async def _get_list_object(
    service: BaseDocumentService[Document, DocumentCreateSchema, DocumentUpdateSchema],
    document_ids: list[PydanticObjectId],
    list_object_class: Type[ObjectT],
) -> ObjectT:
    """
    Retrieve object through list route & deserialize the records

    Parameters
    ----------
    service: BaseDocumentService
        Service
    document_ids: list[ObjectId]
        List of document IDs
    list_object_class: Type[ObjectT]
        List object class

    Returns
    -------
    ObjectT
    """
    res = await service.list_documents_as_dict(
        page=1, page_size=0, query_filter={"_id": {"$in": document_ids}}
    )
    return list_object_class(**{**res, "page_size": 1})


class FeatureOrTargetMetadataExtractor:
    """
    Feature or target metadata extractor.
    """

    def __init__(
        self,
        table_service: TableService,
        semantic_service: SemanticService,
    ):
        self.table_service = table_service
        self.semantic_service = semantic_service

    async def extract(self, op_struct: GroupOperationStructure) -> dict[str, Any]:
        """
        Extract feature metadata from operation structure

        Parameters
        ----------
        op_struct: GroupOperationStructure
            Operation structure

        Returns
        -------
        dict[str, Any]
        """
        # retrieve related tables & semantics
        table_list = await _get_list_object(self.table_service, op_struct.table_ids, TableList)
        semantic_list = await _get_list_object(
            self.semantic_service, table_list.semantic_ids, SemanticList
        )

        # prepare column mapping
        column_map: dict[tuple[Optional[ObjectId], str], Any] = {}
        semantic_map = {semantic.id: semantic.name for semantic in semantic_list.data}
        for table in table_list.data:
            for column in table.columns_info:
                column_map[(table.id, column.name)] = {
                    "table_name": table.name,
                    "semantic": semantic_map.get(column.semantic_id),  # type: ignore
                }

        # construct feature metadata
        source_columns = {}
        reference_map: dict[Any, str] = {}
        for idx, src_col in enumerate(op_struct.source_columns):
            column_metadata = column_map[(src_col.table_id, src_col.name)]
            reference_map[src_col] = f"Input{idx}"
            source_columns[reference_map[src_col]] = {
                "data": column_metadata["table_name"],
                "column_name": src_col.name,
                "semantic": column_metadata["semantic"],
            }

        derived_columns = {}
        for idx, drv_col in enumerate(op_struct.derived_columns):
            columns = [reference_map[col] for col in drv_col.columns]
            reference_map[drv_col] = f"X{idx}"
            derived_columns[reference_map[drv_col]] = {
                "name": drv_col.name,
                "inputs": columns,
                "transforms": drv_col.transforms,
            }

        aggregation_columns = {}
        for idx, agg_col in enumerate(op_struct.aggregations):
            reference_map[agg_col] = f"F{idx}"
            aggregation_columns[reference_map[agg_col]] = {
                "name": agg_col.name,
                "column": reference_map.get(
                    agg_col.column, None
                ),  # for count aggregation, column is None
                "function": agg_col.method,
                "keys": agg_col.keys,
                "window": agg_col.window,
                "category": agg_col.category,
                "filter": agg_col.filter,
            }

        post_aggregation = None
        if op_struct.post_aggregation:
            post_aggregation = {
                "name": op_struct.post_aggregation.name,
                "inputs": [reference_map[col] for col in op_struct.post_aggregation.columns],
                "transforms": op_struct.post_aggregation.transforms,
            }
        return {
            "input_columns": source_columns,
            "derived_columns": derived_columns,
            "aggregations": aggregation_columns,
            "post_aggregation": post_aggregation,
        }
