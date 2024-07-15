"""
Feature metadata extractor
"""

from __future__ import annotations

from typing import Any, Optional, Type, TypeVar, Union

from bson import ObjectId

from featurebyte.models.base import PydanticObjectId
from featurebyte.models.feature import BaseFeatureModel
from featurebyte.query_graph.node.metadata.operation import (
    AggregationColumn,
    DerivedDataColumn,
    GroupOperationStructure,
    PostAggregationColumn,
    SourceDataColumn,
)
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

    async def extract_from_object(
        self, obj: BaseFeatureModel
    ) -> tuple[GroupOperationStructure, dict[str, Any]]:
        """
        Extract feature metadata from feature or target

        Parameters
        ----------
        obj: BaseFeatureModel
            Feature or target like object

        Returns
        -------
        tuple[GroupOperationStructure, dict[str, Any]]
        """
        try:
            op_struct = obj.extract_operation_structure(keep_all_source_columns=False)
            metadata = await self._extract(op_struct=op_struct)
        except KeyError:
            # FIXME (https://featurebyte.atlassian.net/browse/DEV-2045): Setting
            #  keep_all_source_columns to False fails in some edge cases. This is a workaround for
            #  the cases where it fails, though it produces info that is more verbose than desired.
            #  We should clean this up.
            op_struct = obj.extract_operation_structure(keep_all_source_columns=True)
            metadata = await self._extract(op_struct=op_struct)
        return op_struct, metadata

    @classmethod
    def _reference_key_func(
        cls,
        col: Optional[
            Union[SourceDataColumn, DerivedDataColumn, AggregationColumn, PostAggregationColumn]
        ],
    ) -> str:
        # helper function to generate reference key used in the reference map (_extract function)
        # As the col contains other attribute like filter to track whether the column has been applied
        # filtering operation, it could cause referencing error when both col & filtered col are used
        # in deriving a column. A referencing key function is introduced to use the source table(s) &
        # column name information only to construct the reference key.
        if col is None:
            return "None"
        if isinstance(col, SourceDataColumn):
            return f"SourceDataColumn({col.table_id}, {col.name})"
        if isinstance(col, DerivedDataColumn):
            table_ids = sorted([col.table_id for col in col.columns if col.table_id is not None])
            return f"DerivedDataColumn({col.name}, {table_ids})"
        if isinstance(col, AggregationColumn):
            # for count aggregation, col.column is None
            col_key = cls._reference_key_func(col.column) if col.column is not None else None
            return f"AggregationColumn({col.name}, {col_key})"

        assert isinstance(col, PostAggregationColumn)
        col_keys = sorted([cls._reference_key_func(col) for col in col.columns])
        return f"PostAggregationColumn({col.name}, {col_keys})"

    async def _extract(self, op_struct: GroupOperationStructure) -> dict[str, Any]:
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
        reference_map: dict[str, str] = {}
        for idx, src_col in enumerate(op_struct.source_columns):
            column_metadata = column_map[(src_col.table_id, src_col.name)]
            reference_key = self._reference_key_func(src_col)
            reference_map[reference_key] = f"Input{idx}"
            source_columns[reference_map[reference_key]] = {
                "data": column_metadata["table_name"],
                "column_name": src_col.name,
                "semantic": column_metadata["semantic"],
            }

        derived_columns = {}
        for idx, drv_col in enumerate(op_struct.derived_columns):
            columns = [reference_map[self._reference_key_func(col)] for col in drv_col.columns]
            reference_key = self._reference_key_func(drv_col)
            reference_map[reference_key] = f"X{idx}"
            derived_columns[reference_map[reference_key]] = {
                "name": drv_col.name,
                "inputs": columns,
                "transforms": drv_col.transforms,
            }

        aggregation_columns = {}
        for idx, agg_col in enumerate(op_struct.aggregations):
            reference_key = self._reference_key_func(agg_col)
            reference_map[reference_key] = f"F{idx}"
            aggregation_columns[reference_map[reference_key]] = {
                "name": agg_col.name,
                "column": reference_map.get(self._reference_key_func(agg_col.column)),
                "function": agg_col.method,
                "keys": agg_col.keys,
                "window": agg_col.window,
                "offset": agg_col.offset,
                "category": agg_col.category,
                "filter": agg_col.filter,
                "aggregation_type": agg_col.aggregation_type,
            }

        post_aggregation = None
        if op_struct.post_aggregation:
            post_aggregation = {
                "name": op_struct.post_aggregation.name,
                "inputs": [
                    reference_map[self._reference_key_func(col)]
                    for col in op_struct.post_aggregation.columns
                ],
                "transforms": op_struct.post_aggregation.transforms,
            }
        return {
            "input_columns": source_columns,
            "derived_columns": derived_columns,
            "aggregations": aggregation_columns,
            "post_aggregation": post_aggregation,
        }
