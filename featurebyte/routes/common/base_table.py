"""
BaseDataController for API routes
"""
from __future__ import annotations

from typing import Any, Optional, Type, TypeVar, cast

from bson.objectid import ObjectId

from featurebyte.enum import SemanticType
from featurebyte.exception import ColumnNotFoundError
from featurebyte.models.dimension_table import DimensionTableModel
from featurebyte.models.event_table import EventTableModel
from featurebyte.models.item_table import ItemTableModel
from featurebyte.models.scd_table import SCDTableModel
from featurebyte.query_graph.model.column_info import ColumnInfo
from featurebyte.query_graph.model.critical_data_info import CriticalDataInfo
from featurebyte.routes.common.base import BaseDocumentController, PaginatedDocument
from featurebyte.schema.table import TableServiceUpdate, TableUpdate
from featurebyte.service.dimension_table import DimensionTableService
from featurebyte.service.event_table import EventTableService
from featurebyte.service.item_table import ItemTableService
from featurebyte.service.scd_table import SCDTableService
from featurebyte.service.semantic import SemanticService
from featurebyte.service.table_columns_info import TableDocumentService
from featurebyte.service.table_facade import TableFacadeService

TableDocumentT = TypeVar(
    "TableDocumentT", EventTableModel, ItemTableModel, DimensionTableModel, SCDTableModel
)
TableDocumentServiceT = TypeVar(
    "TableDocumentServiceT",
    EventTableService,
    ItemTableService,
    DimensionTableService,
    SCDTableService,
)


class BaseTableDocumentController(
    BaseDocumentController[TableDocumentT, TableDocumentServiceT, PaginatedDocument]
):
    """
    BaseTableDocumentController for API routes
    """

    document_update_schema_class: Type[TableServiceUpdate]
    semantic_tag_rules: dict[str, SemanticType] = {
        "record_creation_timestamp_column": SemanticType.RECORD_CREATION_TIMESTAMP,
    }

    def __init__(
        self,
        service: TableDocumentService,
        table_facade_service: TableFacadeService,
        semantic_service: SemanticService,
    ):
        super().__init__(service)  # type: ignore[arg-type]
        self.table_facade_service = table_facade_service
        self.semantic_service = semantic_service

    async def _get_column_semantic_map(self, document: TableDocumentT) -> dict[str, Any]:
        """
        Construct column name to semantic mapping

        Parameters
        ----------
        document: TableDocumentT
            Newly created document

        Returns
        -------
        dict[str, Any]
        """
        column_semantic_map = {}
        for field, semantic_type in self.semantic_tag_rules.items():
            semantic_id = await self.semantic_service.get_or_create_document(name=semantic_type)
            special_column_name = getattr(document, field)
            if special_column_name:
                column_semantic_map[special_column_name] = semantic_id
        return column_semantic_map

    async def _add_semantic_tags(self, document: TableDocumentT) -> TableDocumentT:
        """
        Add semantic tags to newly created document

        Parameters
        ----------
        document: TableDocumentT
            Newly created document

        Returns
        -------
        TableDocumentT
        """
        column_semantic_map = await self._get_column_semantic_map(document=document)
        columns_info = []
        for col_info in document.columns_info:
            semantic = column_semantic_map.get(col_info.name)
            if semantic:
                columns_info.append(ColumnInfo(**{**col_info.dict(), "semantic_id": semantic.id}))
            else:
                columns_info.append(col_info)

        output = await self.service.update_document(
            document_id=document.id,
            data=self.document_update_schema_class(columns_info=columns_info),  # type: ignore
            return_document=True,
        )
        return cast(TableDocumentT, output)

    async def create_table(self, data: TableDocumentT) -> TableDocumentT:
        """
        Create Table record at persistent

        Parameters
        ----------
        data: TableDocumentT
            EventTable/ItemTable/SCDTable/DimensionTable creation payload

        Returns
        -------
        TableDocumentT
            Newly created table object
        """
        document = await self.service.create_document(data)  # type: ignore[arg-type]
        return await self._add_semantic_tags(document=document)  # type: ignore

    async def update_table(self, document_id: ObjectId, data: TableUpdate) -> TableDocumentT:
        """
        Update Table (for example, to update scheduled task) at persistent (GitDB or MongoDB)

        Parameters
        ----------
        document_id: ObjectId
            Table document ID
        data: TableUpdate
            Table update payload

        Returns
        -------
        TableDocumentT
            Table object with updated attribute(s)
        """

        # Update of columns info is deprecated and will be removed in release 0.5.0
        # See https://featurebyte.atlassian.net/browse/DEV-2000
        if data.columns_info:
            await self.table_facade_service.update_table_columns_info(
                table_id=document_id,
                columns_info=data.columns_info,
                service=self.service,
            )

        if data.status:
            await self.table_facade_service.update_table_status(
                table_id=document_id,
                status=data.status,
                service=self.service,
            )

        # update other parameters
        update_dict = data.dict(exclude={"status": True, "columns_info": True}, exclude_none=True)
        if update_dict:
            await self.service.update_document(
                document_id=document_id,
                data=self.document_update_schema_class(**update_dict),  # type: ignore[arg-type]
                return_document=False,
            )

        return await self.get(document_id=document_id)

    async def update_table_columns_info(
        self, document_id: ObjectId, column_name: str, field: str, data: Any
    ) -> TableDocumentT:
        """
        Update table columns info

        Parameters
        ----------
        document_id: ObjectId
            Table document ID
        column_name: str
            Column name
        field: str
            Field to update
        data: Any
            Data to update

        Returns
        -------
        TableDocumentT
            Table object with updated columns info
        """
        document = await self.service.get_document(document_id=document_id)
        columns_info = document.columns_info
        column_exists = False
        for col_info in columns_info:
            if col_info.name == column_name:
                setattr(col_info, field, data)
                column_exists = True
                break

        if not column_exists:
            raise ColumnNotFoundError(
                f'Column: {column_name} not found in {self.service.class_name} (id: "{document_id}")'
            )

        await self.table_facade_service.update_table_columns_info(
            table_id=document_id,
            columns_info=columns_info,
            service=self.service,
        )
        return await self.get(document_id=document_id)

    async def update_column_entity(
        self, document_id: ObjectId, column_name: str, entity_id: Optional[ObjectId]
    ) -> TableDocumentT:
        """
        Update column entity

        Parameters
        ----------
        document_id: ObjectId
            Table document ID
        column_name: str
            Column name
        entity_id: Optional[ObjectId]
            Entity ID

        Returns
        -------
        TableDocumentT
            Table object with updated entity
        """
        return await self.update_table_columns_info(
            document_id=document_id,
            column_name=column_name,
            field="entity_id",
            data=entity_id,
        )

    async def update_column_critical_data_info(
        self, document_id: ObjectId, column_name: str, critical_data_info: CriticalDataInfo
    ) -> TableDocumentT:
        """
        Update column critical data info

        Parameters
        ----------
        document_id: ObjectId
            Table document ID
        column_name: str
            Column name
        critical_data_info: CriticalDataInfo
            Critical data info

        Returns
        -------
        TableDocumentT
            Table object with updated critical data info
        """
        return await self.update_table_columns_info(
            document_id=document_id,
            column_name=column_name,
            field="critical_data_info",
            data=critical_data_info,
        )

    async def update_column_description(
        self, document_id: ObjectId, column_name: str, description: Optional[str]
    ) -> TableDocumentT:
        """
        Update column description

        Parameters
        ----------
        document_id: ObjectId
            Table document ID
        column_name: str
            Column name
        description: Optional[str]
            Column description

        Returns
        -------
        TableDocumentT
            Table object with updated description
        """
        return await self.update_table_columns_info(
            document_id=document_id,
            column_name=column_name,
            field="description",
            data=description,
        )

    async def update_column_semantic(
        self, document_id: ObjectId, column_name: str, semantic_id: Optional[ObjectId]
    ) -> TableDocumentT:
        """
        Update column semantic

        Parameters
        ----------
        document_id: ObjectId
            Table document ID
        column_name: str
            Column name
        semantic_id: Optional[ObjectId]
            Semantic ID
        """
        return await self.update_table_columns_info(
            document_id=document_id,
            column_name=column_name,
            field="semantic_id",
            data=semantic_id,
        )
