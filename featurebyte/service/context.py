"""
ContextService class
"""
from __future__ import annotations

from typing import Any, Optional

from bson import ObjectId

from featurebyte.exception import DocumentUpdateError
from featurebyte.models.context import ContextModel
from featurebyte.persistent import Persistent
from featurebyte.query_graph.enum import NodeOutputType
from featurebyte.query_graph.node.metadata.operation import NodeOutputCategory, OperationStructure
from featurebyte.schema.context import ContextCreate, ContextUpdate
from featurebyte.service.base_document import BaseDocumentService
from featurebyte.service.entity import EntityService
from featurebyte.service.table import TableService


class ContextService(BaseDocumentService[ContextModel, ContextCreate, ContextUpdate]):
    """
    ContextService class
    """

    document_class = ContextModel

    def __init__(
        self,
        user: Any,
        persistent: Persistent,
        catalog_id: Optional[ObjectId],
        entity_service: EntityService,
    ):
        super().__init__(user, persistent, catalog_id)
        self.entity_service = entity_service

    async def create_document(self, data: ContextCreate) -> ContextModel:
        entities = await self.entity_service.list_documents_as_dict(
            page=1, page_size=0, query_filter={"_id": {"$in": data.entity_ids}}
        )
        found_entity_ids = set(doc["_id"] for doc in entities["data"])
        not_found_entity_ids = set(data.entity_ids).difference(found_entity_ids)
        if not_found_entity_ids:
            # trigger entity not found error
            await self.entity_service.get_document(document_id=list(not_found_entity_ids)[0])
        return await super().create_document(data=data)

    async def _validate_view(
        self, operation_structure: OperationStructure, context: ContextModel
    ) -> None:
        """
        Validate context view operation structure, check that
        - whether all table used in the view can be retrieved from persistent
        - whether the view output contains required entity column(s)

        Parameters
        ----------
        operation_structure: OperationStructure
            Context view's operation structure to be validated
        context: ContextModel
            Context stored at the persistent

        Raises
        ------
        DocumentUpdateError
            When the context view is not a proper context view (frame, view and has all required entities)
        """
        table_service = TableService(
            user=self.user, persistent=self.persistent, catalog_id=self.catalog_id
        )

        # check that it is a proper view
        if operation_structure.output_type != NodeOutputType.FRAME:
            raise DocumentUpdateError("Context view must but a table but not a single column.")
        if operation_structure.output_category != NodeOutputCategory.VIEW:
            raise DocumentUpdateError("Context view must be a view but not a feature.")

        # check that table document can be retrieved from the persistent
        table_id_to_doc = {}
        table_ids = list(set(col.table_id for col in operation_structure.source_columns))
        for table_id in table_ids:
            if table_id is None:
                raise DocumentUpdateError("Table record has not been stored at the persistent.")
            table_id_to_doc[table_id] = await table_service.get_document(document_id=table_id)

        # check that entities can be found on the view
        # TODO: add entity id to operation structure column (DEV-957)
        found_entity_ids = set()
        for source_col in operation_structure.source_columns:
            assert source_col.table_id is not None
            table = table_id_to_doc[source_col.table_id]
            column_info = next(
                (col_info for col_info in table.columns_info if col_info.name == source_col.name),
                None,
            )
            if column_info is None:
                raise DocumentUpdateError(
                    f'Column "{source_col.name}" not found in table "{table.name}".'
                )
            if column_info.entity_id:
                found_entity_ids.add(column_info.entity_id)

        missing_entity_ids = list(set(context.entity_ids).difference(found_entity_ids))
        if missing_entity_ids:
            missing_entities = await self.entity_service.list_documents_as_dict(
                query_filter={"_id": {"$in": missing_entity_ids}}
            )
            missing_entity_names = [entity["name"] for entity in missing_entities["data"]]
            raise DocumentUpdateError(
                f"Entities {missing_entity_names} not found in the context view."
            )

    async def update_document(
        self,
        document_id: ObjectId,
        data: ContextUpdate,
        exclude_none: bool = True,
        document: Optional[ContextModel] = None,
        return_document: bool = True,
    ) -> Optional[ContextModel]:
        document = await self.get_document(document_id=document_id)
        if data.graph and data.node_name:
            node = data.graph.get_node_by_name(data.node_name)
            operation_structure = data.graph.extract_operation_structure(
                node=node, keep_all_source_columns=True
            )
            await self._validate_view(operation_structure=operation_structure, context=document)

        document = await super().update_document(
            document_id=document_id,
            document=document,
            data=data,
            exclude_none=exclude_none,
            return_document=return_document,
        )
        return document
