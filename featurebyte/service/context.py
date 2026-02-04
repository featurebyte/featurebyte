"""
ContextService class
"""

from __future__ import annotations

from typing import Any, List, Optional

from bson import ObjectId
from redis import Redis

from featurebyte.exception import DocumentUpdateError
from featurebyte.models.context import ContextModel, UserProvidedColumn
from featurebyte.persistent import Persistent
from featurebyte.query_graph.enum import NodeOutputType
from featurebyte.query_graph.node.metadata.operation import NodeOutputCategory, OperationStructure
from featurebyte.routes.block_modification_handler import BlockModificationHandler
from featurebyte.schema.context import ContextCreate, ContextUpdate
from featurebyte.service.base_document import BaseDocumentService
from featurebyte.service.entity import EntityService
from featurebyte.service.table import TableService
from featurebyte.storage import Storage


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
        block_modification_handler: BlockModificationHandler,
        table_service: TableService,
        storage: Storage,
        redis: Redis[Any],
    ):
        super().__init__(
            user=user,
            persistent=persistent,
            catalog_id=catalog_id,
            block_modification_handler=block_modification_handler,
            storage=storage,
            redis=redis,
        )
        self.entity_service = entity_service
        self.table_service = table_service

    def _validate_user_provided_columns_update(
        self,
        existing_columns: List[UserProvidedColumn],
        new_columns: List[UserProvidedColumn],
    ) -> None:
        """
        Validate user_provided_columns update.

        Rules:
        - New columns can be added
        - Existing columns cannot be removed
        - Existing column dtypes cannot be changed
        - Existing column descriptions can be updated

        Parameters
        ----------
        existing_columns: List[UserProvidedColumn]
            Current user_provided_columns in the context
        new_columns: List[UserProvidedColumn]
            New user_provided_columns to update to

        Raises
        ------
        DocumentUpdateError
            When validation fails (column removed or dtype changed)
        """
        existing_cols = {col.name: col for col in existing_columns}
        new_cols = {col.name: col for col in new_columns}

        # Check no columns are removed
        removed = set(existing_cols.keys()) - set(new_cols.keys())
        if removed:
            raise DocumentUpdateError(
                f"Cannot remove existing user-provided columns: {sorted(removed)}"
            )

        # Check dtypes not changed for existing columns
        for name, existing_col in existing_cols.items():
            if name in new_cols and new_cols[name].dtype != existing_col.dtype:
                raise DocumentUpdateError(
                    f"Cannot change dtype of existing user-provided column '{name}' "
                    f"from {existing_col.dtype} to {new_cols[name].dtype}"
                )

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
            table_id_to_doc[table_id] = await self.table_service.get_document(document_id=table_id)

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

        missing_entity_ids = list(set(context.primary_entity_ids).difference(found_entity_ids))
        if missing_entity_ids:
            missing_entities = await self.entity_service.list_documents_as_dict(
                query_filter={"_id": {"$in": missing_entity_ids}}
            )
            missing_entity_names = [entity["name"] for entity in missing_entities["data"]]
            raise DocumentUpdateError(
                f"Entities {missing_entity_names} not found in the context view."
            )

    async def update_user_provided_column_description(
        self,
        document_id: ObjectId,
        column_name: str,
        description: Optional[str],
    ) -> ContextModel:
        """
        Update user-provided column description.

        Parameters
        ----------
        document_id: ObjectId
            Context document ID
        column_name: str
            Name of the user-provided column to update
        description: Optional[str]
            New description for the column

        Raises
        ------
        DocumentUpdateError
            When column is not found

        Returns
        -------
        ContextModel
            Updated context document
        """
        document = await self.get_document(
            document_id=document_id, populate_remote_attributes=False
        )

        # Find and update the column
        column_found = False
        updated_columns = []
        for col in document.user_provided_columns:
            if col.name == column_name:
                col.description = description
                column_found = True
            updated_columns.append(col)

        if not column_found:
            raise DocumentUpdateError(f"User-provided column '{column_name}' not found in context")

        # Update the document
        result = await self.update_document(
            document_id=document_id,
            data=ContextUpdate(user_provided_columns=updated_columns),
        )
        assert result is not None
        return result

    async def update_document(
        self,
        document_id: ObjectId,
        data: ContextUpdate,
        exclude_none: bool = True,
        document: Optional[ContextModel] = None,
        return_document: bool = True,
        skip_block_modification_check: bool = False,
        populate_remote_attributes: bool = True,
    ) -> Optional[ContextModel]:
        document = await self.get_document(
            document_id=document_id, populate_remote_attributes=False
        )
        if data.graph and data.node_name:
            node = data.graph.get_node_by_name(data.node_name)
            operation_structure = data.graph.extract_operation_structure(
                node=node, keep_all_source_columns=True
            )
            await self._validate_view(operation_structure=operation_structure, context=document)

        # Validate user_provided_columns update if provided
        if data.user_provided_columns is not None:
            self._validate_user_provided_columns_update(
                existing_columns=document.user_provided_columns,
                new_columns=data.user_provided_columns,
            )

        document = await super().update_document(
            document_id=document_id,
            document=document,
            data=data,
            exclude_none=exclude_none,
            return_document=return_document,
            skip_block_modification_check=skip_block_modification_check,
            populate_remote_attributes=populate_remote_attributes,
        )
        return document
