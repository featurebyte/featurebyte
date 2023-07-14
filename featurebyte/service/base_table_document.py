"""
BaseTableDocumentService class
"""
from __future__ import annotations

from typing import Any, Optional, TypeVar

from bson.objectid import ObjectId

from featurebyte.models.base import UniqueConstraintResolutionSignature
from featurebyte.models.feature_store import TableStatus
from featurebyte.models.persistent import QueryFilter
from featurebyte.schema.table import TableCreate, TableServiceUpdate
from featurebyte.service.base_document import BaseDocumentService
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.service.mixin import Document

DocumentCreate = TypeVar("DocumentCreate", bound=TableCreate)
DocumentUpdate = TypeVar("DocumentUpdate", bound=TableServiceUpdate)


class BaseTableDocumentService(BaseDocumentService[Document, DocumentCreate, DocumentUpdate]):
    """
    BaseTableDocumentService class
    """

    document_update_class: type[DocumentUpdate]

    @property
    def table_type(self) -> str:
        """
        Table type

        Returns
        -------
        str
        """
        return str(self.document_class.__fields__["type"].default.value)

    @property
    def table_type_to_class_name_map(self) -> dict[str, str]:
        """
        Table type to class name mapping

        Returns
        -------
        dict[str, str]
        """
        return {
            "event_table": "EventTable",
            "item_table": "ItemTable",
            "dimension_table": "DimensionTable",
            "scd_table": "SCDTable",
        }

    @property
    def class_name(self) -> str:
        """
        API Object Class name used to represent the underlying collection name

        Returns
        -------
        camel case collection name
        """
        return "".join(elem.title() for elem in self.table_type.split("_"))

    def _construct_get_query_filter(
        self, document_id: ObjectId, use_raw_query_filter: bool = False, **kwargs: Any
    ) -> QueryFilter:
        query_filter = super()._construct_get_query_filter(
            document_id=document_id, use_raw_query_filter=use_raw_query_filter, **kwargs
        )
        query_filter["type"] = self.table_type
        return query_filter

    def construct_list_query_filter(
        self,
        query_filter: Optional[QueryFilter] = None,
        use_raw_query_filter: bool = False,
        **kwargs: Any,
    ) -> QueryFilter:
        output = super().construct_list_query_filter(
            query_filter=query_filter, use_raw_query_filter=use_raw_query_filter, **kwargs
        )
        output["type"] = self.table_type
        return output

    def _get_conflict_message(
        self,
        conflict_doc: dict[str, Any],
        conflict_signature: dict[str, Any],
        resolution_signature: Optional[UniqueConstraintResolutionSignature],
    ) -> str:
        table_type = conflict_doc["type"]
        formatted_conflict_signature = ", ".join(
            f'{key}: "{value}"' for key, value in conflict_signature.items()
        )
        class_name = self.class_name if table_type == self.table_type else "Table"
        message = f"{class_name} ({formatted_conflict_signature}) already exists."
        if resolution_signature:
            if (
                resolution_signature
                in UniqueConstraintResolutionSignature.get_existing_object_type()
            ):
                resolution_statement = UniqueConstraintResolutionSignature.get_resolution_statement(
                    resolution_signature=resolution_signature,
                    class_name=self.table_type_to_class_name_map[table_type],
                    document=conflict_doc,
                )
                message += f" Get the existing object by `{resolution_statement}`."
            if resolution_signature == UniqueConstraintResolutionSignature.RENAME:
                message += (
                    f' Please rename object (name: "{conflict_doc["name"]}") to something else.'
                )
        return message

    async def create_document(self, data: DocumentCreate) -> Document:
        # retrieve feature store to check the feature_store_id is valid
        _ = await FeatureStoreService(
            user=self.user, persistent=self.persistent, catalog_id=self.catalog_id
        ).get_document(document_id=data.tabular_source.feature_store_id)

        # create document ID if it is None
        data_doc_id = data.id or ObjectId()
        payload_dict = {**data.dict(by_alias=True), "_id": data_doc_id}
        if self.is_catalog_specific:
            assert self.catalog_id
            payload_dict["catalog_id"] = self.catalog_id

        # create document for insertion
        document = self.document_class(
            user_id=self.user.id, status=TableStatus.PUBLIC_DRAFT, **payload_dict
        )

        # check any conflict with existing documents
        await self._check_document_unique_constraints(document=document)
        insert_id = await self.persistent.insert_one(
            collection_name=self.collection_name,
            document=document.dict(by_alias=True),
            user_id=self.user.id,
        )
        assert insert_id == document.id
        return await self.get_document(document_id=insert_id)
