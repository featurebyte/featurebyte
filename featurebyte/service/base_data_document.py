"""
BaseDataDocumentService class
"""
from __future__ import annotations

from typing import Any, Optional, TypeVar, cast

from bson.objectid import ObjectId

from featurebyte.models.base import UniqueConstraintResolutionSignature
from featurebyte.models.feature_store import ConstructGraphMixin, DataStatus
from featurebyte.models.persistent import QueryFilter
from featurebyte.schema.tabular_data import DataCreate, DataUpdate
from featurebyte.service.base_document import BaseDocumentService
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.service.mixin import Document

DocumentCreate = TypeVar("DocumentCreate", bound=DataCreate)
DocumentUpdate = TypeVar("DocumentUpdate", bound=DataUpdate)


class BaseDataDocumentService(BaseDocumentService[Document, DocumentCreate, DocumentUpdate]):
    """
    BaseDataDocumentService class
    """

    @property
    def tabular_data_type(self) -> str:
        """
        Tabular data type

        Returns
        -------
        str
        """
        return str(self.document_class.__fields__["type"].default.value)

    @property
    def tabular_data_type_to_class_name_map(self) -> dict[str, str]:
        """
        Tabular data type to class name mapping

        Returns
        -------
        dict[str, str]
        """
        return {
            "event_data": "EventData",
            "item_data": "ItemData",
            "dimension_data": "DimensionData",
            "scd_data": "SCDData",
        }

    @property
    def class_name(self) -> str:
        """
        API Object Class name used to represent the underlying collection name

        Returns
        -------
        camel case collection name
        """
        return "".join(elem.title() for elem in self.tabular_data_type.split("_"))

    def _construct_get_query_filter(self, document_id: ObjectId, **kwargs: Any) -> QueryFilter:
        query_filter = super()._construct_get_query_filter(document_id=document_id, **kwargs)
        query_filter["type"] = self.tabular_data_type
        return query_filter

    def _construct_list_query_filter(
        self, query_filter: Optional[dict[str, Any]] = None, **kwargs: Any
    ) -> QueryFilter:
        output = super()._construct_list_query_filter(query_filter=query_filter, **kwargs)
        output["type"] = self.tabular_data_type
        return output

    def _get_conflict_message(
        self,
        conflict_doc: dict[str, Any],
        conflict_signature: dict[str, Any],
        resolution_signature: Optional[UniqueConstraintResolutionSignature],
    ) -> str:
        tabular_data_type = conflict_doc["type"]
        formatted_conflict_signature = ", ".join(
            f'{key}: "{value}"' for key, value in conflict_signature.items()
        )
        class_name = (
            self.class_name if tabular_data_type == self.tabular_data_type else "TabularData"
        )
        message = f"{class_name} ({formatted_conflict_signature}) already exists."
        if resolution_signature:
            if (
                resolution_signature
                in UniqueConstraintResolutionSignature.get_existing_object_type()
            ):
                resolution_statement = UniqueConstraintResolutionSignature.get_resolution_statement(
                    resolution_signature=resolution_signature,
                    class_name=self.tabular_data_type_to_class_name_map[tabular_data_type],
                    document=conflict_doc,
                )
                message += f" Get the existing object by `{resolution_statement}`."
            if resolution_signature == UniqueConstraintResolutionSignature.RENAME:
                message += (
                    f' Please rename object (name: "{conflict_doc["name"]}") to something else.'
                )
        return message

    async def create_document(self, data: DocumentCreate) -> Document:
        # retrieve feature store
        feature_store = await FeatureStoreService(
            user=self.user, persistent=self.persistent
        ).get_document(document_id=data.tabular_source.feature_store_id)

        # create document ID if it is None
        data_doc_id = data.id or ObjectId()
        payload_dict = {**data.json_dict(), "_id": data_doc_id}

        # create graph & node
        graph, node = cast(ConstructGraphMixin, self.document_class).construct_graph_and_node(
            feature_store_details=feature_store.get_feature_store_details(),
            table_data_dict=payload_dict,
        )

        # create document for insertion
        document = self.document_class(
            user_id=self.user.id,
            status=DataStatus.DRAFT,
            graph=graph,
            node_name=node.name,
            **payload_dict,
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
