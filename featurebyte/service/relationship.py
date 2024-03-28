"""
RelationshipService class
"""

from __future__ import annotations

from typing import TypeVar, cast

from abc import abstractmethod

from bson import ObjectId

from featurebyte.exception import DocumentUpdateError
from featurebyte.models.base import FeatureByteBaseDocumentModel, FeatureByteBaseModel
from featurebyte.models.relationship import Parent, Relationship
from featurebyte.persistent import Persistent
from featurebyte.schema.common.base import BaseDocumentServiceUpdateSchema
from featurebyte.schema.entity import EntityServiceUpdate
from featurebyte.schema.semantic import SemanticServiceUpdate
from featurebyte.service.base_document import BaseDocumentService
from featurebyte.service.entity import EntityService
from featurebyte.service.mixin import OpsServiceMixin
from featurebyte.service.semantic import SemanticService

ParentT = TypeVar("ParentT", bound=Parent)
BaseDocumentServiceT = BaseDocumentService[
    FeatureByteBaseDocumentModel, FeatureByteBaseModel, BaseDocumentServiceUpdateSchema
]


class RelationshipService(OpsServiceMixin):
    """
    RelationshipService class is responsible for manipulating object relationship and maintaining
    the expected relationship property (example, no cyclic relationship like A is an ancestor of B and
    B is also an ancestor of A).
    """

    def __init__(
        self,
        persistent: Persistent,
    ):
        self.persistent = persistent

    @property
    @abstractmethod
    def document_service(self) -> BaseDocumentServiceT:
        """
        DocumentService that is used to update relationship attributes

        Raises
        ------
        NotImplementedError
            If the property has not been overriden
        """

    @classmethod
    @abstractmethod
    def prepare_document_update_payload(
        cls, ancestor_ids: set[ObjectId], parents: list[ParentT]
    ) -> BaseDocumentServiceUpdateSchema:
        """
        Prepare document update payload (by converting Relationship schema into service update schema)

        Parameters
        ----------
        ancestor_ids: list[ObjectId]
            List of ancestor IDs
        parents: list[ParentT]
            List of parents

        Raises
        ------
        NotImplementedError
            If the method has not been overriden
        """

    @staticmethod
    def _validate_add_relationship_operation(
        parent_obj: Relationship, child_obj: Relationship
    ) -> None:
        if parent_obj.id == child_obj.id:
            raise DocumentUpdateError(f'Object "{parent_obj.name}" cannot be both parent & child.')
        if child_obj.id in parent_obj.ancestor_ids:
            raise DocumentUpdateError(
                f'Object "{parent_obj.name}" should not be the parent of object "{child_obj.name}" as '
                f'object "{child_obj.name}" is already an ancestor of object "{parent_obj.name}".'
            )
        if parent_obj.id in child_obj.ancestor_ids:
            raise DocumentUpdateError(
                f'Object "{parent_obj.name}" is already an ancestor of object "{child_obj.name}".'
            )

    async def add_relationship(self, parent: ParentT, child_id: ObjectId) -> Relationship:
        """
        Add parent & child relationship between two objects

        Parameters
        ----------
        parent: ParentT
            Parent object ID
        child_id: ObjectId
            Child object ID

        Returns
        -------
        Updated document
        """
        parent_object = await self.document_service.get_document(document_id=parent.id)
        child_object = await self.document_service.get_document(document_id=child_id)
        assert isinstance(parent_object, Relationship)
        assert isinstance(child_object, Relationship)
        self._validate_add_relationship_operation(parent_obj=parent_object, child_obj=child_object)

        async with self.persistent.start_transaction():
            updated_document = await self.document_service.update_document(
                document_id=child_id,
                data=self.prepare_document_update_payload(
                    ancestor_ids=set(child_object.ancestor_ids).union(
                        self.include_object_id(parent_object.ancestor_ids, parent.id)
                    ),
                    parents=child_object.parents + [parent],
                ),
                return_document=True,
            )
            updated_document = cast(Relationship, updated_document)

            # update all objects which have child_id in their ancestor_ids
            query_filter = {"ancestor_ids": {"$in": [child_id]}}
            async for obj in self.document_service.list_documents_as_dict_iterator(
                query_filter=query_filter
            ):
                await self.document_service.update_document(
                    document_id=obj["_id"],
                    data=self.prepare_document_update_payload(
                        ancestor_ids=set(obj["ancestor_ids"]).union(updated_document.ancestor_ids),
                        parents=obj["parents"],
                    ),
                )

            return updated_document

    @staticmethod
    def _validate_remove_relationship_operation(
        parent_obj: Relationship, child_obj: Relationship
    ) -> None:
        has_relationship = [par for par in child_obj.parents if par.id == parent_obj.id]
        if not has_relationship:
            raise DocumentUpdateError(
                f'Object "{parent_obj.name}" is not the parent of object "{child_obj.name}".'
            )

    async def remove_relationship(self, parent_id: ObjectId, child_id: ObjectId) -> Relationship:
        """
        Remove parent & child relationship between two objects

        Parameters
        ----------
        parent_id: ObjectId
            Parent object
        child_id: ObjectId
            Child object ID

        Returns
        -------
        Updated document
        """
        parent_object = await self.document_service.get_document(document_id=parent_id)
        child_object = await self.document_service.get_document(document_id=child_id)
        assert isinstance(parent_object, Relationship)
        assert isinstance(child_object, Relationship)
        self._validate_remove_relationship_operation(
            parent_obj=parent_object, child_obj=child_object
        )

        async with self.persistent.start_transaction():
            updated_document = await self.document_service.update_document(
                document_id=child_id,
                data=self.prepare_document_update_payload(
                    ancestor_ids=set(child_object.ancestor_ids).difference(  # type: ignore[arg-type]
                        self.include_object_id(parent_object.ancestor_ids, parent_id)
                    ),
                    parents=[par for par in child_object.parents if par.id != parent_id],
                ),
                return_document=True,
            )
            updated_document = cast(Relationship, updated_document)

            # update all objects which have child_id in their ancestor_ids
            query_filter = {"ancestor_ids": {"$in": [child_id]}}
            async for obj in self.document_service.list_documents_as_dict_iterator(
                query_filter=query_filter,
                projection={"_id": 1, "ancestor_ids": 1, "parents": 1},
            ):
                await self.document_service.update_document(
                    document_id=obj["_id"],
                    data=self.prepare_document_update_payload(
                        ancestor_ids=set(obj["ancestor_ids"]).difference(
                            self.include_object_id(parent_object.ancestor_ids, parent_id)
                        ),
                        parents=obj["parents"],
                    ),
                )

            return updated_document


class EntityRelationshipService(RelationshipService):
    """
    EntityRelationshipService is responsible to update relationship between different entities.
    """

    def __init__(self, persistent: Persistent, entity_service: EntityService):
        super().__init__(persistent=persistent)
        self.entity_service = entity_service

    @property
    def document_service(self) -> BaseDocumentServiceT:
        return self.entity_service  # type: ignore[return-value]

    @classmethod
    def prepare_document_update_payload(
        cls, ancestor_ids: set[ObjectId], parents: list[ParentT]
    ) -> BaseDocumentServiceUpdateSchema:
        return EntityServiceUpdate(ancestor_ids=ancestor_ids, parents=parents)


class SemanticRelationshipService(RelationshipService):
    """
    SemanticRelationshipService is responsible to update relationship between different semantics.
    """

    def __init__(self, persistent: Persistent, semantic_service: SemanticService):
        super().__init__(persistent=persistent)
        self.semantic_service = semantic_service

    @property
    def document_service(self) -> BaseDocumentServiceT:
        return self.semantic_service  # type: ignore[return-value]

    @classmethod
    def prepare_document_update_payload(
        cls, ancestor_ids: set[ObjectId], parents: list[ParentT]
    ) -> BaseDocumentServiceUpdateSchema:
        return SemanticServiceUpdate(ancestor_ids=ancestor_ids, parents=parents)
