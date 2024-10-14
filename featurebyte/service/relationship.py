"""
RelationshipService class
"""

from __future__ import annotations

from abc import abstractmethod
from typing import Optional, Set, TypeVar, cast

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

    async def _remove_ancestor_ids(
        self,
        document: Relationship,
        remove_parent_id: Optional[ObjectId],
        remove_ancestor_ids: Set[ObjectId],
        checked_descendant_ids: Set[ObjectId],
    ) -> None:
        """
        Remove ancestor IDs from the document and update all descendant objects

        Parameters
        ----------
        document: Relationship
            Document object
        remove_parent_id: Optional[ObjectId]
            Parent ID to be removed
        remove_ancestor_ids: Set[ObjectId]
            Ancestor IDs to be removed
        checked_descendant_ids: Set[ObjectId]
            Descendant IDs that have been checked
        """
        if document.id in checked_descendant_ids:
            return

        # find the ancestor_ids of the parent objects & keep them
        keep_parents = [par for par in document.parents if par.id != remove_parent_id]
        keep_parent_ids = [par.id for par in keep_parents]

        keep_ancestor_ids = set(keep_parent_ids)
        if keep_parent_ids:
            async for obj in self.document_service.list_documents_as_dict_iterator(
                query_filter={"_id": {"$in": keep_parent_ids}},
                projection={"ancestor_ids": 1},
            ):
                keep_ancestor_ids.update(obj["ancestor_ids"])

        ancestor_ids = (
            set(document.ancestor_ids).difference(remove_ancestor_ids).union(keep_ancestor_ids)
        )
        await self.document_service.update_document(
            document_id=document.id,
            data=self.prepare_document_update_payload(
                ancestor_ids=ancestor_ids,
                parents=keep_parents,
            ),
        )
        checked_descendant_ids.add(document.id)

        async for descendant_obj in self.document_service.list_documents_iterator(
            query_filter={"parents.id": document.id},
        ):
            await self._remove_ancestor_ids(
                document=cast(Relationship, descendant_obj),
                # do not remove parent_id for descendant objects
                remove_parent_id=None,
                remove_ancestor_ids=remove_ancestor_ids,
                checked_descendant_ids=checked_descendant_ids,
            )
            checked_descendant_ids.add(descendant_obj.id)

    async def remove_relationship(self, parent_id: ObjectId, child_id: ObjectId) -> Relationship:
        """
        Remove parent & child relationship between two objects

        Parameters
        ----------
        parent_id: ObjectId
            Parent object to be removed from child object
        child_id: ObjectId
            Child object ID

        Returns
        -------
        Updated document

        Raises
        ------
        DocumentUpdateError
            If the parent object is not the parent of the child object
        """
        parent_obj = await self.document_service.get_document(document_id=parent_id)
        child_obj = await self.document_service.get_document(document_id=child_id)
        assert isinstance(parent_obj, Relationship), "Parent object is not a Relationship object"
        assert isinstance(child_obj, Relationship), "Child object is not a Relationship object"

        # validate relationship first
        has_relationship = [par for par in child_obj.parents if par.id == parent_obj.id]
        if not has_relationship:
            raise DocumentUpdateError(
                f'Object "{parent_obj.name}" is not the parent of object "{child_obj.name}".'
            )

        remove_ancestor_ids = {parent_id}.union(parent_obj.ancestor_ids)
        await self._remove_ancestor_ids(
            document=child_obj,
            remove_parent_id=parent_id,
            remove_ancestor_ids=remove_ancestor_ids,
            checked_descendant_ids=set(),
        )

        updated_document = await self.document_service.get_document(document_id=child_id)
        return cast(Relationship, updated_document)


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
