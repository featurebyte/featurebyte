"""
RelationshipService class
"""
from __future__ import annotations

from typing import Optional, cast

from bson import ObjectId

from featurebyte.exception import DocumentUpdateError
from featurebyte.models.base import FeatureByteBaseDocumentModel
from featurebyte.models.relationship import Relationship
from featurebyte.service.base_document import BaseDocumentService
from featurebyte.service.base_update import BaseUpdateService


class RelationshipService(BaseUpdateService):
    """
    RelationshipService class is responsible for manipulating object relationship and maintaining
    the expected relationship property (example, no cyclic relationship like A is an ancestor of B and
    B is also an ancestor of A).
    """

    @property
    def document_service(self) -> BaseDocumentService[FeatureByteBaseDocumentModel]:
        """
        DocumentService that is used to update relationship attributes

        Raises
        ------
        NotImplementedError
            If the property has not been overriden
        """
        raise NotImplementedError

    @staticmethod
    def _validate_relationship(parent_obj: Relationship, child_obj: Relationship) -> None:
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

    async def add_relationship(
        self, parent_id: ObjectId, child_id: ObjectId, return_document: bool = True
    ) -> Optional[Relationship]:
        """
        Add parent & child relationship between two objects

        Parameters
        ----------
        parent_id: ObjectId
            Parent object ID
        child_id: ObjectId
            Child object ID
        return_document: bool
            Whether to return updated child document

        Returns
        -------
        Updated document
        """
        parent_object = await self.document_service.get_document(document_id=parent_id)
        child_object = await self.document_service.get_document(document_id=child_id)
        assert isinstance(parent_object, Relationship)
        assert isinstance(child_object, Relationship)
        self._validate_relationship(parent_obj=parent_object, child_obj=child_object)

        async with self.persistent.start_transaction():
            updated_document = await self.document_service.update_document(
                document_id=child_id,
                data=Relationship(
                    ancestor_ids=sorted(
                        set(child_object.ancestor_ids).union(
                            self.include_object_id(parent_object.ancestor_ids, parent_id)
                        )
                    ),
                    parent_id=parent_id,
                ),
                return_document=True,
            )
            updated_document = cast(Relationship, updated_document)

            objects = await self.document_service.list_documents(
                query_filter={"ancestor_ids": {"$in": [child_id]}}, page_size=0
            )
            for obj in objects["data"]:
                await self.document_service.update_document(
                    document_id=obj["_id"],
                    data=Relationship(
                        ancestor_ids=sorted(
                            set(obj["ancestor_ids"]).union(updated_document.ancestor_ids)
                        ),
                        parent_id=obj["parent_id"],
                    ),
                )
            return self.conditional_return(updated_document, return_document)
