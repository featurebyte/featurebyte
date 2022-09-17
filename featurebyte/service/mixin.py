"""
This module contains mixin class(es) used in the service directory.
"""
from __future__ import annotations

from bson.objectid import ObjectId

from featurebyte.models.base import PydanticObjectId


class OpsServiceMixin:
    """
    OpsServiceMixin class contains common operation methods used across different type of services
    """

    @staticmethod
    def include_object_id(
        document_ids: list[ObjectId] | list[PydanticObjectId], document_id: ObjectId
    ) -> list[ObjectId]:
        """
        Include document_id to the document_ids list

        Parameters
        ----------
        document_ids: list[ObjectId]
            List of document IDs
        document_id: ObjectId
            Document ID to be included

        Returns
        -------
        List of sorted document_ids
        """
        return sorted(document_ids + [document_id])  # type: ignore

    @staticmethod
    def exclude_object_id(
        document_ids: list[ObjectId] | list[PydanticObjectId], document_id: ObjectId
    ) -> list[ObjectId]:
        """
        Exclude document_id from the document_ids list

        Parameters
        ----------
        document_ids: list[ObjectId]
            List of document IDs
        document_id: ObjectId
            Document ID to be excluded

        Returns
        -------
        List of sorted document_ids
        """
        return sorted(ObjectId(doc_id) for doc_id in document_ids if doc_id != document_id)
