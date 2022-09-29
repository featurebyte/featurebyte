"""
This module contains mixin class(es) used in the service directory.
"""
from __future__ import annotations

from typing import Any, Optional, TypeVar

from bson.objectid import ObjectId
from pydantic import ValidationError

from featurebyte.core.generic import ExtendedFeatureStoreModel
from featurebyte.exception import CredentialsError
from featurebyte.models.base import PydanticObjectId
from featurebyte.models.feature_store import FeatureStoreModel
from featurebyte.session.base import BaseSession

Document = TypeVar("Document")


class OpsServiceMixin:
    """
    OpsServiceMixin class contains common operation methods used across different type of services
    """

    user: Any

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

    @staticmethod
    def conditional_return(document: Document, condition: bool) -> Optional[Document]:
        """
        Return output only if condition is True

        Parameters
        ----------
        document: Document
            Document to be returned
        condition: bool
            Flag to control whether to return document

        Returns
        -------
        Optional[Document]
        """
        if condition:
            return document
        return None

    async def _get_feature_store_session(
        self, feature_store: FeatureStoreModel, get_credential: Any
    ) -> BaseSession:
        """
        Get session for feature store

        Parameters
        ----------
        feature_store: FeatureStoreModel
            ExtendedFeatureStoreModel object
        get_credential: Any
            Get credential handler function

        Returns
        -------
        BaseSession
            BaseSession object

        Raises
        ------
        CredentialsError
            When the credentials used to access the feature store is missing or invalid
        """
        try:
            feature_store = ExtendedFeatureStoreModel(**feature_store.dict())
            return feature_store.get_session(
                credentials={
                    feature_store.name: await get_credential(
                        user_id=self.user.id, feature_store_name=feature_store.name
                    )
                }
            )
        except ValidationError as exc:
            raise CredentialsError(
                f'Credential used to access FeatureStore (name: "{feature_store.name}") is missing or invalid.'
            ) from exc
