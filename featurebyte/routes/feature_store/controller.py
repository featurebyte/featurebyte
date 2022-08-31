"""
FeatureStore API route controller
"""
from __future__ import annotations

from typing import Any, Type

from http import HTTPStatus

from fastapi import HTTPException

from featurebyte.exception import DocumentConflictError, DocumentNotFoundError
from featurebyte.models.feature_store import FeatureStoreModel
from featurebyte.persistent.base import Persistent
from featurebyte.routes.common.base import BaseDocumentController
from featurebyte.schema.feature_store import FeatureStoreCreate, FeatureStoreList
from featurebyte.service.feature_store import FeatureStoreService


class FeatureStoreController(BaseDocumentController[FeatureStoreModel, FeatureStoreList]):
    """
    FeatureStore controller
    """

    paginated_document_class = FeatureStoreList
    document_service_class: Type[FeatureStoreService] = FeatureStoreService  # type: ignore[assignment]

    @classmethod
    async def create_feature_store(
        cls,
        user: Any,
        persistent: Persistent,
        data: FeatureStoreCreate,
    ) -> FeatureStoreModel:
        """
        Create Feature Store at persistent

        Parameters
        ----------
        user: Any
            User class to provide user identifier
        persistent: Persistent
            Object that feature store will be saved to
        data: FeatureStoreCreate
            FeatureStore creation payload

        Returns
        -------
        FeatureStoreModel
            Newly created feature store document

        Raises
        ------
        HTTPException
            If some referenced object not found or there exists conflicting value
        """
        try:
            document = await cls.document_service_class(
                user=user, persistent=persistent
            ).create_document(data)
            return document
        except DocumentNotFoundError as exc:
            raise HTTPException(
                status_code=HTTPStatus.UNPROCESSABLE_ENTITY, detail=str(exc)
            ) from exc
        except DocumentConflictError as exc:
            raise HTTPException(status_code=HTTPStatus.CONFLICT, detail=str(exc)) from exc
