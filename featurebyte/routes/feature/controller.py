"""
Feature API route controller
"""
from __future__ import annotations

from typing import Any, Type

from http import HTTPStatus

from fastapi import HTTPException

from featurebyte.exception import DocumentConflictError, DocumentNotFoundError, DocumentUpdateError
from featurebyte.models.feature import FeatureModel
from featurebyte.persistent import Persistent
from featurebyte.routes.common.base import BaseDocumentController
from featurebyte.schema.feature import FeatureCreate, FeatureList
from featurebyte.service.feature import FeatureService


class FeatureController(BaseDocumentController[FeatureModel, FeatureList]):
    """
    Feature controller
    """

    paginated_document_class = FeatureList
    document_service_class: Type[FeatureService] = FeatureService  # type: ignore[assignment]

    @classmethod
    async def create_feature(
        cls, user: Any, persistent: Persistent, get_credential: Any, data: FeatureCreate
    ) -> FeatureModel:
        """
        Create Feature at persistent (GitDB or MongoDB)

        Parameters
        ----------
        user: Any
            User class to provide user identifier
        persistent: Persistent
            Object that feature will be saved to
        get_credential: Any
            Get credential handler function
        data: FeatureCreate
            Feature creation payload

        Returns
        -------
        FeatureModel
            Newly created feature object

        Raises
        ------
        HTTPException
            When unexpected failure happens during feature namespace retrieval
            When feature namespace creation fails uniqueness constraint check
            When unexpected failure happens during feature namespace creation
        """
        try:
            document = await cls.document_service_class(
                user=user, persistent=persistent
            ).create_document(data=data, get_credential=get_credential)
            return document
        except (DocumentNotFoundError, DocumentUpdateError) as exc:
            raise HTTPException(
                status_code=HTTPStatus.UNPROCESSABLE_ENTITY, detail=str(exc)
            ) from exc
        except DocumentConflictError as exc:
            raise HTTPException(status_code=HTTPStatus.CONFLICT, detail=str(exc)) from exc
