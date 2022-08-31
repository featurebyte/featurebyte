"""
FeatureList API route controller
"""
from __future__ import annotations

from typing import Any, Type

from http import HTTPStatus

from fastapi import HTTPException

from featurebyte.exception import (
    DocumentConflictError,
    DocumentInconsistencyError,
    DocumentNotFoundError,
)
from featurebyte.models.feature import FeatureListModel
from featurebyte.persistent import Persistent
from featurebyte.routes.common.base import BaseDocumentController
from featurebyte.schema.feature_list import FeatureListCreate, FeatureListPaginatedList
from featurebyte.service.feature_list import FeatureListService


class FeatureListController(BaseDocumentController[FeatureListModel, FeatureListPaginatedList]):
    """
    FeatureList controller
    """

    paginated_document_class = FeatureListPaginatedList
    document_service_class: Type[FeatureListService] = FeatureListService  # type: ignore[assignment]

    @classmethod
    async def create_feature_list(
        cls, user: Any, persistent: Persistent, get_credential: Any, data: FeatureListCreate
    ) -> FeatureListModel:
        """
        Create FeatureList at persistent (GitDB or MongoDB)

        Parameters
        ----------
        user: Any
            User class to provide user identifier
        persistent: Persistent
            Object that feature list will be saved to
        get_credential: Any
            Get credential handler function
        data: FeatureListCreate
            Feature creation payload

        Returns
        -------
        FeatureListModel
            Newly created feature list object

        Raises
        ------
        HTTPException
            If some referenced object not found or there exists conflicting value
        """
        try:
            document = await cls.document_service_class(
                user=user, persistent=persistent
            ).create_document(data=data, get_credential=get_credential)
            return document
        except (DocumentNotFoundError, DocumentInconsistencyError) as exc:
            raise HTTPException(
                status_code=HTTPStatus.UNPROCESSABLE_ENTITY, detail=str(exc)
            ) from exc
        except DocumentConflictError as exc:
            raise HTTPException(status_code=HTTPStatus.CONFLICT, detail=str(exc)) from exc
