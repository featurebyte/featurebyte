"""
FeatureNamespace API route controller
"""
from __future__ import annotations

from typing import Any

from http import HTTPStatus

from bson.objectid import ObjectId
from fastapi import HTTPException

from featurebyte.models.feature import (
    DefaultVersionMode,
    FeatureModel,
    FeatureNamespaceModel,
    FeatureReadiness,
)
from featurebyte.persistent import Persistent
from featurebyte.routes.common.base import BaseController
from featurebyte.schema.feature_namespace import (
    FeatureNamespaceCreate,
    FeatureNamespaceList,
    FeatureNamespaceUpdate,
)


class FeatureNamespaceController(BaseController[FeatureNamespaceModel, FeatureNamespaceList]):
    """
    FeatureName controller
    """

    collection_name = FeatureNamespaceModel.collection_name()
    document_class = FeatureNamespaceModel
    paginated_document_class = FeatureNamespaceList

    @classmethod
    async def create_feature_namespace(
        cls, user: Any, persistent: Persistent, data: FeatureNamespaceCreate
    ) -> FeatureNamespaceModel:
        """
        Create Feature Namespace at persistent

        Parameters
        ----------
        user: Any
            User class to provide user identifier
        persistent: Persistent
            Object that feature namespace will be saved to
        data: FeatureNamespaceCreate
            FeatureNamespace creation payload

        Returns
        -------
        FeatureNamespaceModel
            Newly created feature store document
        """
        document = FeatureNamespaceModel(**data.json_dict(), user_id=user.id)
        # check any conflict with existing documents
        await cls.check_document_unique_constraints(
            persistent=persistent,
            user_id=user.id,
            document=document,
        )
        insert_id = await persistent.insert_one(
            collection_name=cls.collection_name,
            document=document.dict(by_alias=True),
            user_id=user.id,
        )
        assert insert_id == document.id
        return await cls.get(user=user, persistent=persistent, document_id=insert_id)

    @classmethod
    async def update_feature_namespace(
        cls,
        user: Any,
        persistent: Persistent,
        feature_namespace_id: ObjectId,
        data: FeatureNamespaceUpdate,
    ) -> FeatureNamespaceModel:
        """
        Update FeatureNamespace stored at persistent (GitDB or MongoDB)

        Parameters
        ----------
        user: Any
            User class to provide user identifier
        persistent: Persistent
            Object that entity will be saved to
        feature_namespace_id: ObjectId
            FeatureNamespace ID
        data: FeatureNamespaceUpdate
            FeatureNamespace update payload

        Returns
        -------
        FeatureNamespaceModel
            FeatureNamespace object with updated attribute(s)

        Raises
        ------
        HTTPException
            When the feature namespace has different name from the feature name
        """

        feature_namespace = await cls.get(
            user=user,
            persistent=persistent,
            document_id=feature_namespace_id,
            exception_detail=f'FeatureNamespace (id: "{feature_namespace_id}") not found.',
        )

        version_ids = list(feature_namespace.version_ids)
        default_version_id = feature_namespace.default_version_id
        readiness = FeatureReadiness(feature_namespace.readiness)
        default_version_mode = DefaultVersionMode(feature_namespace.default_version_mode)

        if data.default_version_mode:
            default_version_mode = DefaultVersionMode(data.default_version_mode)

        if data.version_id:
            # check whether the feature is saved to persistent or not
            feature = await cls.get_document(
                user=user,
                persistent=persistent,
                collection_name=FeatureModel.collection_name(),
                document_id=data.version_id,
            )
            if feature["name"] != feature_namespace.name:
                # sanity check that the feature namespace id has consistent name with feature name
                raise HTTPException(
                    status_code=HTTPStatus.UNPROCESSABLE_ENTITY,
                    detail=(
                        f'Feature (name: "{feature["name"]}") has an inconsistent '
                        f'feature_namespace_id (name: "{feature_namespace.name}").'
                    ),
                )

            version_ids.append(feature["_id"])
            readiness = max(readiness, FeatureReadiness(feature["readiness"]))
            if (
                feature_namespace.default_version_mode == DefaultVersionMode.AUTO
                and feature["readiness"] >= feature_namespace.readiness
            ):
                # if default version mode is AUTO, use the latest best readiness feature as default feature
                default_version_id = feature["_id"]

        update_count = await persistent.update_one(
            collection_name=cls.collection_name,
            query_filter={"_id": ObjectId(feature_namespace_id)},
            update={
                "$set": {
                    "version_ids": version_ids,
                    "readiness": readiness.value,
                    "default_version_id": default_version_id,
                    "default_version_mode": default_version_mode.value,
                }
            },
        )
        assert update_count == 1
        return await cls.get(user=user, persistent=persistent, document_id=feature_namespace_id)
