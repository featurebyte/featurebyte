"""
Feature API route controller
"""
from __future__ import annotations

from typing import Any

from http import HTTPStatus

from bson.objectid import ObjectId
from fastapi import HTTPException

from featurebyte.enum import CollectionName
from featurebyte.models.feature import DefaultVersionMode, FeatureReadiness
from featurebyte.persistent import Persistent
from featurebyte.routes.common.util import get_utc_now
from featurebyte.schema.feature import Feature, FeatureCreate, FeatureNameSpace


class FeatureController:
    """
    Feature controller
    """

    # pylint: disable=too-few-public-methods

    collection_name = CollectionName.FEATURE

    @classmethod
    def create_feature(cls, user: Any, persistent: Persistent, data: FeatureCreate) -> Feature:
        """
        Create Feature at persistent (GitDB or MongoDB)

        Parameters
        ----------
        user: Any
            User class to provide user identifier
        persistent: Persistent
            Object that entity will be saved to
        data: FeatureCreate
            Feature creation payload

        Returns
        -------
        Feature
            Newly created feature object

        Raises
        ------
        HTTPException
            If the feature name conflicts with existing feature name
        """

        # pylint: disable=too-many-locals

        utcnow = get_utc_now()
        document = Feature(
            user_id=user.id,
            created_at=utcnow,
            readiness=FeatureReadiness.DRAFT,
            **data.dict(),
        )

        with persistent.start_transaction() as session:
            conflict_entity = session.find_one(
                collection_name=cls.collection_name, query_filter={"name": data.name}
            )
            if conflict_entity:
                raise HTTPException(
                    status_code=HTTPStatus.CONFLICT,
                    detail=f'Feature name "{data.name}" already exists.',
                )

            for event_data_id in document.event_data_ids:
                event_data = session.find_one(
                    collection_name=CollectionName.EVENT_DATA,
                    query_filter={"_id": ObjectId(event_data_id)},
                )
                if not event_data:
                    raise HTTPException(
                        status_code=HTTPStatus.UNPROCESSABLE_ENTITY,
                        detail=(
                            f'EventData ID "{event_data_id}" not found! '
                            f"Please save the EventData object!",
                        ),
                    )

            insert_id = session.insert_one(
                collection_name=cls.collection_name, document=document.dict(by_alias=True)
            )
            assert insert_id == document.id

            feature_namespace_dict = session.find_one(
                collection_name=CollectionName.FEATURE_NAMESPACE,
                query_filter={"name": document.name},
            )
            if feature_namespace_dict:
                feature_namespace = FeatureNameSpace(**feature_namespace_dict)
                feature_versions = [ObjectId(version) for version in feature_namespace.versions]
                feature_versions.append(insert_id)
                namespace_readiness = feature_namespace.readiness
                default_version_id: ObjectId = feature_namespace.default_version_id
                if feature_namespace.default_version_mode == DefaultVersionMode.AUTO:
                    namespace_readiness = max(namespace_readiness, FeatureReadiness.DRAFT)
                    if namespace_readiness == FeatureReadiness.DRAFT:
                        default_version_id = insert_id

                session.update_one(
                    collection_name=CollectionName.FEATURE_NAMESPACE,
                    query_filter={"_id": feature_namespace.id},
                    update={
                        "$set": {
                            "versions": feature_versions,
                            "readiness": namespace_readiness,
                            "default_version_id": default_version_id,
                        }
                    },
                )
            else:
                doc_feature_namespace = FeatureNameSpace(
                    name=document.name,
                    versions=[insert_id],
                    readiness=FeatureReadiness.DRAFT,
                    created_at=utcnow,
                    default_version_id=insert_id,
                    default_version_mode=DefaultVersionMode.AUTO,
                )
                session.insert_one(
                    collection_name=CollectionName.FEATURE_NAMESPACE,
                    document=doc_feature_namespace.dict(by_alias=True),
                )

        return document
