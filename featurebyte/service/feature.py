"""
FeatureService class
"""
from __future__ import annotations

from typing import Any, Optional

from bson.objectid import ObjectId

from featurebyte.common.model_util import get_version
from featurebyte.core.generic import ExtendedFeatureStoreModel
from featurebyte.enum import SourceType, SpecialColumnName
from featurebyte.exception import (
    DocumentConflictError,
    DocumentInconsistencyError,
    DocumentNotFoundError,
    DuplicatedRegistryError,
)
from featurebyte.feature_manager.model import ExtendedFeatureModel
from featurebyte.feature_manager.snowflake_feature import FeatureManagerSnowflake
from featurebyte.logger import logger
from featurebyte.models.base import VersionIdentifier
from featurebyte.models.event_data import EventDataModel
from featurebyte.models.feature import (
    DefaultVersionMode,
    FeatureModel,
    FeatureNamespaceModel,
    FeatureReadiness,
)
from featurebyte.models.feature_store import FeatureStoreModel
from featurebyte.query_graph.feature_preview import get_feature_preview_sql
from featurebyte.schema.feature import (
    FeatureBriefInfoList,
    FeatureCreate,
    FeatureInfo,
    FeaturePreview,
    FeatureServiceUpdate,
)
from featurebyte.schema.feature_namespace import (
    FeatureNamespaceCreate,
    FeatureNamespaceServiceUpdate,
)
from featurebyte.service.base_document import BaseDocumentService, GetInfoServiceMixin
from featurebyte.service.feature_namespace import FeatureNamespaceService


async def validate_feature_version_and_namespace_consistency(
    feature: FeatureModel, feature_namespace: FeatureNamespaceModel
) -> None:
    """
    Validate whether the feature list & feature list namespace are consistent

    Parameters
    ----------
    feature: FeatureModel
        Feature object
    feature_namespace: FeatureNamespaceModel
        FeatureNamespace object

    Raises
    ------
    DocumentInconsistencyError
        If the inconsistency between version & namespace found
    """
    attrs = ["name", "dtype", "entity_ids", "event_data_ids"]
    for attr in attrs:
        version_attr = getattr(feature, attr)
        namespace_attr = getattr(feature_namespace, attr)
        version_attr_str: str | list[str] = f'"{version_attr}"'
        namespace_attr_str: str | list[str] = f'"{namespace_attr}"'
        if isinstance(version_attr, list):
            version_attr = sorted(version_attr)
            version_attr_str = [str(val) for val in version_attr]

        if isinstance(namespace_attr, list):
            namespace_attr = sorted(namespace_attr)
            namespace_attr_str = [str(val) for val in namespace_attr]

        if version_attr != namespace_attr:
            raise DocumentInconsistencyError(
                f'Feature (name: "{feature.name}") object(s) within the same namespace '
                f'must have the same "{attr}" value (namespace: {namespace_attr_str}, '
                f"feature: {version_attr_str})."
            )


class FeatureService(BaseDocumentService[FeatureModel], GetInfoServiceMixin[FeatureInfo]):
    """
    FeatureService class
    """

    document_class = FeatureModel

    async def _insert_feature_registry(
        self, document: ExtendedFeatureModel, get_credential: Any
    ) -> None:
        """
        Insert feature registry into feature store

        Parameters
        ----------
        document: ExtendedFeatureModel
            Feature document
        get_credential: Any
            Get credential handler function

        Raises
        ------
        DocumentConflictError
            When the feature registry already exists at the feature store
        Exception
            Other errors during registry insertion / removal
        """
        feature_store = document.feature_store
        if feature_store.type == SourceType.SNOWFLAKE:
            db_session = await self._get_feature_store_session(
                feature_store=feature_store,
                get_credential=get_credential,
            )
            feature_manager = FeatureManagerSnowflake(session=db_session)
            try:
                feature_manager.insert_feature_registry(document)
            except DuplicatedRegistryError as exc:
                # someone else already registered the feature at snowflake
                # do not remove the current registry & raise error to remove persistent record
                raise DocumentConflictError(
                    f'Feature (name: "{document.name}") has been registered by '
                    f"other feature at Snowflake feature store."
                ) from exc
            except Exception as exc:  # pylint: disable=broad-except
                logger.error(f"error with insert_feature_registry: {exc}")
                # for other exceptions, cleanup feature registry record & persistent record
                try:
                    feature_manager.remove_feature_registry(document)
                except Exception as remove_exc:  # pylint: disable=broad-except
                    raise remove_exc from exc

    async def _get_feature_version(self, name: str) -> VersionIdentifier:
        version_name = get_version()
        _, count = await self.persistent.find(
            collection_name=self.collection_name,
            query_filter={"name": name, "version.name": version_name},
        )
        return VersionIdentifier(name=version_name, suffix=count or None)

    async def create_document(  # type: ignore[override]
        self, data: FeatureCreate, get_credential: Any = None
    ) -> FeatureModel:
        document = FeatureModel(
            **{
                **data.json_dict(),
                "readiness": FeatureReadiness.DRAFT,
                "version": await self._get_feature_version(data.name),
                "user_id": self.user.id,
            }
        )

        async with self.persistent.start_transaction() as session:
            # check any conflict with existing documents
            await self._check_document_unique_constraints(document=document)

            # check event_data has been saved at persistent storage or not
            for event_data_id in data.event_data_ids:
                _ = await self._get_document(
                    document_id=event_data_id,
                    collection_name=EventDataModel.collection_name(),
                )

            insert_id = await session.insert_one(
                collection_name=self.collection_name,
                document=document.dict(by_alias=True),
                user_id=self.user.id,
            )
            assert insert_id == document.id

            feature_namespace_service = FeatureNamespaceService(
                user=self.user, persistent=self.persistent
            )
            try:
                feature_namespace = await feature_namespace_service.get_document(
                    document_id=document.feature_namespace_id,
                )
                await validate_feature_version_and_namespace_consistency(
                    feature=document, feature_namespace=feature_namespace
                )
                feature_namespace = await feature_namespace_service.update_document(
                    document_id=document.feature_namespace_id,
                    data=FeatureNamespaceServiceUpdate(
                        feature_ids=self.include_object_id(
                            feature_namespace.feature_ids, document.id
                        )
                    ),
                    return_document=True,
                )  # type: ignore[assignment]
                assert feature_namespace is not None

            except DocumentNotFoundError:
                feature_namespace = await feature_namespace_service.create_document(
                    data=FeatureNamespaceCreate(
                        _id=document.feature_namespace_id,
                        name=document.name,
                        dtype=document.dtype,
                        feature_ids=[insert_id],
                        readiness=FeatureReadiness.DRAFT,
                        default_feature_id=insert_id,
                        default_version_mode=DefaultVersionMode.AUTO,
                        entity_ids=sorted(document.entity_ids),
                        event_data_ids=sorted(document.event_data_ids),
                    ),
                )

            # insert feature registry into feature store
            feature_store_dict = await self._get_document(
                document_id=data.tabular_source.feature_store_id,
                collection_name=FeatureStoreModel.collection_name(),
            )
            feature_store = ExtendedFeatureStoreModel(**feature_store_dict)
            extended_feature = ExtendedFeatureModel(
                **document.dict(by_alias=True),
                is_default=document.id == feature_namespace.default_feature_id,
                feature_store=feature_store,
            )
            await self._insert_feature_registry(extended_feature, get_credential)
        return await self.get_document(document_id=insert_id)

    async def update_document(  # type: ignore[override]
        self,
        document_id: ObjectId,
        data: FeatureServiceUpdate,
        exclude_none: bool = True,
        document: Optional[FeatureModel] = None,
        return_document: bool = True,
    ) -> Optional[FeatureModel]:
        # pylint: disable=duplicate-code
        if document is None:
            await self.get_document(document_id=document_id)

        await self.persistent.update_one(
            collection_name=self.collection_name,
            query_filter=self._construct_get_query_filter(document_id=document_id),
            update={"$set": data.dict(exclude_none=exclude_none)},
            user_id=self.user.id,
        )

        if return_document:
            return await self.get_document(document_id=document_id)
        return None

    async def get_info(self, document_id: ObjectId, verbose: bool) -> FeatureInfo:
        feature = await self.get_document(document_id=document_id)
        feature_namespace_service = FeatureNamespaceService(
            user=self.user, persistent=self.persistent
        )
        namespace_info = await feature_namespace_service.get_info(
            document_id=feature.feature_namespace_id,
            verbose=verbose,
        )
        default_feature = await self.get_document(document_id=namespace_info.default_feature_id)
        versions_info = None
        if verbose:
            namespace = await feature_namespace_service.get_document(
                document_id=feature.feature_namespace_id
            )
            versions_info = FeatureBriefInfoList.from_paginated_data(
                await self.list_documents(
                    page=1,
                    page_size=0,
                    query_filter={"_id": {"$in": namespace.feature_ids}},
                )
            )

        return FeatureInfo(
            **namespace_info.dict(),
            version={"this": feature.version.to_str(), "default": default_feature.version.to_str()},
            readiness={"this": feature.readiness, "default": default_feature.readiness},
            versions_info=versions_info,
        )

    async def preview(self, feature_preview: FeaturePreview, get_credential: Any) -> str:
        """
        Preview a Feature

        Parameters
        ----------
        feature_preview: FeaturePreview
            FeaturePreview object
        get_credential: Any
            Get credential handler function

        Returns
        -------
        str
            Dataframe converted to json string

        Raises
        ------
        KeyError
            Invalid point_in_time_and_serving_name payload
        """
        graph = feature_preview.feature.graph
        point_in_time_and_serving_name = feature_preview.point_in_time_and_serving_name

        if SpecialColumnName.POINT_IN_TIME not in point_in_time_and_serving_name:
            raise KeyError(f"Point in time column not provided: {SpecialColumnName.POINT_IN_TIME}")

        inception_node = graph.get_node_by_name(feature_preview.feature.row_index_lineage[0])
        serving_names = inception_node.parameters.serving_names
        if serving_names is not None:
            for col in serving_names:
                if col not in point_in_time_and_serving_name:
                    raise KeyError(f"Serving name not provided: {col}")

        feature_store_dict = graph.nodes["input_1"]["parameters"]["feature_store"]
        db_session = await self._get_feature_store_session(
            feature_store=FeatureStoreModel(
                **feature_store_dict, name=feature_preview.feature_store_name
            ),
            get_credential=get_credential,
        )
        preview_sql = get_feature_preview_sql(
            graph=graph,
            nodes=[feature_preview.feature.node],
            point_in_time_and_serving_name=feature_preview.point_in_time_and_serving_name,
        )
        result = db_session.execute_query(preview_sql)
        return self._convert_dataframe_as_json(result)
