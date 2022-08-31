"""
FeatureListService class
"""
from __future__ import annotations

from typing import Any, List, Optional

from bson.objectid import ObjectId

from featurebyte.core.generic import ExtendedFeatureStoreModel
from featurebyte.enum import SourceType
from featurebyte.exception import (
    DocumentConflictError,
    DocumentInconsistencyError,
    DuplicatedRegistryError,
)
from featurebyte.feature_manager.model import ExtendedFeatureListModel
from featurebyte.feature_manager.snowflake_feature_list import FeatureListManagerSnowflake
from featurebyte.models.base import FeatureByteBaseModel
from featurebyte.models.feature import (
    FeatureListModel,
    FeatureModel,
    FeatureReadiness,
    FeatureSignature,
)
from featurebyte.models.feature_store import FeatureStoreModel
from featurebyte.schema.feature_list import FeatureListCreate
from featurebyte.service.base_document import BaseDocumentService
from featurebyte.service.common.operation import DictProject, DictTransform


class FeatureListService(BaseDocumentService[FeatureListModel]):
    """
    FeatureListService class
    """

    document_class = FeatureListModel
    info_transform = DictTransform(
        rule={
            **BaseDocumentService.base_info_transform_rule,
            "__root__": DictProject(rule=["readiness", "status"]),
            "features": DictProject(rule="feature"),
        }
    )
    foreign_key_map = {"feature_ids": FeatureModel.collection_name()}

    async def _insert_feature_list_registry(
        self,
        document: ExtendedFeatureListModel,
        feature_store: ExtendedFeatureStoreModel,
        get_credential: Any,
    ) -> None:
        """
        Insert feature list registry into feature list store

        Parameters
        ----------
        document: ExtendedFeatureListModel
            ExtendedFeatureList document
        feature_store: ExtendedFeatureStoreModel
            FeatureStore document
        get_credential: Any
            Get credential handler function

        Raises
        ------
        HTTPException
            When the feature registry already exists at the feature store
        """
        if feature_store.type == SourceType.SNOWFLAKE:
            db_session = feature_store.get_session(
                credentials={
                    feature_store.name: await get_credential(
                        user_id=self.user.id, feature_store_name=feature_store.name
                    )
                }
            )
            feature_list_manager = FeatureListManagerSnowflake(session=db_session)
            try:
                feature_list_manager.insert_feature_list_registry(document)
            except DuplicatedRegistryError as exc:
                # someone else already registered the feature at snowflake
                # do not remove the current registry & raise error to remove persistent record
                raise DocumentConflictError(
                    f'FeatureList (name: "{document.name}") has been registered by '
                    f"other feature list at Snowflake feature list store."
                ) from exc

    async def create_document(
        self, data: FeatureListCreate, get_credential: Any = None
    ) -> FeatureListModel:
        # sort feature_ids before saving to persistent storage to ease feature_ids comparison in uniqueness check
        document = FeatureListModel(
            **{**data.json_dict(), "feature_ids": sorted(data.feature_ids), "user_id": self.user.id}
        )

        async with self.persistent.start_transaction() as session:
            # check any conflict with existing documents
            await self._check_document_unique_constraints(document=document)

            # check whether the feature(s) in the feature list saved to persistent or not
            feature_store_id: Optional[ObjectId] = None
            feature_signatures: List[FeatureSignature] = []
            feature_list_readiness: FeatureReadiness = FeatureReadiness.PRODUCTION_READY
            for feature_id in document.feature_ids:
                feature_dict = await self._get_document(
                    document_id=feature_id,
                    collection_name=FeatureModel.collection_name(),
                )
                feature = FeatureModel(**feature_dict)
                feature_list_readiness = min(
                    feature_list_readiness, FeatureReadiness(feature.readiness)
                )
                feature_signatures.append(
                    FeatureSignature(id=feature.id, name=feature.name, version=feature.version)
                )
                if feature_store_id and (
                    feature_store_id != feature.tabular_source.feature_store_id
                ):
                    raise DocumentInconsistencyError(
                        "All the Feature objects within the same FeatureList object must be from the same "
                        "feature store."
                    )

                # store previous feature store id
                feature_store_id = feature.tabular_source.feature_store_id

            # update document with readiness
            document = FeatureListModel(
                **{**document.dict(by_alias=True), "readiness": feature_list_readiness}
            )
            feature_store_dict = await self._get_document(
                document_id=ObjectId(feature_store_id),
                collection_name=FeatureStoreModel.collection_name(),
            )
            feature_store = ExtendedFeatureStoreModel(**feature_store_dict)

            insert_id = await session.insert_one(
                collection_name=self.collection_name,
                document=document.dict(by_alias=True),
                user_id=self.user.id,
            )
            assert insert_id == document.id

            # insert feature list registry into feature list store
            await self._insert_feature_list_registry(
                document=ExtendedFeatureListModel(
                    **document.dict(by_alias=True), features=feature_signatures
                ),
                feature_store=feature_store,
                get_credential=get_credential,
            )
        return await self.get_document(document_id=insert_id)

    async def update_document(
        self, document_id: ObjectId, data: FeatureByteBaseModel
    ) -> FeatureListModel:
        # TODO: implement proper logic to update feature list document
        return await self.get_document(document_id=document_id)
