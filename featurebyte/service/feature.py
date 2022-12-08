"""
FeatureService class
"""
from __future__ import annotations

from featurebyte.common.model_util import get_version
from featurebyte.exception import DocumentInconsistencyError, DocumentNotFoundError
from featurebyte.models.base import VersionIdentifier
from featurebyte.models.feature import (
    DefaultVersionMode,
    FeatureModel,
    FeatureNamespaceModel,
    FeatureReadiness,
)
from featurebyte.schema.feature import FeatureCreate, FeatureServiceUpdate
from featurebyte.schema.feature_namespace import (
    FeatureNamespaceCreate,
    FeatureNamespaceServiceUpdate,
)
from featurebyte.service.base_document import BaseDocumentService
from featurebyte.service.feature_namespace import FeatureNamespaceService
from featurebyte.service.tabular_data import DataService


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
    attrs = ["name", "dtype", "entity_ids", "tabular_data_ids"]
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


class FeatureService(BaseDocumentService[FeatureModel, FeatureCreate, FeatureServiceUpdate]):
    """
    FeatureService class
    """

    document_class = FeatureModel

    async def _get_feature_version(self, name: str) -> VersionIdentifier:
        version_name = get_version()
        _, count = await self.persistent.find(
            collection_name=self.collection_name,
            query_filter={"name": name, "version.name": version_name},
        )
        return VersionIdentifier(name=version_name, suffix=count or None)

    async def create_document(self, data: FeatureCreate) -> FeatureModel:
        document = FeatureModel(
            **{
                **data.json_dict(),
                "readiness": FeatureReadiness.DRAFT,
                "version": await self._get_feature_version(data.name),
                "user_id": self.user.id,
            }
        )

        # prepare the raw graph (without getting aggressively pruned) & aggressively pruned graph
        raw_graph = document.graph
        graph, node_name = document.graph.prepare_to_store(target_node=document.node)

        async with self.persistent.start_transaction() as session:
            # check any conflict with existing documents
            await self._check_document_unique_constraints(document=document)

            # check whether data has been saved at persistent storage
            data_service = DataService(user=self.user, persistent=self.persistent)
            for tabular_data_id in data.tabular_data_ids:
                _ = await data_service.get_document(document_id=tabular_data_id)

            insert_id = await session.insert_one(
                collection_name=self.collection_name,
                document={
                    **document.dict(by_alias=True),
                    "graph": graph.dict(),
                    "node_name": node_name,
                    "raw_graph": raw_graph.dict(),
                },
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
                await feature_namespace_service.update_document(
                    document_id=document.feature_namespace_id,
                    data=FeatureNamespaceServiceUpdate(
                        feature_ids=self.include_object_id(
                            feature_namespace.feature_ids, document.id
                        )
                    ),
                    return_document=True,
                )
            except DocumentNotFoundError:
                await feature_namespace_service.create_document(
                    data=FeatureNamespaceCreate(
                        _id=document.feature_namespace_id,
                        name=document.name,
                        dtype=document.dtype,
                        feature_ids=[insert_id],
                        readiness=FeatureReadiness.DRAFT,
                        default_feature_id=insert_id,
                        default_version_mode=DefaultVersionMode.AUTO,
                        entity_ids=sorted(document.entity_ids),
                        tabular_data_ids=sorted(document.tabular_data_ids),
                    ),
                )
        return await self.get_document(document_id=insert_id)

    async def get_document_by_name_and_version(
        self, name: str, version: VersionIdentifier
    ) -> FeatureModel:
        """
        Retrieve feature given name & version

        Parameters
        ----------
        name: str
            Feature name
        version: VersionIdentifier
            Feature version

        Returns
        -------
        FeatureModel

        Raises
        ------
        DocumentNotFoundError
            If the specified feature name & version cannot be found
        """
        document_dict = await self.persistent.find_one(
            collection_name=self.collection_name,
            query_filter={"name": name, "version": version.dict()},
            user_id=self.user.id,
        )
        if document_dict is None:
            exception_detail = (
                f'{self.class_name} (name: "{name}", version: "{version.to_str()}") not found. '
                f"Please save the {self.class_name} object first."
            )
            raise DocumentNotFoundError(exception_detail)
        return FeatureModel(**document_dict)
