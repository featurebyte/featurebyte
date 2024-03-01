"""
Feast registry model
"""
from typing import List, Optional

from pathlib import Path

import pymongo

# pylint: disable=no-name-in-module
from feast.protos.feast.core.Registry_pb2 import Registry as RegistryProto
from pydantic import Field

from featurebyte.models.base import (
    FeatureByteCatalogBaseDocumentModel,
    PydanticObjectId,
    UniqueValuesConstraint,
)


class FeastRegistryModel(FeatureByteCatalogBaseDocumentModel):
    """Feast registry model"""

    offline_table_name_prefix: str
    registry: bytes = Field(default_factory=bytes, exclude=True)
    feature_store_id: PydanticObjectId
    registry_path: Optional[str] = Field(default=None)

    @property
    def remote_attribute_paths(self) -> List[Path]:
        paths = []
        if self.registry_path:
            paths.append(Path(self.registry_path))
        return paths

    def registry_proto(self) -> RegistryProto:
        """
        Get feast registry proto

        Returns
        -------
        RegistryProto
            Feast registry proto
        """
        registry_proto = RegistryProto()
        registry_proto.ParseFromString(self.registry)
        return registry_proto

    class Settings(FeatureByteCatalogBaseDocumentModel.Settings):
        """
        MongoDB settings
        """

        collection_name = "feast_registry"
        unique_constraints = [
            UniqueValuesConstraint(
                fields=("_id",),
                conflict_fields_signature={"id": ["_id"]},
                resolution_signature=None,
            ),
            UniqueValuesConstraint(
                fields=("name",),
                conflict_fields_signature={"id": ["_id"]},
                resolution_signature=None,
            ),
        ]
        indexes = FeatureByteCatalogBaseDocumentModel.Settings.indexes + [
            pymongo.operations.IndexModel("feature_store_id"),
        ]
        auditable = False
