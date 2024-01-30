"""
Feast registry model
"""
from typing import Optional

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

    offline_table_name_prefix: Optional[str] = Field(default=None)
    registry: bytes
    feature_store_id: PydanticObjectId

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
