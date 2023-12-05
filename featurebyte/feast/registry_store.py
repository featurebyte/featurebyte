"""
Featurebyte registry store
"""
from pathlib import Path

# pylint: disable=no-name-in-module
from feast.infra.registry.file import FileRegistryStore
from feast.protos.feast.core.Registry_pb2 import Registry as RegistryProto
from feast.repo_config import RegistryConfig


class FeatureByteRegistryStore(FileRegistryStore):
    """FeatureByte registry store"""

    def __init__(self, registry_config: RegistryConfig, repo_path: Path):
        super().__init__(registry_config, repo_path)

        # read registry from file & store in memory
        self._registry_proto = super().get_registry_proto()

    def get_registry_proto(self):
        """Get registry proto"""
        return self._registry_proto

    def _write_registry(self, registry_proto: RegistryProto):
        pass
