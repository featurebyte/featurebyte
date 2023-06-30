"""
Namespace service
"""
from featurebyte import get_version
from featurebyte.models.base import VersionIdentifier


class NamespaceServiceMixin:
    async def _get_feature_version(self, name: str) -> VersionIdentifier:
        version_name = get_version()
        query_result = await self.list_documents(
            query_filter={"name": name, "version.name": version_name}
        )
        count = query_result["total"]
        return VersionIdentifier(name=version_name, suffix=count or None)
