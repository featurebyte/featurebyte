"""
Base namespace service
"""
from featurebyte.common.utils import get_version
from featurebyte.models.base import VersionIdentifier
from featurebyte.schema.common.base import BaseDocumentServiceUpdateSchema
from featurebyte.service.base_document import BaseDocumentService
from featurebyte.service.mixin import Document, DocumentCreateSchema


class BaseNamespaceService(
    BaseDocumentService[Document, DocumentCreateSchema, BaseDocumentServiceUpdateSchema]
):
    """
    Base namespace service
    """

    async def get_document_version(self, name: str) -> VersionIdentifier:
        version_name = get_version()
        query_result = await self.list_documents(
            query_filter={"name": name, "version.name": version_name}
        )
        count = query_result["total"]
        return VersionIdentifier(name=version_name, suffix=count or None)
