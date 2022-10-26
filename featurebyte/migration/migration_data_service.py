"""
Migration related service class(es)
"""
from featurebyte.migration.model import (
    MigrationMetadata,
    SchemaMetadataCreate,
    SchemaMetadataModel,
    SchemaMetadataUpdate,
)
from featurebyte.service.base_document import BaseDocumentService


class SchemaMetadataService(
    BaseDocumentService[SchemaMetadataModel, SchemaMetadataCreate, SchemaMetadataUpdate]
):
    """SchemaMetadataService class"""

    document_class = SchemaMetadataModel

    async def get_or_create_document(self) -> SchemaMetadataModel:
        """
        Get schema metadata document (create the document if it does not exist)

        Returns
        -------
        SchemaMetadataModel
        """
        documents = await self.list_documents(
            page=1, page_size=0, query_filter={"name": MigrationMetadata.SCHEMA_METADATA.value}
        )
        if documents["data"]:
            return SchemaMetadataModel(**documents["data"][0])
        return await self.create_document(data=SchemaMetadataCreate(**SchemaMetadataModel().dict()))
