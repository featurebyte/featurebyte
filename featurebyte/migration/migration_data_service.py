"""
Migration related service class(es)
"""

from featurebyte.migration.model import (
    SchemaMetadataCreate,
    SchemaMetadataModel,
    SchemaMetadataUpdate,
)
from featurebyte.service.base_document import BaseDocumentService
from featurebyte.service.mixin import GetOrCreateMixin


class SchemaMetadataService(
    BaseDocumentService[SchemaMetadataModel, SchemaMetadataCreate, SchemaMetadataUpdate],
    GetOrCreateMixin[SchemaMetadataModel, SchemaMetadataCreate],
):
    """SchemaMetadataService class"""

    document_class = SchemaMetadataModel
    document_create_class = SchemaMetadataCreate
