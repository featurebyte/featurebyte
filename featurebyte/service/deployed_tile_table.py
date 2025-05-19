"""
DeployedTileTableService class
"""

from __future__ import annotations

from featurebyte.models.deployed_tile_table import DeployedTileTableModel
from featurebyte.schema.common.base import BaseDocumentServiceUpdateSchema
from featurebyte.service.base_document import BaseDocumentService


class DeployedTileTableService(
    BaseDocumentService[
        DeployedTileTableModel, DeployedTileTableModel, BaseDocumentServiceUpdateSchema
    ]
):
    """
    DeployedTileTableService class
    """

    document_class = DeployedTileTableModel
