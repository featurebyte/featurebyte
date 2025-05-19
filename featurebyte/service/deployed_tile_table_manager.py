"""
DeployedTileTableManagerService class
"""

from __future__ import annotations

from featurebyte.service.deployed_tile_table import DeployedTileTableService


class DeployedTileTableManagerService:
    """
    DeployedTileTableManager is responsible for managing deployed tile tables during deployment's
    enablement / disablement.
    """

    def __init__(self, deployed_tile_table_service: DeployedTileTableService) -> None:
        self.deployed_tile_table_service = deployed_tile_table_service
