"""
OnlineStoreTableVersion model
"""

from __future__ import annotations

from typing import List

import pymongo
from pydantic import StrictStr

from featurebyte.models.base import FeatureByteCatalogBaseDocumentModel, UniqueValuesConstraint
from featurebyte.query_graph.node.schema import TableDetails
from featurebyte.schema.common.base import BaseDocumentServiceUpdateSchema


class OnlineStoreTableVersion(FeatureByteCatalogBaseDocumentModel):
    """Model for a OnlineStoreTableVersion"""

    online_store_table_name: StrictStr
    aggregation_result_name: StrictStr
    version: int

    @property
    def warehouse_tables(self) -> list[TableDetails]:
        return [TableDetails(table_name=self.online_store_table_name)]

    class Settings(FeatureByteCatalogBaseDocumentModel.Settings):
        """
        MongoDB settings
        """

        collection_name: str = "online_store_table_version"
        unique_constraints: List[UniqueValuesConstraint] = [
            UniqueValuesConstraint(
                fields=("_id",),
                conflict_fields_signature={"id": ["_id"]},
                resolution_signature=None,
            ),
        ]
        indexes = FeatureByteCatalogBaseDocumentModel.Settings.indexes + [
            pymongo.operations.IndexModel("aggregation_result_name"),
        ]
        auditable = False


class OnlineStoreTableVersionUpdate(BaseDocumentServiceUpdateSchema):
    """
    Schema for OnlineStoreTableVersionUpdate
    """

    version: int
