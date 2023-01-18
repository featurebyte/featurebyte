"""
Migration service for data warehouse working schema
"""
from __future__ import annotations

from typing import Any, Optional

import textwrap

import pandas as pd
from pydantic import Field

from featurebyte.feature_manager.model import ExtendedFeatureModel
from featurebyte.migration.service import migrate
from featurebyte.migration.service.mixin import DataWarehouseMigrationMixin
from featurebyte.models.feature_store import FeatureStoreModel
from featurebyte.models.persistent import Document
from featurebyte.persistent.base import Persistent
from featurebyte.service.feature import FeatureService
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.utils.credential import get_credential


class TileColumnTypeExtractor:
    """
    Responsible for building a mapping from tile column names to tile types based on saved Features
    """

    def __init__(self, user: Any, persistent: Persistent):
        self.feature_service = FeatureService(user, persistent)
        self.tile_column_name_to_type = None

    async def setup(self) -> None:
        self.tile_column_name_to_type = await self._build_tile_column_name_to_type_mapping(
            self.feature_service
        )

    @staticmethod
    async def _build_tile_column_name_to_type_mapping(
        feature_service: FeatureService,
    ) -> dict[str, str]:
        tile_column_name_to_type = {}
        feature_documents = await feature_service.list_documents(page_size=0)
        for doc in feature_documents["data"]:
            feature_model = ExtendedFeatureModel(**doc)
            for tile_spec in feature_model.tile_specs:
                for tile_column_name, tile_column_type in zip(
                    tile_spec.value_column_names, tile_spec.value_column_types
                ):
                    tile_column_name_to_type[tile_column_name] = tile_column_type
        return tile_column_name_to_type

    def get_tile_column_type(self, tile_column_name: str) -> str | None:
        return self.tile_column_name_to_type.get(tile_column_name)

    def transform_value_column_names(self, value_column_names: pd.Series) -> pd.Series:
        def _transform(column_names: list[str]) -> str | None:
            column_types = []
            for column_name in column_names:
                column_type = self.get_tile_column_type(column_name)
                if column_type is None:
                    return None
                column_types.append(column_type)
            return ",".join(column_types)

        return value_column_names.str.replace('"', "").str.split(",").apply(_transform)


class DataWarehouseMigrationService(FeatureStoreService, DataWarehouseMigrationMixin):

    extractor: Optional[TileColumnTypeExtractor] = Field(default=None)

    @migrate(version=6, description="Add VALUE_COLUMN_TYPES column in TILE_REGISTRY table")
    async def add_tile_value_types_column(self) -> None:
        """
        Add VALUE_COLUMN_TYPES column in TILE_REGISTRY table
        """
        collection_names = await self.persistent.list_collection_names()
        if "feature_store" not in collection_names:
            return

        tile_column_type_extractor = TileColumnTypeExtractor(self.user, self.persistent)
        await tile_column_type_extractor.setup()
        self.extractor = tile_column_type_extractor

        # migrate all records and audit records
        await self.migrate_all_records()

    async def migrate_record(self, document: Document) -> None:
        feature_store = FeatureStoreModel(**document)
        session_manager_service = self.get_session_manager_service(self.user, self.persistent)
        session = await session_manager_service.get_feature_store_session(
            feature_store, get_credential=get_credential
        )
        df_tile_registry = await session.execute_query(
            "SELECT TILE_ID, VALUE_COLUMN_NAMES FROM TILE_REGISTRY"
        )
        df_tile_registry["VALUE_COLUMN_TYPES"] = self.extractor.transform_value_column_names(
            df_tile_registry["VALUE_COLUMN_NAMES"]
        )
        await session.register_table("UPDATED_TILE_REGISTRY", df_tile_registry)

        await session.execute_query(
            "ALTER TABLE TILE_REGISTRY ADD COLUMN VALUE_COLUMN_TYPES VARCHAR"
        )

        update_query = textwrap.dedent(
            """
            UPDATE TILE_REGISTRY
            SET VALUE_COLUMN_TYPES = UPDATED_TILE_REGISTRY.VALUE_COLUMN_TYPES
            FROM UPDATED_TILE_REGISTRY
            WHERE TILE_REGISTRY.TILE_ID = UPDATED_TILE_REGISTRY.TILE_ID
            """
        ).strip()
        await session.execute_query(update_query)
