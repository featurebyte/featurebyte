"""
Migration service for data warehouse working schema
"""
from __future__ import annotations

from typing import Any, Optional

import textwrap

import pandas as pd
from snowflake.connector.errors import ProgrammingError

from featurebyte.feature_manager.model import ExtendedFeatureModel
from featurebyte.migration.service import migrate
from featurebyte.migration.service.mixin import DataWarehouseMigrationMixin
from featurebyte.models.feature_store import FeatureStoreModel
from featurebyte.persistent.base import Persistent
from featurebyte.service.feature import FeatureService
from featurebyte.session.base import BaseSession


class TileColumnTypeExtractor:
    """
    Responsible for building a mapping from tile column names to tile types based on saved Features
    """

    def __init__(self, user: Any, persistent: Persistent):
        self.feature_service = FeatureService(user, persistent)
        self.tile_column_name_to_type: Optional[dict[str, str]] = None

    async def setup(self) -> None:
        """
        Set up the object by loading existing Feature documents
        """
        self.tile_column_name_to_type = await self._build_tile_column_name_to_type_mapping(
            self.feature_service
        )

    @staticmethod
    async def _build_tile_column_name_to_type_mapping(
        feature_service: FeatureService,
    ) -> dict[str, str]:
        tile_column_name_to_type = {}
        feature_documents = feature_service.list_documents_iterator({})
        async for doc in feature_documents:
            feature_model = ExtendedFeatureModel(**doc)
            for tile_spec in feature_model.tile_specs:
                for tile_column_name, tile_column_type in zip(
                    tile_spec.value_column_names, tile_spec.value_column_types
                ):
                    tile_column_name_to_type[tile_column_name] = tile_column_type
        return tile_column_name_to_type

    def get_tile_column_type(self, tile_column_name: str) -> str | None:
        """
        Get tile column type for the given tile column name. Return None if the column type is not
        known (when none of the existing Feature corresponds to the tile column; could be due to
        unsaved features or TILE_REGISTRY table being in a bad state)

        Parameters
        ----------
        tile_column_name: str
            Tile column name

        Returns
        -------
        str | None
        """
        assert self.tile_column_name_to_type is not None
        return self.tile_column_name_to_type.get(tile_column_name)

    def get_tile_column_types_from_names(self, value_column_names: pd.Series) -> pd.Series:
        """
        Transform the VALUE_COLUMN_NAMES column from TILE_REGISTRY to VALUE_COLUMN_TYPES

        Parameters
        ----------
        value_column_names: Series
            Series of tile column names from TILE_REGISTRY

        Returns
        -------
        Series
        """

        def _transform(column_names: list[str]) -> str:
            column_types = []
            for column_name in column_names:
                column_type = self.get_tile_column_type(column_name)
                if column_type is None:
                    column_type = "FLOAT"
                column_types.append(column_type)
            return ",".join(column_types)

        return value_column_names.str.replace('"', "").str.split(",").apply(_transform)


class DataWarehouseMigrationService(DataWarehouseMigrationMixin):
    """
    DataWarehouseMigrationService class

    Responsible for migrating the featurebyte working schema in the data warehouse
    """

    extractor: TileColumnTypeExtractor

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

    async def migrate_record_with_session(
        self, feature_store: FeatureStoreModel, session: BaseSession
    ) -> None:
        _ = feature_store

        df_tile_registry = await session.execute_query("SELECT * FROM TILE_REGISTRY")
        if "VALUE_COLUMN_TYPES" in df_tile_registry:  # type: ignore[operator]
            return

        df_tile_registry["VALUE_COLUMN_TYPES"] = self.extractor.get_tile_column_types_from_names(  # type: ignore[index]
            df_tile_registry["VALUE_COLUMN_NAMES"]  # type: ignore[index]
        )
        await session.register_table(
            "UPDATED_TILE_REGISTRY",
            df_tile_registry[["TILE_ID", "VALUE_COLUMN_NAMES", "VALUE_COLUMN_TYPES"]],  # type: ignore[index]
        )

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

        # Update columns in tile tables where the type is not FLOAT. In such cases, tile generation
        # likely encountered error and updating to correct type should fix it.
        for _, row in df_tile_registry.iterrows():  # type: ignore[union-attr]
            tile_id = row["TILE_ID"]
            tile_column_names = row["VALUE_COLUMN_NAMES"].replace('"', "").split(",")
            tile_column_types = row["VALUE_COLUMN_TYPES"].split(",")
            for tile_column_name, tile_column_type in zip(tile_column_names, tile_column_types):
                if tile_column_type == "FLOAT":
                    continue
                try:
                    await session.execute_query(
                        f"""
                        ALTER TABLE {tile_id} DROP COLUMN {tile_column_name}
                        """
                    )
                    await session.execute_query(
                        f"""
                        ALTER TABLE {tile_id} ADD COLUMN {tile_column_name} {tile_column_type} DEFAULT NULL
                        """
                    )
                except ProgrammingError:
                    # ok if column tile table or tile column doesn't exist yet
                    pass
