"""
Migration service for data warehouse working schema
"""

from __future__ import annotations

import textwrap
from typing import Optional

import pandas as pd
from snowflake.connector.errors import ProgrammingError

from featurebyte.feature_manager.model import ExtendedFeatureModel
from featurebyte.logging import get_logger
from featurebyte.migration.service import migrate
from featurebyte.migration.service.mixin import DataWarehouseMigrationMixin
from featurebyte.models.feature_store import FeatureStoreModel
from featurebyte.models.persistent import QueryFilter
from featurebyte.persistent import Persistent
from featurebyte.service.feature import FeatureService
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.service.session_manager import SessionManagerService
from featurebyte.service.working_schema import WorkingSchemaService
from featurebyte.session.base import BaseSession

logger = get_logger(__name__)


class TileColumnTypeExtractor:
    """
    Responsible for building a mapping from tile column names to tile types based on saved Features
    """

    def __init__(self, feature_service: FeatureService):
        self.feature_service = feature_service
        self.tile_column_name_to_type: Optional[dict[str, str]] = None

    async def setup(self) -> None:
        """
        Set up the object by loading existing Feature documents
        """
        self.tile_column_name_to_type = await self._build_tile_column_name_to_type_mapping()

    async def _build_tile_column_name_to_type_mapping(self) -> dict[str, str]:
        tile_column_name_to_type = {}
        # activate use of raw query filter to retrieve all documents regardless of catalog membership
        with self.feature_service.allow_use_raw_query_filter():
            feature_documents = self.feature_service.list_documents_as_dict_iterator(
                query_filter={}, use_raw_query_filter=True
            )

            async for doc in feature_documents:
                feature_model = ExtendedFeatureModel(**doc)
                try:
                    tile_specs = feature_model.tile_specs
                except BaseException as _:
                    logger.exception(f"Failed to extract tile_specs for {doc}")
                    # For tile columns with no known type, they will be given a FLOAT type below
                    continue
                for tile_spec in tile_specs:
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


class DataWarehouseMigrationServiceV1(DataWarehouseMigrationMixin):
    """
    DataWarehouseMigrationService class

    Responsible for migrating the featurebyte working schema in the data warehouse
    """

    extractor: TileColumnTypeExtractor

    def __init__(
        self,
        persistent: Persistent,
        session_manager_service: SessionManagerService,
        feature_store_service: FeatureStoreService,
        tile_column_type_extractor: TileColumnTypeExtractor,
    ):
        super().__init__(persistent, session_manager_service, feature_store_service)
        self.tile_column_type_extractor = tile_column_type_extractor

    async def _add_tile_value_types_column(self, migration_version: int) -> None:
        """
        Add VALUE_COLUMN_TYPES column in TILE_REGISTRY table

        Parameters
        ----------
        migration_version: int
            Migration version
        """
        collection_names = await self.persistent.list_collection_names()
        if "feature_store" not in collection_names:
            return

        await self.tile_column_type_extractor.setup()

        # migrate all records and audit records
        await self.migrate_all_records(version=migration_version)

    @migrate(version=1, description="Add VALUE_COLUMN_TYPES column in TILE_REGISTRY table")
    async def add_tile_value_types_column(self) -> None:
        """
        Add VALUE_COLUMN_TYPES column in TILE_REGISTRY table
        """
        await self._add_tile_value_types_column(migration_version=6)

    @migrate(
        version=2,
        description="Add VALUE_COLUMN_TYPES column in TILE_REGISTRY table (fix session uses wrong user ID issue)",
    )
    async def add_tile_value_types_column_again(self) -> None:
        """
        Add VALUE_COLUMN_TYPES column in TILE_REGISTRY table (fix version 6 issues, since the migrations are
        expected to be idempotent, running the step twice consecutively should be ok).
        """
        await self._add_tile_value_types_column(migration_version=7)

    async def migrate_record_with_session(
        self, feature_store: FeatureStoreModel, session: BaseSession
    ) -> None:
        _ = feature_store

        df_tile_registry = await session.execute_query("SELECT * FROM TILE_REGISTRY")
        if "VALUE_COLUMN_TYPES" in df_tile_registry:  # type: ignore[operator]
            return

        df_tile_registry["VALUE_COLUMN_TYPES"] = (  # type: ignore[index]
            self.tile_column_type_extractor.get_tile_column_types_from_names(
                df_tile_registry["VALUE_COLUMN_NAMES"]  # type: ignore[index]
            )
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


class DataWarehouseMigrationServiceV3(DataWarehouseMigrationMixin):
    """
    Reset working schema from scratch due to major changes in the warehouse schema in order to:

    1. Add online serving support for complex features involving multiple tile tables
    2. Fix jobs scheduling bug caused by tile_id collision
    """

    def __init__(
        self,
        persistent: Persistent,
        session_manager_service: SessionManagerService,
        feature_store_service: FeatureStoreService,
        working_schema_service: WorkingSchemaService,
    ):
        super().__init__(persistent, session_manager_service, feature_store_service)
        self.working_schema_service = working_schema_service

    @migrate(version=3, description="Reset working schema from scratch")
    async def reset_working_schema(self, query_filter: Optional[QueryFilter] = None) -> None:
        """
        Reset working schema from scratch

        Parameters
        ----------
        query_filter: Optional[QueryFilter]
            Query filter used to filter the documents used for migration. Used only in test when
            intending to migrate a specific document.
        """
        await self.migrate_all_records(version=8, query_filter=query_filter)

    async def migrate_record_with_session(
        self, feature_store: FeatureStoreModel, session: BaseSession
    ) -> None:
        await self.working_schema_service.recreate_working_schema(
            feature_store_id=feature_store.id, session=session
        )
