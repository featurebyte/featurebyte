"""
Tile Registry Job Script
"""

from featurebyte.logging import get_logger
from featurebyte.models.tile_registry import TileModel
from featurebyte.service.tile_registry_service import TileRegistryService
from featurebyte.sql.tile_common import TileCommon

logger = get_logger(__name__)

LIST_TABLE_SCHEMA_TIMEOUT_SECONDS = 10 * 60


class TileRegistry(TileCommon):
    """
    Tile Registry script
    """

    table_name: str
    table_exist: bool
    tile_registry_service: TileRegistryService

    async def execute(self) -> None:
        """
        Execute tile registry operation
        """

        input_value_columns = [value for value in self.value_column_names if value.strip()]

        input_value_columns_types = [value for value in self.value_column_types if value.strip()]

        tile_model = await self.tile_registry_service.get_tile_model(
            self.tile_id, self.aggregation_id
        )

        if tile_model is None:
            logger.info(
                f"No registry record for tile_id {self.tile_id} and aggregation_id {self.aggregation_id}, creating new record"
            )
            tile_model = TileModel(
                feature_store_id=self.feature_store_id,
                tile_id=self.tile_id,
                aggregation_id=self.aggregation_id,
                tile_sql=self.sql,
                entity_column_names=self.entity_column_names,
                value_column_names=self.value_column_names,
                value_column_types=self.value_column_types,
                frequency_minute=self.frequency_minute,
                time_modulo_frequency_second=self.time_modulo_frequency_second,
                blind_spot_second=self.blind_spot_second,
            )
            await self.tile_registry_service.create_document(tile_model)

        if self.table_exist:
            cols = [
                c.upper()
                for c in (
                    await self._session.list_table_schema(
                        self.table_name,
                        self._session.database_name,
                        self._session.schema_name,
                        timeout=LIST_TABLE_SCHEMA_TIMEOUT_SECONDS,
                    )
                ).keys()
            ]
            tile_add_sql = f"ALTER TABLE {self.table_name} ADD COLUMN\n"
            add_statements = []
            for i, input_column in enumerate(input_value_columns):
                if input_column.upper() not in cols:
                    element_type = input_value_columns_types[i]
                    add_statements.append(f"{input_column} {element_type}")
                    if "_MONITOR" in self.table_name:
                        add_statements.append(f"OLD_{input_column} {element_type}")

            if add_statements:
                tile_add_sql += ",\n".join(add_statements)
                logger.debug(f"tile_add_sql: {tile_add_sql}")
                await self._session.retry_sql(tile_add_sql)
