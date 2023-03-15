"""
Tile Registry Job Script for SP_TILE_REGISTRY
"""


from featurebyte.logger import logger
from featurebyte.sql.spark.common import retry_sql
from featurebyte.sql.spark.tile_common import TileCommon


class TileRegistry(TileCommon):
    """
    Tile Registry script corresponding to SP_TILE_REGISTRY stored procedure
    """

    table_name: str
    table_exist: bool

    # pylint: disable=too-many-locals
    async def execute(self) -> None:
        """
        Execute tile registry operation
        """

        input_value_columns = [value for value in self.value_column_names if value.strip()]
        logger.debug(f"input_value_columns: {input_value_columns}")

        input_value_columns_types = [value for value in self.value_column_types if value.strip()]
        logger.debug(f"input_value_columns_types: {input_value_columns_types}")

        registry_df = await self._spark.execute_query(
            f"select count(*) as TILE_COUNT from tile_registry where tile_id = '{self.tile_id}'"
        )

        if registry_df is not None and registry_df["TILE_COUNT"].iloc[0] == 0:
            logger.info(f"No registry record for tile_id {self.tile_id}, creating new record")
            escape_sql = self.sql.replace("'", "''")
            insert_sql = f"""
                insert into tile_registry(
                    TILE_ID,
                    TILE_SQL,
                    ENTITY_COLUMN_NAMES,
                    VALUE_COLUMN_NAMES,
                    VALUE_COLUMN_TYPES,
                    FREQUENCY_MINUTE,
                    TIME_MODULO_FREQUENCY_SECOND,
                    BLIND_SPOT_SECOND,
                    IS_ENABLED,
                    CREATED_AT,
                    LAST_TILE_START_DATE_ONLINE,
                    LAST_TILE_INDEX_ONLINE,
                    LAST_TILE_START_DATE_OFFLINE,
                    LAST_TILE_INDEX_OFFLINE
                )
                VALUES (
                    '{self.tile_id}',
                    '{escape_sql}',
                    '{self.entity_column_names_str}',
                    '{self.value_column_names_str}',
                    '{self.value_column_types_str}',
                    {self.frequency_minute},
                    {self.tile_modulo_frequency_second},
                    {self.blind_spot_second},
                    TRUE,
                    current_timestamp(),
                    null,
                    null,
                    null,
                    null
                )
            """
            await retry_sql(self._spark, insert_sql)

        if self.table_exist:
            cols_df = await self._spark.execute_query(f"SHOW COLUMNS IN {self.table_name}")
            cols = []

            if cols_df is not None:
                for _, row in cols_df.iterrows():
                    cols.append(row["col_name"])

            logger.debug(f"cols: {cols}")

            tile_add_sql = f"ALTER TABLE {self.table_name} ADD COLUMN\n"
            add_statements = []
            for i, input_column in enumerate(input_value_columns):
                if input_column not in cols:
                    element_type = input_value_columns_types[i]
                    add_statements.append(f"{input_column} {element_type}")
                    if "_MONITOR" in self.table_name:
                        add_statements.append(f"OLD_{input_column} {element_type}")

            if add_statements:
                tile_add_sql += ",\n".join(add_statements)
                await self._spark.execute_query(tile_add_sql)
