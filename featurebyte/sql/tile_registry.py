"""
Tile Registry Job Script for SP_TILE_REGISTRY
"""


from featurebyte.logger import logger
from featurebyte.sql.common import retry_sql
from featurebyte.sql.tile_common import TileCommon


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
            f"SELECT COUNT(*) as TILE_COUNT from tile_registry "
            f"WHERE TILE_ID = '{self.tile_id}' "
            f"AND AGGREGATION_ID = '{self.aggregation_id}' "
        )

        if registry_df is not None and registry_df["TILE_COUNT"].iloc[0] == 0:
            logger.info(
                f"No registry record for tile_id {self.tile_id} and aggregation_id {self.aggregation_id}, creating new record"
            )
            escape_sql = self.sql.replace("'", "''")
            insert_sql = f"""
                insert into tile_registry(
                    TILE_ID,
                    AGGREGATION_ID,
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
                    '{self.aggregation_id}',
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
