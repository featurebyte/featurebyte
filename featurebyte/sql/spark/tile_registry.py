"""
Tile Registry Job Script for SP_TILE_REGISTRY
"""


from featurebyte.logger import logger
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

        registry_df = await self._spark.execute_query(
            f"select VALUE_COLUMN_NAMES, VALUE_COLUMN_TYPES from tile_registry where tile_id = '{self.tile_id}'"
        )

        # res = registry_df.select("names", "types").collect()
        # logger.debug(f"res: {res}")

        input_value_columns = [value for value in self.value_column_names if value.strip()]
        logger.debug(f"input_value_columns: {input_value_columns}")

        input_value_columns_types = [value for value in self.value_column_types if value.strip()]
        logger.debug(f"input_value_columns_types: {input_value_columns_types}")

        if registry_df is not None and len(registry_df) > 0:
            value_cols = registry_df["VALUE_COLUMN_NAMES"].iloc[0]
            value_cols_types = registry_df["VALUE_COLUMN_TYPES"].iloc[0]
            logger.debug(f"value_cols: {value_cols}")
            logger.debug(f"value_cols_types: {value_cols_types}")

            exist_columns = [value for value in value_cols.split(",") if value.strip()]
            exist_columns_types = [value for value in value_cols_types.split(",") if value.strip()]

            for i, input_column in enumerate(input_value_columns):
                if input_column not in exist_columns:
                    exist_columns.append(input_column)

                    input_column_type = input_value_columns_types[i]
                    exist_columns_types.append(input_column_type)

            new_value_columns_str = ",".join(exist_columns)
            logger.debug(f"new_value_columns_str: {new_value_columns_str}")
            new_value_columns_types_str = ",".join(exist_columns_types)
            logger.debug(f"new_value_columns_types_str: {new_value_columns_types_str}")

            update_sql = f"""
                            UPDATE TILE_REGISTRY SET
                                VALUE_COLUMN_NAMES = '{new_value_columns_str}',
                                VALUE_COLUMN_TYPES = '{new_value_columns_types_str}'
                            WHERE TILE_ID = '{self.tile_id}'
                         """
            await self._spark.execute_query(update_sql)
        else:
            logger.info("No value columns")
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
            await self._spark.execute_query(insert_sql)

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
