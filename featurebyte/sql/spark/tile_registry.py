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

        input_value_columns = [value for value in self.value_column_names if value.strip()]
        logger.debug(f"input_value_columns: {input_value_columns}")

        input_value_columns_types = [value for value in self.value_column_types if value.strip()]
        logger.debug(f"input_value_columns_types: {input_value_columns_types}")

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
