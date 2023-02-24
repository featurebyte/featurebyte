"""
Databricks Tile Generate Job Script
"""
from typing import Optional

from featurebyte.logger import logger
from featurebyte.sql.spark.common import construct_create_delta_table_query
from featurebyte.sql.spark.tile_common import TileCommon
from featurebyte.sql.spark.tile_registry import TileRegistry


# pylint: disable=too-many-locals
class TileGenerate(TileCommon):
    """
    Tile Generate script corresponding to SP_TILE_GENERATE stored procedure
    """

    tile_start_date_column: str
    tile_type: str
    last_tile_start_str: Optional[str]
    tile_last_start_date_column: Optional[str]

    async def execute(self) -> None:
        """
        Execute tile generate operation
        """

        tile_table_exist_flag = True
        try:
            await self._spark.execute_query(f"select * from {self.tile_id} limit 1")
        except Exception:  # pylint: disable=broad-except
            tile_table_exist_flag = False

        # 2. Update TILE_REGISTRY & Add New Columns TILE Table
        tile_sql = self.sql.replace("'", "''")

        # pylint: disable=duplicate-code
        await TileRegistry(
            spark_session=self._spark,
            sql=tile_sql,
            table_name=self.tile_id,
            table_exist=tile_table_exist_flag,
            tile_start_date_column=self.tile_start_date_column,
            tile_modulo_frequency_second=self.tile_modulo_frequency_second,
            blind_spot_second=self.blind_spot_second,
            frequency_minute=self.frequency_minute,
            entity_column_names=self.entity_column_names,
            value_column_names=self.value_column_names,
            value_column_types=self.value_column_types,
            tile_id=self.tile_id,
            tile_type=self.tile_type,
        ).execute()

        tile_sql = f"""
            select
                F_TIMESTAMP_TO_INDEX({self.tile_start_date_column},
                    {self.tile_modulo_frequency_second},
                    {self.blind_spot_second},
                    {self.frequency_minute}
                ) as index,
                {self.entity_column_names_str},
                {self.value_column_names_str},
                current_timestamp() as created_at
            from ({self.sql})
        """

        entity_insert_cols = []
        entity_filter_cols = []
        for element in self.entity_column_names:
            element = element.strip()
            entity_insert_cols.append(f"b.`{element}`")
            entity_filter_cols.append(f"a.`{element}` <=> b.`{element}`")

        entity_insert_cols_str = ",".join(entity_insert_cols)
        entity_filter_cols_str = " AND ".join(entity_filter_cols)

        value_insert_cols = []
        value_update_cols = []
        for element in self.value_column_names:
            element = element.strip()
            value_insert_cols.append("b." + element)
            value_update_cols.append("a." + element + " = b." + element)

        value_insert_cols_str = ",".join(value_insert_cols)
        value_update_cols_str = ",".join(value_update_cols)

        logger.debug(f"entity_insert_cols_str: {entity_insert_cols_str}")
        logger.debug(f"entity_filter_cols_str: {entity_filter_cols_str}")
        logger.debug(f"value_insert_cols_str: {value_insert_cols_str}")
        logger.debug(f"value_update_cols_str: {value_update_cols_str}")

        # insert new records and update existing records
        if not tile_table_exist_flag:
            logger.info("creating tile table: ", self.tile_id)
            create_sql = construct_create_delta_table_query(self.tile_id, tile_sql)
            await self._spark.execute_query(create_sql)

        else:
            if self.entity_column_names:
                on_condition_str = f"a.INDEX = b.INDEX AND {entity_filter_cols_str}"
                insert_str = f"INDEX, {self.entity_column_names_str}, {self.value_column_names_str}, CREATED_AT"
                values_str = f"b.INDEX, {entity_insert_cols_str}, {value_insert_cols_str}, current_timestamp()"
            else:
                on_condition_str = "a.INDEX = b.INDEX"
                insert_str = f"INDEX, {self.value_column_names_str}, CREATED_AT"
                values_str = f"b.INDEX, {value_insert_cols_str}, current_timestamp()"

            merge_sql = f"""
                merge into {self.tile_id} a using ({tile_sql}) b
                    on {on_condition_str}
                    when matched then
                        update set a.created_at = current_timestamp(), {value_update_cols_str}
                    when not matched then
                        insert ({insert_str})
                            values ({values_str})
            """
            await self._spark.execute_query(merge_sql)

        if self.last_tile_start_str:
            logger.debug("last_tile_start_str: ", self.last_tile_start_str)

            index_df = await self._spark.execute_query(
                f"""
                    select F_TIMESTAMP_TO_INDEX(
                        '{self.last_tile_start_str}',
                        {self.tile_modulo_frequency_second},
                        {self.blind_spot_second},
                        {self.frequency_minute}
                    ) as value
                """
            )

            if index_df is None or len(index_df) == 0:
                return

            ind_value = index_df["value"].iloc[0]

            update_tile_last_ind_sql = f"""
                UPDATE TILE_REGISTRY
                    SET
                        LAST_TILE_INDEX_{self.tile_type} = {ind_value},
                        {self.tile_last_start_date_column}_{self.tile_type} = '{self.last_tile_start_str}'
                WHERE TILE_ID = '{self.tile_id}'
            """
            await self._spark.execute_query(update_tile_last_ind_sql)
