"""
Databricks Tile Generate Job Script
"""
from typing import Optional

from featurebyte.logger import logger
from featurebyte.sql.spark.tile_common import TileCommon
from featurebyte.sql.spark.tile_registry import TileRegistry


class TileGenerate(TileCommon):
    tile_start_date_column: str
    tile_type: str
    last_tile_start_str: Optional[str]
    tile_last_start_date_column: Optional[str]

    def execute(self) -> None:

        tile_table_exist = self._spark.catalog.tableExists(self.tile_id)
        tile_table_exist_flag = "Y" if tile_table_exist else "N"

        # 2. Update TILE_REGISTRY & Add New Columns TILE Table

        tile_sql = self.sql.replace("'", "''")

        tile_registry_ins = TileRegistry(
            spark_session=self._spark,
            sql=tile_sql,
            table_name=self.tile_id,
            table_exist=tile_table_exist_flag,
            featurebyte_database=self.featurebyte_database,
            tile_start_date_column=self.tile_start_date_column,
            tile_modulo_frequency_second=self.tile_modulo_frequency_second,
            blind_spot_second=self.blind_spot_second,
            frequency_minute=self.frequency_minute,
            entity_column_names=self.entity_column_names,
            value_column_names=self.value_column_names,
            value_column_types=self.value_column_types,
            tile_id=self.tile_id,
            tile_type=self.tile_type,
        )
        logger.info("\n\nCalling tile_registry.execute\n")
        tile_registry_ins.execute()
        logger.info("\nEnd of calling tile_registry.execute\n\n")

        tile_sql = f"""
            select
                F_TIMESTAMP_TO_INDEX({self.tile_start_date_column},
                    {self.tile_modulo_frequency_second},
                    {self.blind_spot_second},
                    {self.frequency_minute}
                ) as index,
                {self.entity_column_names}, {self.value_column_names},
                current_timestamp() as created_at
            from ({self.sql})
        """
        logger.debug("tile_sql:", tile_sql)

        entity_insert_cols = []
        entity_filter_cols = []
        for element in self.entity_column_names.split(","):
            element = element.strip()
            entity_insert_cols.append("b." + element)
            entity_filter_cols.append("a." + element + " = b." + element)

        entity_insert_cols_str = ",".join(entity_insert_cols)
        entity_filter_cols_str = " AND ".join(entity_filter_cols)

        value_insert_cols = []
        value_update_cols = []
        for element in self.value_column_names.split(","):
            element = element.strip()
            value_insert_cols.append("b." + element)
            value_update_cols.append("a." + element + " = b." + element)

        value_insert_cols_str = ",".join(value_insert_cols)
        value_update_cols_str = ",".join(value_update_cols)

        logger.debug("entity_insert_cols_str: ", entity_insert_cols_str)
        logger.debug("entity_filter_cols_str: ", entity_filter_cols_str)
        logger.debug("value_insert_cols_str: ", value_insert_cols_str)
        logger.debug("value_update_cols_str: ", value_update_cols_str)

        # insert new records and update existing records
        if not tile_table_exist:
            logger.info("creating tile table: ", self.tile_id)
            self._spark.sql(f"create table {self.tile_id} using delta as {tile_sql}")

        else:
            if self.entity_column_names:
                on_condition_str = f"a.INDEX = b.INDEX AND {entity_filter_cols_str}"
                insert_str = (
                    f"INDEX, {self.entity_column_names}, {self.value_column_names}, CREATED_AT"
                )
                values_str = f"b.INDEX, {entity_insert_cols_str}, {value_insert_cols_str}, current_timestamp()"
            else:
                on_condition_str = "a.INDEX = b.INDEX"
                insert_str = f"INDEX, {self.value_column_names}, CREATED_AT"
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
            logger.debug("merging data: ", merge_sql)
            self._spark.sql(merge_sql)

        if self.last_tile_start_str:
            logger.debug("last_tile_start_str: ", self.last_tile_start_str)

            df = self._spark.sql(
                f"select F_TIMESTAMP_TO_INDEX('{self.last_tile_start_str}', {self.tile_modulo_frequency_second}, {self.blind_spot_second}, {self.frequency_minute}) as value"
            )
            ind_value = df.select("value").collect()[0].value

            update_tile_last_ind_sql = f"""
                UPDATE TILE_REGISTRY SET LAST_TILE_INDEX_{self.tile_type} = {ind_value}, {self.tile_last_start_date_column}_{self.tile_type} = '{self.last_tile_start_str}'
                WHERE TILE_ID = '{self.tile_id}'
            """
            logger.debug(update_tile_last_ind_sql)
            self._spark.sql(update_tile_last_ind_sql)
