"""
Databricks Tile Generate Job Script
"""
from typing import Optional

import dateutil.parser

from featurebyte.common import date_util
from featurebyte.logging import get_logger
from featurebyte.sql.common import construct_create_table_query, retry_sql
from featurebyte.sql.tile_common import TileCommon
from featurebyte.sql.tile_registry import TileRegistry

logger = get_logger(__name__)


class TileGenerate(TileCommon):
    """
    Tile Generate script
    """

    tile_start_date_column: str
    tile_type: str
    last_tile_start_str: Optional[str]
    tile_last_start_date_column: Optional[str]

    async def execute(self) -> None:
        """
        Execute tile generate operation
        """
        # pylint: disable=too-many-statements
        tile_table_exist_flag = await self.table_exists(self.tile_id)
        logger.debug(f"tile_table_exist_flag: {tile_table_exist_flag}")

        # 2. Update TILE_REGISTRY & Add New Columns TILE Table
        tile_sql = self.sql.replace("'", "''")

        # pylint: disable=duplicate-code
        await TileRegistry(
            session=self._session,
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
            aggregation_id=self.aggregation_id,
        ).execute()

        tile_sql = self._construct_tile_sql_with_index()

        entity_insert_cols = []
        entity_filter_cols = []
        for element in self.entity_column_names:
            quote_element = self.quote_column(element.strip())
            entity_insert_cols.append(f"b.{quote_element}")
            entity_filter_cols.append(
                self.quote_column_null_aware_equal(f"a.{quote_element}", f"b.{quote_element}")
            )

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
            logger.debug(f"creating tile table: {self.tile_id}")
            create_sql = construct_create_table_query(self.tile_id, tile_sql, session=self._session)
            logger.debug(f"create_sql: {create_sql}")
            await retry_sql(self._session, create_sql)
            logger.debug(f"done creating table: {self.tile_id}")
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
            await retry_sql(session=self._session, sql=merge_sql)

        if self.last_tile_start_str:
            logger.debug(f"last_tile_start_str: {self.last_tile_start_str}")

            ind_value = date_util.timestamp_utc_to_tile_index(
                dateutil.parser.isoparse(self.last_tile_start_str),
                self.tile_modulo_frequency_second,
                self.blind_spot_second,
                self.frequency_minute,
            )

            logger.debug(f"ind_value: {ind_value}")

            update_tile_last_ind_sql = f"""
                UPDATE TILE_REGISTRY
                    SET
                        LAST_TILE_INDEX_{self.tile_type} = {ind_value},
                        {self.tile_last_start_date_column}_{self.tile_type} = to_timestamp('{self.last_tile_start_str}')
                WHERE TILE_ID = '{self.tile_id}'
                AND AGGREGATION_ID = '{self.aggregation_id}'
            """
            await retry_sql(self._session, update_tile_last_ind_sql)

    def _construct_tile_sql_with_index(self) -> str:
        if self.entity_column_names:
            entity_and_value_column_names_str = (
                f"{self.entity_column_names_str}, {self.value_column_names_str}"
            )
        else:
            entity_and_value_column_names_str = self.value_column_names_str

        tile_sql = f"""
            select
                F_TIMESTAMP_TO_INDEX({self.tile_start_date_column},
                    {self.tile_modulo_frequency_second},
                    {self.blind_spot_second},
                    {self.frequency_minute}
                ) as index,
                {entity_and_value_column_names_str},
                current_timestamp() as created_at
            from ({self.sql})
        """
        return tile_sql
