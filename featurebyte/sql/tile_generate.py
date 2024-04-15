"""
Databricks Tile Generate Job Script
"""

from typing import Optional

import dateutil.parser

from featurebyte.common import date_util
from featurebyte.logging import get_logger
from featurebyte.models.tile import TileType
from featurebyte.service.tile_registry_service import TileRegistryService
from featurebyte.sql.tile_common import TileCommon
from featurebyte.sql.tile_registry import TileRegistry

logger = get_logger(__name__)


class TileGenerate(TileCommon):
    """
    Tile Generate script
    """

    tile_type: TileType
    last_tile_start_str: Optional[str]
    tile_registry_service: TileRegistryService

    async def execute(self) -> None:
        """
        Execute tile generate operation
        """
        # pylint: disable=too-many-statements
        tile_table_exist_flag = await self.table_exists(self.tile_id)

        # pylint: disable=duplicate-code
        await TileRegistry(
            session=self._session,
            sql=self.sql,
            table_name=self.tile_id,
            table_exist=tile_table_exist_flag,
            time_modulo_frequency_second=self.time_modulo_frequency_second,
            blind_spot_second=self.blind_spot_second,
            frequency_minute=self.frequency_minute,
            entity_column_names=self.entity_column_names,
            value_column_names=self.value_column_names,
            value_column_types=self.value_column_types,
            tile_id=self.tile_id,
            aggregation_id=self.aggregation_id,
            feature_store_id=self.feature_store_id,
            tile_registry_service=self.tile_registry_service,
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

        # insert new records and update existing records
        if not tile_table_exist_flag:
            logger.debug(f"creating tile table: {self.tile_id}")
            await self._session.create_table_as(
                table_details=self.tile_id,
                select_expr=tile_sql,
                retry=True,
            )
            logger.debug(f"done creating table: {self.tile_id}")
        else:
            logger.debug("merging into tile table", extra={"tile_id": self.tile_id})
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
            await self._session.retry_sql(sql=merge_sql)

        if self.last_tile_start_str:
            ind_value = date_util.timestamp_utc_to_tile_index(
                dateutil.parser.isoparse(self.last_tile_start_str),
                self.time_modulo_frequency_second,
                self.blind_spot_second,
                self.frequency_minute,
            )

            logger.debug(
                "Using specified last_tile_start_str",
                extra={"last_tile_start_str": self.last_tile_start_str, "ind_value": ind_value},
            )

            await self.tile_registry_service.update_last_run_metadata(
                tile_id=self.tile_id,
                aggregation_id=self.aggregation_id,
                tile_type=self.tile_type,
                tile_index=ind_value,
                tile_end_date=dateutil.parser.isoparse(self.last_tile_start_str),
            )

    def _construct_tile_sql_with_index(self) -> str:
        if self.entity_column_names:
            entity_and_value_column_names_str = (
                f"{self.entity_column_names_str}, {self.value_column_names_str}"
            )
        else:
            entity_and_value_column_names_str = self.value_column_names_str

        tile_sql = f"""
            select
                index,
                {entity_and_value_column_names_str},
                current_timestamp() as created_at
            from ({self.sql})
        """
        return tile_sql
