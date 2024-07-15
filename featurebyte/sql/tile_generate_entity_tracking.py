"""
Tile Generate entity tracking Job script
"""

from typing import Any, List

from featurebyte.enum import InternalName
from featurebyte.logging import get_logger
from featurebyte.session.base import BaseSession
from featurebyte.sql.base import BaseSqlModel

logger = get_logger(__name__)


class TileGenerateEntityTracking(BaseSqlModel):
    """
    Tile Generate entity tracking script
    """

    entity_column_names: List[str]
    entity_tracker_table_name: str
    entity_table: str

    def __init__(self, session: BaseSession, **kwargs: Any):
        """
        Initialize Tile Generate Entity Tracking

        Parameters
        ----------
        session: BaseSession
            input SparkSession
        kwargs: Any
            constructor arguments
        """
        super().__init__(session=session, **kwargs)

    async def execute(self) -> None:
        """
        Execute tile generate entity tracking operation
        """

        tracking_table_name = self.entity_tracker_table_name
        tracking_table_exist_flag = await self.table_exists(tracking_table_name)
        logger.debug(f"tracking_table_exist_flag: {tracking_table_exist_flag}")

        entity_insert_cols = []
        entity_filter_cols = []
        escaped_entity_column_names = []
        for element in self.entity_column_names:
            quote_element = self.quote_column(element.strip())
            entity_insert_cols.append(f"b.{quote_element}")
            escaped_entity_column_names.append(f"{quote_element}")
            entity_filter_cols.append(
                self.quote_column_null_aware_equal(f"a.{quote_element}", f"b.{quote_element}")
            )

        escaped_entity_column_names_str = ",".join(escaped_entity_column_names)

        # create table or insert new records or update existing records
        tile_last_start_date_column = InternalName.TILE_LAST_START_DATE
        if not tracking_table_exist_flag:
            cols = ", ".join(
                [
                    self.quote_column(col)
                    for col in self.entity_column_names + [tile_last_start_date_column]
                ]
            )
            entity_table = f"select {cols} from ({self.entity_table})"
            await self._session.create_table_as(
                table_details=tracking_table_name,
                select_expr=entity_table,
                retry=True,
            )
        else:
            if self.entity_column_names:
                entity_insert_cols_str = ", ".join(entity_insert_cols)
                entity_filter_cols_str = " AND ".join(entity_filter_cols)
                merge_sql = f"""
                    merge into {tracking_table_name} a using ({self.entity_table}) b
                        on {entity_filter_cols_str}
                        when matched then
                            update set a.{tile_last_start_date_column} = b.{tile_last_start_date_column}
                        when not matched then
                            insert ({escaped_entity_column_names_str}, {tile_last_start_date_column})
                                values ({entity_insert_cols_str}, b.{tile_last_start_date_column})
                """
            else:
                merge_sql = f"""
                    merge into {tracking_table_name} a using ({self.entity_table}) b
                        on true
                        when matched then
                            update set a.{tile_last_start_date_column} = b.{tile_last_start_date_column}
                        when not matched then
                            insert ({tile_last_start_date_column})
                                values (b.{tile_last_start_date_column})
                """
            await self._session.retry_sql(merge_sql)
