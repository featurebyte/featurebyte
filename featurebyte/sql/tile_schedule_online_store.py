"""
Tile Generate online store Job Script
"""
from typing import Any

from datetime import datetime

from featurebyte.logging import get_logger
from featurebyte.session.base import BaseSession
from featurebyte.sql.base import BaselSqlModel
from featurebyte.sql.common import construct_create_table_query, retry_sql

logger = get_logger(__name__)


class TileScheduleOnlineStore(BaselSqlModel):
    """
    Tile Schedule Online Store script
    """

    aggregation_id: str
    job_schedule_ts_str: str

    def __init__(self, session: BaseSession, **kwargs: Any):
        """
        Initialize Tile Schedule Online Store Instance

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
        Execute tile schedule online store operation
        """
        # pylint: disable=too-many-locals,too-many-statements
        select_sql = f"""
            SELECT
              RESULT_ID,
              SQL_QUERY,
              ONLINE_STORE_TABLE_NAME,
              ENTITY_COLUMN_NAMES,
              RESULT_TYPE
            FROM ONLINE_STORE_MAPPING
            WHERE
              AGGREGATION_ID ILIKE '{self.aggregation_id}' AND IS_DELETED = FALSE
        """
        online_store_df = await retry_sql(self._session, select_sql)
        if online_store_df is None or len(online_store_df) == 0:
            return

        for _, row in online_store_df.iterrows():
            f_name = row["RESULT_ID"]
            f_sql = row["SQL_QUERY"]
            fs_table = row["ONLINE_STORE_TABLE_NAME"]
            f_entity_columns = row["ENTITY_COLUMN_NAMES"]
            f_value_type = row["RESULT_TYPE"]
            f_sql = f_sql.replace(
                "__FB_POINT_IN_TIME_SQL_PLACEHOLDER", "'" + self.job_schedule_ts_str + "'"
            )

            logger.debug(f"{f_name}, {fs_table}, {f_entity_columns}, {f_value_type}")

            entity_columns = (
                [col.replace('"', "") for col in f_entity_columns.split(",")]
                if f_entity_columns
                else []
            )

            # check if feature store table exists
            fs_table_exist_flag = await self.table_exists(fs_table)
            logger.debug(f"fs_table_exist_flag: {fs_table_exist_flag}")

            entities_fname_str = ", ".join(
                [self.quote_column(col) for col in entity_columns + [f_name]]
            )

            current_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            if not fs_table_exist_flag:
                # feature store table does not exist, create table with the input feature sql
                create_sql = construct_create_table_query(
                    fs_table, f"select {entities_fname_str} from ({f_sql})", session=self._session
                )
                await retry_sql(self._session, create_sql)

                await retry_sql(
                    self._session,
                    f"ALTER TABLE {fs_table} ADD COLUMN UPDATED_AT_{f_name} TIMESTAMP",
                )

                await retry_sql(
                    session=self._session,
                    sql=f"UPDATE {fs_table} SET UPDATED_AT_{f_name} = to_timestamp('{current_ts}')",
                )
            else:
                # feature store table already exists, insert records with the input feature sql
                entity_insert_cols = []
                entity_filter_cols = []
                for element in entity_columns:
                    quote_element = self.quote_column(element)
                    entity_insert_cols.append(f"b.{quote_element}")
                    entity_filter_cols.append(f"a.{quote_element} = b.{quote_element}")

                entity_insert_cols_str = ", ".join(entity_insert_cols)
                entity_filter_cols_str = " AND ".join(entity_filter_cols)

                # check whether feature value column exists, if not add the new column
                cols = await self.get_table_columns(fs_table)
                col_exists = f_name.upper() in cols

                quote_f_name = self.quote_column(f_name)

                if not col_exists:
                    await retry_sql(
                        self._session,
                        f"ALTER TABLE {fs_table} ADD COLUMN {quote_f_name} {f_value_type}",
                    )
                    await retry_sql(
                        self._session,
                        f"ALTER TABLE {fs_table} ADD COLUMN UPDATED_AT_{f_name} TIMESTAMP",
                    )
                    logger.debug(f"done adding column ({f_name}) to table {fs_table}")

                # update or insert feature values for entities that are in entity universe
                if entity_columns:
                    on_condition_str = entity_filter_cols_str
                    values_args = f"{entity_insert_cols_str}, b.{quote_f_name}"
                else:
                    on_condition_str = "true"
                    values_args = f"b.{quote_f_name}"

                # update or insert feature values for entities that are in entity universe
                merge_sql = f"""
                     merge into {fs_table} a using ({f_sql}) b
                         on {on_condition_str}
                         when matched then
                             update set a.{quote_f_name} = b.{quote_f_name}, a.UPDATED_AT_{f_name} = to_timestamp('{current_ts}')
                         when not matched then
                             insert ({entities_fname_str}, UPDATED_AT_{f_name})
                                 values ({values_args}, to_timestamp('{current_ts}'))
                 """

                await retry_sql(session=self._session, sql=merge_sql)

                # remove feature values for entities that are not in entity universe
                remove_values_sql = f"""UPDATE {fs_table} SET {quote_f_name} = NULL WHERE UPDATED_AT_{f_name} < to_timestamp('{current_ts}')"""
                await retry_sql(session=self._session, sql=remove_values_sql)
