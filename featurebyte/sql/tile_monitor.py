"""
Tile Monitor Job
"""
from featurebyte.logging import get_logger
from featurebyte.sql.common import construct_create_table_query, retry_sql
from featurebyte.sql.tile_common import TileCommon
from featurebyte.sql.tile_registry import TileRegistry

logger = get_logger(__name__)


class TileMonitor(TileCommon):
    """
    Tile Monitor script
    """

    monitor_sql: str
    tile_start_date_column: str
    tile_type: str

    async def execute(self) -> None:
        """
        Execute tile monitor operation
        """

        tile_table_exist_flag = await self.table_exists(self.tile_id)
        logger.debug(f"tile_table_exist_flag: {tile_table_exist_flag}")

        if not tile_table_exist_flag:
            logger.info(f"tile table {self.tile_id} does not exist")
        else:
            tile_sql = self.monitor_sql.replace("'", "''")

            await TileRegistry(
                session=self._session,
                sql=tile_sql,
                table_name=self.tile_id,
                table_exist=True,
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

            new_tile_sql = f"""
                select
                    {self.tile_start_date_column},
                    F_TIMESTAMP_TO_INDEX(
                        {self.tile_start_date_column},
                        {self.tile_modulo_frequency_second},
                        {self.blind_spot_second},
                        {self.frequency_minute}
                    ) as INDEX,
                    {self.entity_column_names_str},
                    {self.value_column_names_str}
                from ({self.monitor_sql})
            """

            entity_filter_cols_str = " AND ".join(
                [
                    f"a.{self.quote_column(c)} = b.{self.quote_column(c)}"
                    for c in self.entity_column_names
                ]
            )
            value_select_cols_str = " , ".join(
                [f"b.{c} as OLD_{c}" for c in self.value_column_names]
            )
            value_filter_cols_str = " OR ".join(
                [
                    f"{c} != OLD_{c} or ({c} is not null and OLD_{c} is null)"
                    for c in self.value_column_names
                ]
            )

            compare_sql = f"""
                select * from
                    (select
                        a.*,
                        {value_select_cols_str},
                        cast('{self.tile_type}' as string) as TILE_TYPE,
                        DATEADD(
                            SECOND,
                            ({self.blind_spot_second}+{self.frequency_minute}*60),
                            a.{self.tile_start_date_column}
                        ) as EXPECTED_CREATED_AT,
                        current_timestamp() as CREATED_AT
                    from
                        ({new_tile_sql}) a left outer join {self.tile_id} b
                    on
                        a.INDEX = b.INDEX AND {entity_filter_cols_str})
                where {value_filter_cols_str}
            """

            monitor_table_name = f"{self.tile_id}_MONITOR"
            tile_monitor_exist_flag = await self.table_exists(monitor_table_name)
            logger.debug(f"tile_monitor_exist_flag: {tile_monitor_exist_flag}")

            if not tile_monitor_exist_flag:
                create_sql = construct_create_table_query(
                    monitor_table_name, compare_sql, session=self._session
                )
                await retry_sql(self._session, create_sql)
            else:
                tile_registry_ins = TileRegistry(
                    session=self._session,
                    sql=tile_sql,
                    table_name=monitor_table_name,
                    table_exist=True,
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
                )
                logger.info("Calling tile_registry.execute")
                await tile_registry_ins.execute()
                logger.info("End of calling tile_registry.execute")

                # spark does not support insert with partial columns
                # need to use merge for insertion
                entity_column_names_str_src = " , ".join(
                    [f"b.{c}" for c in self.entity_column_names_str.split(",")]
                )
                old_value_insert_cols_str_target = " , ".join(
                    [f"OLD_{c}" for c in self.value_column_names]
                )
                value_insert_cols_str = " , ".join([f"b.{c}" for c in self.value_column_names])
                old_value_insert_cols_str = " , ".join(
                    [f"b.OLD_{c}" for c in self.value_column_names]
                )

                insert_sql = f"""
                    MERGE into {monitor_table_name} a using ({compare_sql}) b
                        ON a.INDEX = b.INDEX AND a.CREATED_AT = b.CREATED_AT
                    WHEN NOT MATCHED THEN
                        INSERT
                        (
                            {self.tile_start_date_column},
                            INDEX,
                            {self.entity_column_names_str},
                            {self.value_column_names_str},
                            {old_value_insert_cols_str_target},
                            TILE_TYPE,
                            EXPECTED_CREATED_AT,
                            CREATED_AT
                        ) VALUES
                        (
                            b.{self.tile_start_date_column},
                            b.INDEX,
                            {entity_column_names_str_src},
                            {value_insert_cols_str},
                            {old_value_insert_cols_str},
                            b.TILE_TYPE,
                            b.EXPECTED_CREATED_AT,
                            b.CREATED_AT
                        )
                """
                await retry_sql(session=self._session, sql=insert_sql)

            insert_monitor_summary_sql = f"""
                INSERT INTO TILE_MONITOR_SUMMARY(TILE_ID, TILE_START_DATE, TILE_TYPE, CREATED_AT)
                SELECT
                    '{self.tile_id}' as TILE_ID,
                    {self.tile_start_date_column} as TILE_START_DATE,
                    TILE_TYPE,
                    current_timestamp()
                FROM ({compare_sql})
            """
            await retry_sql(self._session, insert_monitor_summary_sql)
