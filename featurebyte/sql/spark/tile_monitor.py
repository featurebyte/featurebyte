"""
Tile Monitor Job for SP_TILE_MONITOR
"""
from featurebyte.logger import logger
from featurebyte.sql.spark.common import retry_sql
from featurebyte.sql.spark.tile_common import TileCommon
from featurebyte.sql.spark.tile_registry import TileRegistry


class TileMonitor(TileCommon):
    """
    Tile Monitor script corresponding to SP_TILE_MONITOR stored procedure
    """

    monitor_sql: str
    tile_start_date_column: str
    tile_type: str

    async def execute(self) -> None:
        """
        Execute tile monitor operation
        """

        tile_table_exist_flag = True
        try:
            await self._spark.execute_query(f"select * from {self.tile_id} limit 1")
        except Exception:  # pylint: disable=broad-except
            tile_table_exist_flag = False

        logger.debug(f"tile_table_exist_flag: {tile_table_exist_flag}")

        if not tile_table_exist_flag:
            logger.info(f"tile table {self.tile_id} does not exist")
        else:

            tile_sql = self.monitor_sql.replace("'", "''")

            await TileRegistry(
                spark_session=self._spark,
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
                [f"a.`{c}` = b.`{c}`" for c in self.entity_column_names]
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
            tile_monitor_exist_flag = True
            try:
                await self._spark.execute_query(f"select * from {monitor_table_name} limit 1")
            except Exception:  # pylint: disable=broad-except
                tile_monitor_exist_flag = False
            logger.debug(f"tile_monitor_exist_flag: {tile_monitor_exist_flag}")

            if not tile_monitor_exist_flag:
                await self._spark.execute_query(
                    f"create table {monitor_table_name} using delta as {compare_sql}"
                )
            else:
                tile_registry_ins = TileRegistry(
                    spark_session=self._spark,
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
                )
                logger.info("\n\nCalling tile_registry.execute\n")
                await tile_registry_ins.execute()
                logger.info("\nEnd of calling tile_registry.execute\n\n")

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
                await retry_sql(self._spark, insert_sql)

            insert_monitor_summary_sql = f"""
                INSERT INTO TILE_MONITOR_SUMMARY(TILE_ID, TILE_START_DATE, TILE_TYPE, CREATED_AT)
                SELECT
                    '{self.tile_id}' as TILE_ID,
                    {self.tile_start_date_column} as TILE_START_DATE,
                    TILE_TYPE,
                    current_timestamp()
                FROM ({compare_sql})
            """
            await retry_sql(self._spark, insert_monitor_summary_sql)
