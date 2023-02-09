"""
Tile Monitor Job for SP_TILE_MONITOR
"""
from tile_common import TileCommon
from tile_registry import TileRegistry


class TileMonitor(TileCommon):
    monitor_sql: str
    tile_start_date_column: str
    tile_type: str

    def execute(self) -> None:

        tile_table_exist = self._spark.catalog.tableExists(self.tile_id)
        print("tile_table_exist: ", tile_table_exist)

        if not tile_table_exist:
            print(f"tile table {self.tile_id} does not exist")
        else:
            df = self._spark.sql(
                f"select value_column_names from tile_registry where tile_id = '{self.tile_id}'"
            )
            existing_value_columns = df.collect()[0].value_column_names

            tile_sql = self.monitor_sql.replace("'", "''")

            tile_registry_ins = TileRegistry(
                spark_session=self._spark,
                sql=tile_sql,
                table_name=self.tile_id,
                table_exist="Y",
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
            print("\n\nCalling tile_registry.execute\n")
            tile_registry_ins.execute()
            print("\nEnd of calling tile_registry.execute\n\n")

            new_tile_sql = f"""
                select
                    {self.tile_start_date_column},
                    F_TIMESTAMP_TO_INDEX({self.tile_start_date_column}, {self.tile_modulo_frequency_second}, {self.blind_spot_second}, {self.frequency_minute}) as INDEX,
                    {self.entity_column_names},
                    {existing_value_columns}
                from ({self.monitor_sql})
            """
            print("new_tile_sql: ", new_tile_sql)

            entity_filter_cols_str = " AND ".join(
                [f"a.{c} = b.{c}" for c in self.entity_column_names.split(",")]
            )
            value_select_cols_str = " , ".join(
                [f"b.{c} as old_{c}" for c in existing_value_columns.split(",")]
            )
            value_insert_cols_str = " , ".join(
                [f"old_{c}" for c in existing_value_columns.split(",")]
            )
            value_filter_cols_str = " OR ".join(
                [
                    f"{c} != old_{c} or ({c} is not null and old_{c} is null)"
                    for c in existing_value_columns.split(",")
                ]
            )

            compare_sql = f"""
                select * from
                    (select
                        a.*,
                        {value_select_cols_str},
                        cast('{self.tile_type}' as string) as TILE_TYPE,
                        DATEADD(SECOND, ({self.blind_spot_second}+{self.frequency_minute}*60), a.{self.tile_start_date_column}) as EXPECTED_CREATED_AT,
                        current_timestamp() as CREATED_AT
                    from
                        ({new_tile_sql}) a left outer join {self.tile_id} b
                    on
                        a.INDEX = b.INDEX AND {entity_filter_cols_str})
                where {value_filter_cols_str}
            """
            print("compare_sql: ", compare_sql)

            monitor_table_name = f"{self.tile_id}_MONITOR"

            tile_monitor_exist = self._spark.catalog.tableExists(monitor_table_name)
            print("tile_monitor_exist: ", tile_monitor_exist)

            if not tile_monitor_exist:
                self._spark.sql(f"create table {monitor_table_name} as {compare_sql}")
            else:
                tile_registry_ins = TileRegistry(
                    spark_session=self._spark,
                    sql=tile_sql,
                    table_name=monitor_table_name,
                    table_exist="Y",
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
                print("\n\nCalling tile_registry.execute\n")
                tile_registry_ins.execute()
                print("\nEnd of calling tile_registry.execute\n\n")

                insert_sql = f"""
                    insert into {monitor_table_name}
                        ({self.tile_start_date_column}, INDEX, {self.entity_column_names}, {existing_value_columns}, {value_insert_cols_str}, TILE_TYPE, EXPECTED_CREATED_AT, CREATED_AT)
                        {compare_sql}
                """
                print(insert_sql)
                self._spark.sql(insert_sql)

                insert_monitor_summary_sql = f"""
                    INSERT INTO TILE_MONITOR_SUMMARY(TILE_ID, TILE_START_DATE, TILE_TYPE, CREATED_AT)
                    SELECT
                        '{self.tile_id}' as TILE_ID,
                        {self.tile_start_date_column} as TILE_START_DATE,
                        TILE_TYPE,
                        current_timestamp()
                    FROM ({compare_sql})
                """
                print(insert_monitor_summary_sql)
                self._spark.sql(insert_monitor_summary_sql)
