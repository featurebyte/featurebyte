"""
Tile Monitor Job
"""

import os

from sqlglot import expressions

from featurebyte.enum import InternalName
from featurebyte.logging import get_logger
from featurebyte.query_graph.sql.ast.literal import make_literal_value
from featurebyte.query_graph.sql.common import get_qualified_column_identifier, sql_to_string
from featurebyte.service.tile_registry_service import TileRegistryService
from featurebyte.sql.tile_common import TileCommon
from featurebyte.sql.tile_registry import TileRegistry

logger = get_logger(__name__)


class TileMonitor(TileCommon):
    """
    Tile Monitor script
    """

    monitor_sql: str
    tile_type: str
    tile_registry_service: TileRegistryService

    async def execute(self) -> None:
        """
        Execute tile monitor operation
        """
        # Disable tile monitoring for now since it is not yet user facing but the current
        # implementation incurs significant cost when many features are deployed.
        if not int(os.environ.get("FEATUREBYTE_TILE_MONITORING_ENABLED", "0")):
            return

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

            new_tile_sql = f"""
                select
                    F_INDEX_TO_TIMESTAMP(
                        INDEX,
                        {self.time_modulo_frequency_second},
                        {self.blind_spot_second},
                        {self.frequency_minute}
                    ) as {InternalName.TILE_START_DATE},
                    INDEX,
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

            offset_expr = expressions.Add(
                this=expressions.Mul(
                    this=make_literal_value(self.frequency_minute),
                    expression=make_literal_value(60),
                ),
                expression=make_literal_value(self.blind_spot_second),
            )
            expected_created_at_expr = self.adapter.dateadd_microsecond(
                quantity_expr=expressions.Mul(this=offset_expr, expression=make_literal_value(1e6)),
                timestamp_expr=get_qualified_column_identifier(InternalName.TILE_START_DATE, "a"),
            )
            compare_sql = f"""
                select * from
                    (select
                        a.*,
                        {value_select_cols_str},
                        cast('{self.tile_type}' as string) as TILE_TYPE,
                        {sql_to_string(expected_created_at_expr, source_type=self._session.source_type)} as EXPECTED_CREATED_AT,
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
                await self._session.create_table_as(
                    table_details=monitor_table_name,
                    select_expr=compare_sql,
                    retry=True,
                )
            else:
                tile_registry_ins = TileRegistry(
                    session=self._session,
                    sql=tile_sql,
                    table_name=monitor_table_name,
                    table_exist=True,
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
                            {InternalName.TILE_START_DATE},
                            INDEX,
                            {self.entity_column_names_str},
                            {self.value_column_names_str},
                            {old_value_insert_cols_str_target},
                            TILE_TYPE,
                            EXPECTED_CREATED_AT,
                            CREATED_AT
                        ) VALUES
                        (
                            b.{InternalName.TILE_START_DATE},
                            b.INDEX,
                            {entity_column_names_str_src},
                            {value_insert_cols_str},
                            {old_value_insert_cols_str},
                            b.TILE_TYPE,
                            b.EXPECTED_CREATED_AT,
                            b.CREATED_AT
                        )
                """
                await self._session.retry_sql(insert_sql)

            insert_monitor_summary_sql = f"""
                INSERT INTO TILE_MONITOR_SUMMARY(TILE_ID, TILE_START_DATE, TILE_TYPE, CREATED_AT)
                SELECT
                    '{self.tile_id}' as TILE_ID,
                    {InternalName.TILE_START_DATE} as TILE_START_DATE,
                    TILE_TYPE,
                    current_timestamp()
                FROM ({compare_sql})
            """
            await self._session.retry_sql(insert_monitor_summary_sql)
