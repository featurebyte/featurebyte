"""
Tile Registry Job Script
"""

from sqlglot import expressions

from featurebyte.exception import DocumentConflictError
from featurebyte.logging import get_logger
from featurebyte.models.tile_registry import TileModel
from featurebyte.query_graph.sql.common import quoted_identifier
from featurebyte.service.tile_registry_service import TileRegistryService
from featurebyte.session.base import LONG_RUNNING_EXECUTE_QUERY_TIMEOUT_SECONDS
from featurebyte.sql.tile_common import TileCommon

logger = get_logger(__name__)


class TileRegistry(TileCommon):
    """
    Tile Registry script
    """

    computed_tiles_table_name: str
    tile_registry_service: TileRegistryService

    async def execute(self) -> None:
        """
        Execute tile registry operation
        """
        input_value_columns = [value for value in self.value_column_names if value.strip()]

        input_value_columns_types = [value for value in self.value_column_types if value.strip()]

        tile_model = await self.tile_registry_service.get_tile_model(
            self.tile_id, self.aggregation_id
        )

        if tile_model is None:
            logger.info(
                f"No registry record for tile_id {self.tile_id} and aggregation_id {self.aggregation_id}, creating new record"
            )
            tile_model = TileModel(
                feature_store_id=self.feature_store_id,
                tile_id=self.tile_id,
                aggregation_id=self.aggregation_id,
                tile_sql=self.sql,
                entity_column_names=self.entity_column_names,
                value_column_names=self.value_column_names,
                value_column_types=self.value_column_types,
                frequency_minute=self.frequency_minute,
                time_modulo_frequency_second=self.time_modulo_frequency_second,
                blind_spot_second=self.blind_spot_second,
            )
            try:
                await self.tile_registry_service.create_document(tile_model)
            except DocumentConflictError:
                # Can occur on concurrent tile tasks creating the same tile table
                pass

        table_exist = await self.table_exists(self.tile_id)
        if not table_exist:
            column_parts = ["index"]
            column_parts.extend([self.quote_column(col) for col in self.entity_column_names])
            column_parts.append("created_at")
            await self._session.create_table_as(
                table_details=self.tile_id,
                select_expr=expressions.select(
                    "index",
                    *[quoted_identifier(col) for col in self.entity_column_names],
                    "created_at",
                )
                .from_(self.computed_tiles_table_name)
                .limit(0),
                exists=True,
            )
            table_exist = True

        if table_exist:
            cols = [
                c.upper()
                for c in (
                    await self._session.list_table_schema(
                        self.tile_id,
                        self._session.database_name,
                        self._session.schema_name,
                        timeout=LONG_RUNNING_EXECUTE_QUERY_TIMEOUT_SECONDS,
                    )
                ).keys()
            ]

            table_expr = expressions.Table(this=expressions.Identifier(this=self.tile_id))
            add_statements = []
            for i, input_column in enumerate(input_value_columns):
                if input_column.upper() not in cols:
                    element_type = input_value_columns_types[i]
                    column_def = expressions.ColumnDef(
                        this=expressions.Identifier(this=input_column),
                        kind=element_type,
                    )
                    add_statements.append(
                        self.adapter.alter_table_add_columns(table_expr, [column_def])
                    )
                    if "_MONITOR" in self.tile_id:
                        add_statements.append(f"OLD_{input_column} {element_type}")
                        column_def = expressions.ColumnDef(
                            this=expressions.Identifier(this=f"OLD_{input_column}"),
                            kind=element_type,
                        )
                        add_statements.append(
                            self.adapter.alter_table_add_columns(table_expr, [column_def])
                        )

            for query in add_statements:
                try:
                    await self._session.execute_query_long_running(query)
                except self._session.no_schema_error:
                    # Can occur on concurrent tile tasks creating the same tile table
                    pass
