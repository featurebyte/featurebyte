"""
FeatureMaterializeService class
"""
from __future__ import annotations

from typing import AsyncIterator, List, Optional, cast

from contextlib import asynccontextmanager
from dataclasses import dataclass

from bson import ObjectId
from sqlglot import expressions

from featurebyte.models.feature_list import FeatureCluster
from featurebyte.models.offline_store_feature_table import OfflineStoreFeatureTableModel
from featurebyte.query_graph.node import Node
from featurebyte.query_graph.node.generic import AliasNode, ProjectNode
from featurebyte.query_graph.node.schema import TableDetails
from featurebyte.query_graph.sql.adapter import get_sql_adapter
from featurebyte.query_graph.sql.ast.literal import make_literal_value
from featurebyte.query_graph.sql.common import (
    get_qualified_column_identifier,
    quoted_identifier,
    sql_to_string,
)
from featurebyte.query_graph.sql.online_serving import (
    TemporaryBatchRequestTable,
    get_online_features,
)
from featurebyte.service.entity import EntityService
from featurebyte.service.feature import FeatureService
from featurebyte.service.online_store_compute_query_service import OnlineStoreComputeQueryService
from featurebyte.service.online_store_table_version import OnlineStoreTableVersionService
from featurebyte.session.base import BaseSession

FEATURE_TIMESTAMP_COLUMN = "__feature_timestamp"


@dataclass
class MaterializedFeatures:
    """
    Information about materialised features ready to be published
    """

    materialized_table_name: str
    column_names: List[str]
    data_types: List[str]
    serving_names: List[str]

    @property
    def serving_names_and_column_names(self) -> List[str]:
        """
        Returns serving names and column names in a combined list

        Returns
        -------
        List[str]
        """
        return self.serving_names + self.column_names


class FeatureMaterializeService:
    """
    FeatureMaterializeService is responsible for materialising a set of currently online enabled
    features so that they can be published to an external feature store. These features are
    materialised into a new table in the data warehouse.
    """

    def __init__(
        self,
        feature_service: FeatureService,
        online_store_compute_query_service: OnlineStoreComputeQueryService,
        entity_service: EntityService,
        online_store_table_version_service: OnlineStoreTableVersionService,
    ):
        self.feature_service = feature_service
        self.online_store_compute_query_service = online_store_compute_query_service
        self.entity_service = entity_service
        self.online_store_table_version_service = online_store_table_version_service

    @asynccontextmanager
    async def materialize_features(
        self,
        session: BaseSession,
        feature_table_model: OfflineStoreFeatureTableModel,
        selected_columns: Optional[List[str]] = None,
    ) -> AsyncIterator[MaterializedFeatures]:
        """
        Materialise features for the provided offline store feature table.

        Parameters
        ----------
        session: BaseSession
            Session object
        feature_table_model: OfflineStoreFeatureTableModel
            OfflineStoreFeatureTableModel object
        selected_columns: Optional[List[str]]
            Selected columns to materialize

        Yields
        ------
        MaterializedFeatures
            Metadata of the materialized features
        """
        # Create temporary batch request table with the universe of entities
        unique_id = ObjectId()
        batch_request_table = TemporaryBatchRequestTable(
            table_details=TableDetails(
                database_name=session.database_name,
                schema_name=session.schema_name,
                table_name=f"TEMP_REQUEST_TABLE_{unique_id}".upper(),
            ),
            column_names=feature_table_model.serving_names,
        )
        adapter = get_sql_adapter(session.source_type)
        create_batch_request_table_query = sql_to_string(
            adapter.create_table_as(
                table_details=batch_request_table.table_details,
                select_expr=feature_table_model.entity_universe.get_entity_universe_expr(),
            ),
            source_type=session.source_type,
        )
        await session.execute_query_long_running(create_batch_request_table_query)

        # Materialize features
        output_table_details = TableDetails(
            database_name=session.database_name,
            schema_name=session.schema_name,
            table_name=f"TEMP_FEATURE_TABLE_{unique_id}".upper(),
        )
        try:
            if selected_columns is None:
                nodes = feature_table_model.feature_cluster.nodes
            else:
                nodes = self._get_nodes_for_selected_columns(
                    feature_table_model.feature_cluster, selected_columns
                )
            await get_online_features(
                session=session,
                graph=feature_table_model.feature_cluster.graph,
                nodes=nodes,
                request_data=batch_request_table,
                source_type=session.source_type,
                online_store_table_version_service=self.online_store_table_version_service,
                output_table_details=output_table_details,
            )

            column_names = []
            column_dtypes = []
            for column_name, data_type in zip(
                feature_table_model.output_column_names, feature_table_model.output_dtypes
            ):
                if selected_columns is None or column_name in selected_columns:
                    column_names.append(column_name)
                    column_dtypes.append(data_type)

            yield MaterializedFeatures(
                materialized_table_name=output_table_details.table_name,
                column_names=column_names,
                data_types=[adapter.get_physical_type_from_dtype(dtype) for dtype in column_dtypes],
                serving_names=feature_table_model.serving_names,
            )

        finally:
            # Delete temporary batch request table and materialized feature table
            for table_details in [batch_request_table.table_details, output_table_details]:
                await session.drop_table(
                    table_name=table_details.table_name,
                    schema_name=table_details.schema_name,  # type: ignore
                    database_name=table_details.database_name,  # type: ignore
                    if_exists=True,
                )

    async def scheduled_materialize_features(
        self,
        session: BaseSession,
        feature_table_model: OfflineStoreFeatureTableModel,
    ) -> None:
        """
        Materialize features for the provided offline store feature table. This method is expected
        to be called by a scheduler on regular interval, and the feature table is assumed to have
        the correct schema already.

        Parameters
        ----------
        session: BaseSession
            Session object
        feature_table_model: OfflineStoreFeatureTableModel
            OfflineStoreFeatureTableModel object
        """
        async with self.materialize_features(session, feature_table_model) as materialized_features:
            await self._insert_into_feature_table(
                session,
                feature_table_model,
                materialized_features,
            )

    async def initialize_new_columns(
        self,
        session: BaseSession,
        feature_table_model: OfflineStoreFeatureTableModel,
    ) -> None:
        """
        Initialize new columns in the feature table. This is expected to be called when an
        OfflineStoreFeatureTable is updated to include new columns.

        Parameters
        ----------
        session: BaseSession
            Session object
        feature_table_model: OfflineStoreFeatureTableModel
            OfflineStoreFeatureTableModel object
        """
        has_existing_table = await self._feature_table_exists(session, feature_table_model)

        if has_existing_table:
            selected_columns = await self._ensure_compatible_schema(session, feature_table_model)
            if not selected_columns:
                return
        else:
            selected_columns = None

        async with self.materialize_features(
            session=session,
            feature_table_model=feature_table_model,
            selected_columns=selected_columns,
        ) as materialized_features:
            if not has_existing_table:
                # Create feature table is it doesn't exist yet
                await self._create_feature_table(
                    session,
                    feature_table_model,
                    materialized_features,
                )
            else:
                # Merge into existing feature table
                last_feature_timestamp = await self._get_last_feature_timestamp(
                    session, feature_table_model.name
                )
                await self._merge_into_feature_table(
                    session=session,
                    feature_table_model=feature_table_model,
                    materialized_features=materialized_features,
                    feature_timestamp_value=last_feature_timestamp,
                )

    @staticmethod
    async def _create_feature_table(
        session: BaseSession,
        feature_table_model: OfflineStoreFeatureTableModel,
        materialized_features: MaterializedFeatures,
    ) -> None:
        adapter = get_sql_adapter(session.source_type)
        query = sql_to_string(
            adapter.create_table_as(
                table_details=TableDetails(
                    database_name=session.database_name,
                    schema_name=session.schema_name,
                    table_name=feature_table_model.name,
                ),
                select_expr=expressions.select(
                    expressions.alias_(
                        adapter.current_timestamp(), alias=FEATURE_TIMESTAMP_COLUMN, quoted=True
                    )
                )
                .select(
                    *[
                        quoted_identifier(column)
                        for column in materialized_features.serving_names_and_column_names
                    ]
                )
                .from_(quoted_identifier(materialized_features.materialized_table_name)),
            ),
            source_type=session.source_type,
        )
        await session.execute_query(query)

    @staticmethod
    async def _insert_into_feature_table(
        session: BaseSession,
        feature_table_model: OfflineStoreFeatureTableModel,
        materialized_features: MaterializedFeatures,
    ) -> None:
        adapter = get_sql_adapter(session.source_type)
        query = sql_to_string(
            expressions.Insert(
                this=expressions.Schema(
                    this=expressions.Table(this=quoted_identifier(feature_table_model.name)),
                    expressions=[quoted_identifier(FEATURE_TIMESTAMP_COLUMN)]
                    + [
                        quoted_identifier(column)
                        for column in materialized_features.serving_names_and_column_names
                    ],
                ),
                expression=expressions.select(
                    expressions.alias_(
                        adapter.current_timestamp(), alias=FEATURE_TIMESTAMP_COLUMN, quoted=True
                    )
                )
                .select(
                    *[
                        quoted_identifier(column)
                        for column in materialized_features.serving_names_and_column_names
                    ]
                )
                .from_(quoted_identifier(materialized_features.materialized_table_name)),
            ),
            source_type=session.source_type,
        )
        await session.execute_query(query)

    @staticmethod
    async def _get_last_feature_timestamp(session: BaseSession, feature_table_name: str) -> str:
        query = sql_to_string(
            expressions.select(
                expressions.alias_(
                    expressions.Max(
                        this=expressions.Column(this=quoted_identifier(FEATURE_TIMESTAMP_COLUMN))
                    ),
                    alias="RESULT",
                )
            ).from_(quoted_identifier(feature_table_name)),
            source_type=session.source_type,
        )
        result = await session.execute_query(query)
        return str(result["RESULT"].iloc[0])  # type: ignore

    @staticmethod
    async def _merge_into_feature_table(
        session: BaseSession,
        feature_table_model: OfflineStoreFeatureTableModel,
        materialized_features: MaterializedFeatures,
        feature_timestamp_value: str,
    ) -> None:
        feature_timestamp_value_expr = expressions.Anonymous(
            this="TO_TIMESTAMP", expressions=[make_literal_value(feature_timestamp_value)]
        )
        merge_conditions = [
            expressions.EQ(
                this=get_qualified_column_identifier(serving_name, "offline_store_table"),
                expression=get_qualified_column_identifier(serving_name, "materialized_features"),
            )
            for serving_name in feature_table_model.serving_names
        ] + [
            expressions.EQ(
                this=quoted_identifier(FEATURE_TIMESTAMP_COLUMN),
                expression=feature_timestamp_value_expr,
            ),
        ]

        update_expr = expressions.Update(
            expressions=[
                expressions.EQ(
                    this=get_qualified_column_identifier(column, "offline_store_table"),
                    expression=get_qualified_column_identifier(column, "materialized_features"),
                )
                for column in materialized_features.column_names
            ]
        )
        insert_expr = expressions.Insert(
            this=expressions.Tuple(
                expressions=[
                    quoted_identifier(col)
                    for col in [FEATURE_TIMESTAMP_COLUMN]
                    + materialized_features.serving_names_and_column_names
                ]
            ),
            expression=expressions.Tuple(
                expressions=[feature_timestamp_value_expr]
                + [
                    get_qualified_column_identifier(col, "materialized_features")
                    for col in materialized_features.serving_names_and_column_names
                ]
            ),
        )
        merge_expr = expressions.Merge(
            this=expressions.Table(
                this=quoted_identifier(feature_table_model.name),
                alias=expressions.TableAlias(this="offline_store_table"),
            ),
            using=expressions.select(
                *[
                    quoted_identifier(column)
                    for column in materialized_features.serving_names_and_column_names
                ]
            )
            .from_(quoted_identifier(materialized_features.materialized_table_name))
            .subquery(alias="materialized_features"),
            on=expressions.and_(*merge_conditions),
            expressions=[
                expressions.When(
                    this=expressions.Column(this=expressions.Identifier(this="MATCHED")),
                    then=update_expr,
                ),
                expressions.When(
                    this=expressions.Not(
                        this=expressions.Column(this=expressions.Identifier(this="MATCHED"))
                    ),
                    then=insert_expr,
                ),
            ],
        )
        query = sql_to_string(merge_expr, source_type=session.source_type)

        await session.execute_query(query)

    @classmethod
    async def _feature_table_exists(
        cls,
        session: BaseSession,
        feature_table_model: OfflineStoreFeatureTableModel,
    ) -> bool:
        try:
            query = sql_to_string(
                expressions.select("*").from_(quoted_identifier(feature_table_model.name)).limit(1),
                source_type=session.source_type,
            )
            await session.execute_query(query)
            return True
        except session._no_schema_error:  # pylint: disable=protected-access
            return False

    @classmethod
    async def _ensure_compatible_schema(
        cls,
        session: BaseSession,
        feature_table_model: OfflineStoreFeatureTableModel,
    ) -> List[str]:
        existing_columns = [
            c.upper()
            for c in (
                await session.list_table_schema(
                    feature_table_model.name, session.database_name, session.schema_name
                )
            ).keys()
        ]
        new_columns = [
            col
            for col in feature_table_model.output_column_names
            if col.upper() not in existing_columns
        ]

        if new_columns:
            query = sql_to_string(
                expressions.AlterTable(
                    this=expressions.Table(this=quoted_identifier(feature_table_model.name)),
                    actions=cls._get_column_defs(feature_table_model, new_columns),
                ),
                source_type=session.source_type,
            )
            await session.execute_query(query)

        return new_columns

    @staticmethod
    def _get_column_defs(
        feature_table_model: OfflineStoreFeatureTableModel,
        columns: List[str],
    ) -> List[expressions.ColumnDef]:
        columns_and_types = {}
        for column_name, column_data_type in zip(
            feature_table_model.output_column_names, feature_table_model.output_dtypes
        ):
            if column_name in columns:
                columns_and_types[column_name] = column_data_type

        return [
            expressions.ColumnDef(
                this=quoted_identifier(column_name),
                kind=expressions.DataType.build(column_data_type),
            )
            for column_name, column_data_type in columns_and_types.items()
        ]

    @staticmethod
    def _get_nodes_for_selected_columns(
        feature_cluster: FeatureCluster, selected_columns: List[str]
    ) -> List[Node]:
        selected_nodes = []
        for node in feature_cluster.nodes:
            if isinstance(node, AliasNode):
                feature_name = cast(str, node.parameters.name)
            else:
                assert isinstance(node, ProjectNode)
                feature_name = cast(str, node.parameters.columns[0])
            if feature_name in selected_columns:
                selected_nodes.append(node)
        return selected_nodes
