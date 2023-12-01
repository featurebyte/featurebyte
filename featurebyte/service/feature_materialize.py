"""
FeatureMaterializeService class
"""
from __future__ import annotations

from typing import List, Optional

from dataclasses import dataclass

from bson import ObjectId

from featurebyte.models.offline_store_feature_table import OfflineStoreFeatureTableModel
from featurebyte.query_graph.node.schema import TableDetails
from featurebyte.query_graph.sql.adapter import get_sql_adapter
from featurebyte.query_graph.sql.common import sql_to_string
from featurebyte.query_graph.sql.online_serving import (
    TemporaryBatchRequestTable,
    get_online_features,
)
from featurebyte.service.entity import EntityService
from featurebyte.service.feature import FeatureService
from featurebyte.service.online_store_compute_query_service import OnlineStoreComputeQueryService
from featurebyte.service.online_store_table_version import OnlineStoreTableVersionService
from featurebyte.session.base import BaseSession


@dataclass
class MaterializedFeatures:
    """
    Information about materialised features ready to be published
    """

    materialized_table_name: str
    names: List[str]
    data_types: List[str]
    serving_names: List[str]


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

    async def materialize_features(
        self,
        session: BaseSession,
        feature_table_model: OfflineStoreFeatureTableModel,
    ) -> Optional[MaterializedFeatures]:
        """
        Materialise currently online enabled features matching the given primary entity and feature
        job settings into a new table in the data warehouse.

        Parameters
        ----------
        session: BaseSession
            Session object
        feature_table_model: OfflineStoreFeatureTableModel
            OfflineStoreFeatureTableModel object

        Returns
        -------
        Optional[MaterializedFeatures]
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
        graph, node_names = feature_table_model.ingest_graph_and_node_names
        try:
            await get_online_features(
                session=session,
                graph=graph,  # type: ignore
                nodes=[graph.get_node_by_name(node_name) for node_name in node_names],
                request_data=batch_request_table,
                source_type=session.source_type,
                online_store_table_version_service=self.online_store_table_version_service,
                output_table_details=output_table_details,
            )
        finally:
            # Delete temporary batch request table
            await session.drop_table(
                table_name=batch_request_table.table_details.table_name,
                schema_name=batch_request_table.table_details.schema_name,  # type: ignore
                database_name=batch_request_table.table_details.database_name,  # type: ignore
                if_exists=True,
            )

        return MaterializedFeatures(
            materialized_table_name=output_table_details.table_name,
            names=feature_table_model.output_column_names,
            data_types=[
                adapter.get_physical_type_from_dtype(dtype)
                for dtype in feature_table_model.output_dtypes
            ],
            serving_names=feature_table_model.serving_names,
        )
