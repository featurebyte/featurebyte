"""
Entity serving names service
"""

from typing import Any, Dict, List, Optional, Sequence

from bson import ObjectId

from featurebyte.models.feature_store import FeatureStoreModel, TableModel
from featurebyte.models.relationship_analysis import derive_primary_entity
from featurebyte.query_graph.sql.interpreter import GraphInterpreter
from featurebyte.service.entity import EntityService
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.service.session_manager import SessionManagerService
from featurebyte.service.table import TableService


class EntityServingNamesService:
    """
    EntityServingNamesService class is responsible for gett.
    """

    def __init__(
        self,
        session_manager_service: SessionManagerService,
        feature_store_service: FeatureStoreService,
        entity_service: EntityService,
        table_service: TableService,
    ):
        self.session_manager_service = session_manager_service
        self.feature_store_service = feature_store_service
        self.entity_service = entity_service
        self.table_service = table_service

    async def get_table_column_unique_values(
        self,
        feature_store: FeatureStoreModel,
        table: TableModel,
        column_name: str,
        num_rows: int,
    ) -> List[Any]:
        """
        Get unique values for a column in a table

        Parameters
        ----------
        feature_store: FeatureStoreModel
            Feature store
        table: TableModel
            Table
        column_name: str
            Column name
        num_rows: int
            Number of rows to return

        Returns
        -------
        List[Any]
        """
        graph, node = table.construct_graph_and_node(
            feature_store_details=feature_store.get_feature_store_details(),
            table_data_dict=table.model_dump(by_alias=True),
        )
        db_session = await self.session_manager_service.get_feature_store_session(
            feature_store=feature_store,
        )
        unique_values_sql = GraphInterpreter(
            graph, source_info=feature_store.get_source_info()
        ).construct_unique_values_sql(
            node_name=node.name, column_name=column_name, num_rows=num_rows
        )
        result = await db_session.execute_query(unique_values_sql)
        assert result is not None
        unique_values: List[Any] = result[column_name].to_list()
        return unique_values

    async def get_sample_entity_serving_names(
        self, entity_ids: Sequence[ObjectId], table_ids: Optional[Sequence[ObjectId]], count: int
    ) -> List[Dict[str, str]]:
        """
        Get sample entity serving names for a list of entities and tables

        Parameters
        ----------
        entity_ids: Sequence[ObjectId]
            List of entity ids
        table_ids: Optional[Sequence[ObjectId]]
            List of table ids
        count: int
            Number of sample entity serving names to return

        Returns
        -------
        List[Dict[str, str]]
        """
        primary_entity = derive_primary_entity(
            entities=[
                entity
                async for entity in self.entity_service.list_documents_iterator(
                    query_filter={"_id": {"$in": list(entity_ids)}}
                )
            ]
        )
        entities: Dict[ObjectId, Dict[str, List[str]]] = {
            entity.id: {"serving_name": entity.serving_names} for entity in primary_entity
        }
        if table_ids is None:
            table_ids = []
            for entity in primary_entity:
                if entity.primary_table_ids:
                    table_ids.append(entity.primary_table_ids[0])
                else:
                    table_ids.append(entity.table_ids[0])

        tables = self.table_service.list_documents_iterator(
            query_filter={"_id": {"$in": table_ids}}
        )

        feature_store: Optional[FeatureStoreModel] = None
        async for table in tables:
            assert isinstance(table, TableModel)
            if not feature_store:
                feature_store = await self.feature_store_service.get_document(
                    document_id=table.tabular_source.feature_store_id
                )
            else:
                assert (
                    feature_store.id == table.tabular_source.feature_store_id
                ), "Feature List tables must be in the same feature store"

            entity_columns = [
                column for column in table.columns_info if column.entity_id in entities
            ]
            if entity_columns:
                for column in entity_columns:
                    # skip if sample values already exists unless column is a primary key for the table
                    assert column.entity_id is not None, column
                    existing_sample_values = entities[column.entity_id].get("sample_value")
                    if existing_sample_values and column.name not in table.primary_key_columns:
                        continue

                    entities[column.entity_id][
                        "sample_value"
                    ] = await self.get_table_column_unique_values(
                        feature_store=feature_store,
                        table=table,
                        column_name=column.name,
                        num_rows=count,
                    )

        return [
            {
                entity["serving_name"][0]: entity["sample_value"][
                    row_idx % len(entity["sample_value"])
                ]
                for entity in entities.values()
            }
            for row_idx in range(count)
        ]

    async def get_entity_id_to_serving_name_for_offline_store(
        self, entity_ids: Sequence[ObjectId]
    ) -> Dict[ObjectId, str]:
        """
        Get entity id to serving name mapping for offline store

        Parameters
        ----------
        entity_ids: List[ObjectId]
            List of entity ids

        Returns
        -------
        Dict[ObjectId, str]
        """
        primary_entities = [
            entity
            async for entity in self.entity_service.list_documents_iterator(
                query_filter={"_id": {"$in": entity_ids}}
            )
        ]
        return {entity.id: entity.serving_names[0] for entity in primary_entities}
