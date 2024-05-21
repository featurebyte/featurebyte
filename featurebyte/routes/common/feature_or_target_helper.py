"""
Feature or target helper
"""

from typing import Any, Dict, List

from featurebyte.models.base import PydanticObjectId
from featurebyte.query_graph.graph import QueryGraph
from featurebyte.routes.catalog.catalog_name_injector import CatalogNameInjector
from featurebyte.service.table import TableService


class FeatureOrTargetHelper:
    """
    Feature or target helper class
    """

    def __init__(
        self,
        catalog_name_injector: CatalogNameInjector,
        table_service: TableService,
    ):
        self.catalog_name_injector = catalog_name_injector
        self.table_service = table_service

    async def get_primary_tables(
        self,
        table_ids: List[PydanticObjectId],
        catalog_id: PydanticObjectId,
        graph: QueryGraph,
        node_name: str,
    ) -> List[Dict[str, Any]]:
        """
        Get primary tables

        Parameters
        ----------
        table_ids: List[PydanticObjectId]
            Table ids
        catalog_id: PydanticObjectId
            Catalog id
        graph: QueryGraph
            Query graph
        node_name: str
            Node name

        Returns
        -------
        List[dict]
            List of primary tables
        """
        # Get input table metadata
        tables = await self.table_service.list_documents_as_dict(
            page=1, page_size=0, query_filter={"_id": {"$in": table_ids}}
        )
        # Add catalog name to entities and tables
        _, updated_docs = await self.catalog_name_injector.add_name(catalog_id, [tables])
        tables = updated_docs[0]

        # derive primary tables
        table_id_to_doc = {table["_id"]: table for table in tables["data"]}
        primary_input_nodes = graph.get_primary_input_nodes(node_name=node_name)
        return [table_id_to_doc[node.parameters.id] for node in primary_input_nodes]
