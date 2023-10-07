"""
OnlineServingService class
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Union

import json
import os

import pandas as pd
from jinja2 import Template

from featurebyte.exception import (
    FeatureListNotOnlineEnabledError,
    UnsupportedRequestCodeTemplateLanguage,
)
from featurebyte.models.batch_request_table import BatchRequestTableModel
from featurebyte.models.deployment import DeploymentModel
from featurebyte.models.feature_list import FeatureListModel
from featurebyte.models.feature_store import FeatureStoreModel, TableModel
from featurebyte.query_graph.node.schema import TableDetails
from featurebyte.query_graph.sql.interpreter import GraphInterpreter
from featurebyte.query_graph.sql.online_serving import get_online_features
from featurebyte.schema.deployment import OnlineFeaturesResponseModel
from featurebyte.schema.info import DeploymentRequestCodeTemplate
from featurebyte.service.entity import EntityService, get_primary_entity_from_entities
from featurebyte.service.entity_validation import EntityValidationService
from featurebyte.service.feature_list_namespace import FeatureListNamespaceService
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.service.online_store_table_version import OnlineStoreTableVersionService
from featurebyte.service.session_manager import SessionManagerService
from featurebyte.service.table import TableService


class OnlineServingService:
    """
    OnlineServingService is responsible for retrieving features from online store
    """

    def __init__(
        self,
        session_manager_service: SessionManagerService,
        entity_validation_service: EntityValidationService,
        online_store_table_version_service: OnlineStoreTableVersionService,
        feature_store_service: FeatureStoreService,
        feature_list_namespace_service: FeatureListNamespaceService,
        entity_service: EntityService,
        table_service: TableService,
    ):
        self.feature_store_service = feature_store_service
        self.session_manager_service = session_manager_service
        self.entity_validation_service = entity_validation_service
        self.online_store_table_version_service = online_store_table_version_service
        self.feature_list_namespace_service = feature_list_namespace_service
        self.entity_service = entity_service
        self.table_service = table_service

    async def get_online_features_from_feature_list(
        self,
        feature_list: FeatureListModel,
        request_data: Union[List[Dict[str, Any]], BatchRequestTableModel],
        output_table_details: Optional[TableDetails] = None,
    ) -> Optional[OnlineFeaturesResponseModel]:
        """
        Get online features for a Feature List given a list of entity serving names

        Parameters
        ----------
        feature_list: FeatureListModel
            Feature List
        request_data: Union[List[Dict[str, Any]], BatchRequestTableModel]
            Request data containing entity serving names
        output_table_details: Optional[TableDetails]
            Output table details

        Returns
        -------
        Optional[OnlineFeaturesResponseModel]

        Raises
        ------
        RuntimeError
            When the provided FeatureList is not available for online serving
        FeatureListNotOnlineEnabledError
            When the provided FeatureList is not online enabled
        """

        if feature_list.feature_clusters is None:
            raise RuntimeError("Online serving not available for this Feature List")

        if not feature_list.deployed:
            raise FeatureListNotOnlineEnabledError("Feature List is not online enabled")

        feature_cluster = feature_list.feature_clusters[0]
        feature_store = await self.feature_store_service.get_document(
            document_id=feature_cluster.feature_store_id
        )

        if isinstance(request_data, list):
            request_input = pd.DataFrame(request_data)
            request_column_names = set(request_data[0].keys())
        else:
            request_input = request_data
            request_column_names = {col.name for col in request_data.columns_info}

        parent_serving_preparation = (
            await self.entity_validation_service.validate_entities_or_prepare_for_parent_serving(
                graph=feature_cluster.graph,
                nodes=feature_cluster.nodes,
                request_column_names=request_column_names,
                feature_store=feature_store,
            )
        )

        db_session = await self.session_manager_service.get_feature_store_session(
            feature_store=feature_store,
        )
        features = await get_online_features(
            session=db_session,
            graph=feature_cluster.graph,
            nodes=feature_cluster.nodes,
            request_data=request_input,
            source_type=feature_store.type,
            parent_serving_preparation=parent_serving_preparation,
            output_table_details=output_table_details,
            online_store_table_version_service=self.online_store_table_version_service,
        )
        if features is None:
            return None
        return OnlineFeaturesResponseModel(features=features)

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
            table_data_dict=table.dict(by_alias=True),
        )
        db_session = await self.session_manager_service.get_feature_store_session(
            feature_store=feature_store,
        )
        unique_values_sql = GraphInterpreter(
            graph, source_type=feature_store.type
        ).construct_unique_values_sql(
            node_name=node.name, column_name=column_name, num_rows=num_rows
        )
        result = await db_session.execute_query(unique_values_sql)
        assert result is not None
        unique_values: List[Any] = result[column_name].to_list()
        return unique_values

    async def get_request_code_template(  # pylint: disable=too-many-locals
        self,
        deployment: DeploymentModel,
        feature_list: FeatureListModel,
        language: str,
    ) -> DeploymentRequestCodeTemplate:
        """
        Get request code template for a deployment

        Parameters
        ----------
        deployment: DeploymentModel
            Deployment model
        feature_list: FeatureListModel
            Feature List model
        language: str
            Language of the template

        Returns
        -------
        DeploymentRequestCodeTemplate

        Raises
        ------
        UnsupportedRequestCodeTemplateLanguage
            When the provided language is not supported
        """

        template_file_path = os.path.join(
            os.path.dirname(__file__), f"templates/online_serving/{language}.tpl"
        )
        if not os.path.exists(template_file_path):
            raise UnsupportedRequestCodeTemplateLanguage("Supported languages: ['python', 'sh']")

        # construct entity serving names
        entity_serving_names = await self.get_sample_entity_serving_names(
            feature_list=feature_list,
            count=1,
        )

        # construct serving url
        headers = {
            "Content-Type": "application/json",
            "active-catalog-id": str(feature_list.catalog_id),
            "Authorization": "Bearer <API_TOKEN>",
        }

        # populate template
        with open(
            file=template_file_path,
            mode="r",
            encoding="utf-8",
        ) as file_object:
            template = Template(file_object.read())

        return DeploymentRequestCodeTemplate(
            code_template=template.render(
                headers=json.dumps(headers),
                header_params=" \\\n    ".join(
                    [f"-H '{key}: {value}'" for key, value in headers.items()]
                ),
                serving_url=f"<FEATUREBYTE_SERVICE_URL>/deployment/{deployment.id}/online_features",
                entity_serving_names=json.dumps(entity_serving_names),
            ),
            entity_serving_names=entity_serving_names,
            language=language,
        )

    async def get_sample_entity_serving_names(  # pylint: disable=too-many-locals
        self, feature_list: FeatureListModel, count: int
    ) -> List[Dict[str, str]]:
        """
        Get request code template for a deployment

        Parameters
        ----------
        feature_list: FeatureListModel
            Feature List model
        count: int
            Number of sample entity serving names to return

        Returns
        -------
        List[Dict[str, str]]
        """
        # get entities and tables used for the feature list
        feature_list_namespace = await self.feature_list_namespace_service.get_document(
            document_id=feature_list.feature_list_namespace_id
        )
        entities_docs = await self.entity_service.list_documents_as_dict(
            page=1, page_size=0, query_filter={"_id": {"$in": feature_list_namespace.entity_ids}}
        )
        primary_entity = get_primary_entity_from_entities(entities_docs)
        entities = {
            entity["_id"]: {"serving_name": entity["serving_names"]}
            for entity in primary_entity["data"]
        }
        tables = self.table_service.list_documents_iterator(
            query_filter={"_id": {"$in": feature_list_namespace.table_ids}}
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
