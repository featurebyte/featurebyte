"""
OnlineServingService class
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Union, cast

import json
import os
from datetime import datetime

import pandas as pd
from jinja2 import Template

from featurebyte.enum import SpecialColumnName
from featurebyte.exception import (
    FeatureListNotOnlineEnabledError,
    UnsupportedRequestCodeTemplateLanguage,
)
from featurebyte.feast.service.feature_store import FeastFeatureStoreService
from featurebyte.models.batch_request_table import BatchRequestTableModel
from featurebyte.models.deployment import DeploymentModel
from featurebyte.models.feature_list import FeatureCluster, FeatureListModel
from featurebyte.query_graph.enum import NodeType
from featurebyte.query_graph.node.request import RequestColumnNode
from featurebyte.query_graph.node.schema import TableDetails
from featurebyte.query_graph.sql.online_serving import get_online_features
from featurebyte.schema.deployment import OnlineFeaturesResponseModel
from featurebyte.schema.info import DeploymentRequestCodeTemplate
from featurebyte.service.entity import EntityService
from featurebyte.service.entity_validation import EntityValidationService
from featurebyte.service.feature_list import FeatureListService
from featurebyte.service.feature_list_namespace import FeatureListNamespaceService
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.service.online_store_table_version import OnlineStoreTableVersionService
from featurebyte.service.session_manager import SessionManagerService
from featurebyte.service.table import TableService


class OnlineServingService:  # pylint: disable=too-many-instance-attributes
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
        feature_list_service: FeatureListService,
        entity_service: EntityService,
        table_service: TableService,
        feast_feature_store_service: FeastFeatureStoreService,
    ):
        self.feature_store_service = feature_store_service
        self.session_manager_service = session_manager_service
        self.entity_validation_service = entity_validation_service
        self.online_store_table_version_service = online_store_table_version_service
        self.feature_list_namespace_service = feature_list_namespace_service
        self.feature_list_service = feature_list_service
        self.entity_service = entity_service
        self.table_service = table_service
        self.feast_feature_store_service = feast_feature_store_service

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

    async def get_online_features_by_feast(
        self,
        feature_list: FeatureListModel,
        request_data: List[Dict[str, Any]],
    ) -> OnlineFeaturesResponseModel:
        """
        Get online features for a Feature List via feast online store

        Parameters
        ----------
        feature_list: FeatureListModel
            Feature List
        request_data: List[Dict[str, Any]]
            Request data containing entity serving names

        Returns
        -------
        OnlineFeaturesResponseModel

        Raises
        ------
        RuntimeError
            When the provided FeatureList is not available for online serving using feast.
        """
        feast_store = await self.feast_feature_store_service.get_feast_feature_store_for_catalog()
        if feast_store is None:
            raise RuntimeError("Feast feature store is not available")

        feature_service = feast_store.get_feature_service(feature_list.name)
        entity_rows = request_data[:]

        assert feature_list.feature_clusters is not None
        feature_cluster = feature_list.feature_clusters[0]

        # Validate required entities are present
        request_column_names = set(request_data[0].keys())
        feature_store = await self.feature_store_service.get_document(
            document_id=feature_cluster.feature_store_id
        )
        _ = await self.entity_validation_service.validate_entities_or_prepare_for_parent_serving(
            graph=feature_cluster.graph,
            nodes=feature_cluster.nodes,
            request_column_names=request_column_names,
            feature_store=feature_store,
        )

        # Include point in time column if it is required
        has_point_in_time_column = self._has_point_in_time_request_column(feature_cluster)
        if has_point_in_time_column:
            point_in_time_value = datetime.utcnow().isoformat()
            for row in entity_rows:
                row[SpecialColumnName.POINT_IN_TIME] = point_in_time_value

        feast_online_features = feast_store.get_online_features(feature_service, request_data)

        return OnlineFeaturesResponseModel(
            features=feast_online_features.to_df().to_dict(orient="records")
        )

    @staticmethod
    def _has_point_in_time_request_column(feature_cluster: FeatureCluster) -> bool:
        for node in feature_cluster.nodes:
            for request_column_node in feature_cluster.graph.iterate_nodes(
                node, NodeType.REQUEST_COLUMN
            ):
                request_column_node = cast(RequestColumnNode, request_column_node)
                if request_column_node.parameters.column_name == SpecialColumnName.POINT_IN_TIME:
                    return True
        return False

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
        entity_serving_names = await self.feature_list_service.get_sample_entity_serving_names(
            feature_list_id=feature_list.id,
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
