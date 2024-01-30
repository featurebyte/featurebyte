"""
OnlineServingService class
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple, Union

import json
import os
import time
from dataclasses import dataclass
from datetime import datetime

import pandas as pd
from feast.feature_store import FeatureStore as FeastFeatureStore
from jinja2 import Template

from featurebyte.enum import SpecialColumnName
from featurebyte.exception import (
    FeatureListNotOnlineEnabledError,
    RequiredEntityNotProvidedError,
    UnsupportedRequestCodeTemplateLanguage,
)
from featurebyte.logging import get_logger
from featurebyte.models.base import PydanticObjectId, VersionIdentifier
from featurebyte.models.batch_request_table import BatchRequestTableModel
from featurebyte.models.deployment import DeploymentModel
from featurebyte.models.entity_lookup_feature_table import get_lookup_feature_table_name
from featurebyte.models.entity_validation import EntityInfo
from featurebyte.models.feature_list import FeatureCluster, FeatureListModel
from featurebyte.query_graph.model.entity_lookup_plan import EntityLookupPlanner
from featurebyte.query_graph.node.generic import GroupByNode
from featurebyte.query_graph.node.request import RequestColumnNode
from featurebyte.query_graph.node.schema import TableDetails
from featurebyte.query_graph.sql.entity import (
    get_combined_serving_names,
    get_combined_serving_names_pandas,
)
from featurebyte.query_graph.sql.online_serving import get_online_features
from featurebyte.schema.deployment import OnlineFeaturesResponseModel
from featurebyte.schema.info import DeploymentRequestCodeTemplate
from featurebyte.service.entity import EntityService
from featurebyte.service.entity_validation import EntityValidationService
from featurebyte.service.feature import FeatureService
from featurebyte.service.feature_list import FeatureListService
from featurebyte.service.feature_list_namespace import FeatureListNamespaceService
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.service.offline_store_feature_table import OfflineStoreFeatureTableService
from featurebyte.service.online_store_table_version import OnlineStoreTableVersionService
from featurebyte.service.session_manager import SessionManagerService
from featurebyte.service.table import TableService

logger = get_logger(__name__)


@dataclass
class RequestColumnsMetadata:
    """
    Metadata about the request columns
    """

    updated_request_data: List[Dict[str, Any]]
    df_extra_columns: Optional[pd.DataFrame]


class OnlineServingService:  # pylint: disable=too-many-instance-attributes
    """
    OnlineServingService is responsible for retrieving features from online store
    """

    def __init__(  # pylint: disable=too-many-arguments
        self,
        session_manager_service: SessionManagerService,
        entity_validation_service: EntityValidationService,
        online_store_table_version_service: OnlineStoreTableVersionService,
        feature_store_service: FeatureStoreService,
        feature_list_namespace_service: FeatureListNamespaceService,
        feature_list_service: FeatureListService,
        feature_service: FeatureService,
        entity_service: EntityService,
        table_service: TableService,
        offline_store_feature_table_service: OfflineStoreFeatureTableService,
    ):
        self.feature_store_service = feature_store_service
        self.session_manager_service = session_manager_service
        self.entity_validation_service = entity_validation_service
        self.online_store_table_version_service = online_store_table_version_service
        self.feature_list_namespace_service = feature_list_namespace_service
        self.feature_list_service = feature_list_service
        self.feature_service = feature_service
        self.entity_service = entity_service
        self.table_service = table_service
        self.offline_store_feature_table_service = offline_store_feature_table_service

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
        feast_store: FeastFeatureStore,
        request_data: List[Dict[str, Any]],
    ) -> OnlineFeaturesResponseModel:
        """
        Get online features for a Feature List via feast online store

        Parameters
        ----------
        feature_list: FeatureListModel
            Feature List
        feast_store: FeastFeatureStore
            FeastFeatureStore object
        request_data: List[Dict[str, Any]]
            Request data containing entity serving names

        Returns
        -------
        OnlineFeaturesResponseModel
        """
        assert feature_list.feature_clusters is not None
        feature_cluster = feature_list.feature_clusters[0]

        # Validate required entities are present
        request_column_names = set(request_data[0].keys())
        entity_info = await self.entity_validation_service.get_entity_info_from_request(
            graph=feature_cluster.graph,
            nodes=feature_cluster.nodes,
            request_column_names=request_column_names,
        )

        # Lookup parent entities if needed before retrieving features from feature service
        if entity_info.missing_entities:
            request_data, added_column_names = self._lookup_parent_entities_by_feast(
                request_data=request_data,
                feature_list=feature_list,
                entity_info=entity_info,
                feast_store=feast_store,
            )
        else:
            added_column_names = []

        request_data_metadata = await self._process_request_columns(
            request_data=request_data,
            feature_ids=feature_list.feature_ids,
            feature_cluster=feature_list.feature_clusters[0],
            added_column_names=added_column_names,
        )
        tic = time.time()
        feast_online_features = feast_store.get_online_features(
            feast_store.get_feature_service(feature_list.versioned_name),
            request_data_metadata.updated_request_data,
        )
        logger.debug("Feast get_online_features took %f seconds", time.time() - tic)

        # Map feature names to the original names
        feature_docs = await self.feature_service.list_documents_as_dict(
            query_filter={"_id": {"$in": feature_list.feature_ids}},
            projection={"name": 1, "version": 1},
        )
        feature_name_map = {}
        for feature_doc in feature_docs["data"]:
            feature_name = feature_doc["name"]
            feature_version = VersionIdentifier(**feature_doc["version"]).to_str()
            feature_name_map[f"{feature_name}_{feature_version}"] = feature_name
        online_features_df = feast_online_features.to_df().rename(columns=feature_name_map)

        # Excluded looked up parent entities from result
        if added_column_names:
            online_features_df.drop(added_column_names, axis=1, inplace=True)

        # Add back extra columns
        if request_data_metadata.df_extra_columns:
            for col in request_data_metadata.df_extra_columns:
                online_features_df[col] = request_data_metadata.df_extra_columns[col].values

        features = online_features_df.to_dict(orient="records")
        return OnlineFeaturesResponseModel(features=features)

    @staticmethod
    def _lookup_parent_entities_by_feast(
        request_data: List[Dict[str, Any]],
        feature_list: FeatureListModel,
        entity_info: EntityInfo,
        feast_store: FeastFeatureStore,
    ) -> Tuple[List[Dict[str, Any]], List[str]]:
        # Validate missing entities can be looked up based on available relationships
        try:
            assert feature_list.relationships_info is not None
            lookup_steps = EntityLookupPlanner.generate_lookup_steps(
                available_entity_ids=[entity.id for entity in entity_info.provided_entities],
                required_entity_ids=[entity.id for entity in entity_info.required_entities],
                relationships_info=feature_list.relationships_info,
            )
        except RequiredEntityNotProvidedError as exc:
            assert exc.missing_entity_ids is not None
            raise RequiredEntityNotProvidedError(  # pylint: disable=raise-missing-from
                entity_info.format_missing_entities_error(exc.missing_entity_ids)
            )

        # Lookup parent entities through feast store
        added_column_names = []
        df_entity_rows = pd.DataFrame(request_data)
        for lookup_step in lookup_steps:
            child_entity = entity_info.get_entity(lookup_step.entity_id)
            parent_entity = entity_info.get_entity(lookup_step.related_entity_id)
            lookup_feature_name = parent_entity.serving_names[0]
            entity_lookup_rows = df_entity_rows[[child_entity.serving_names[0]]].to_dict(
                orient="records"
            )
            entity_lookup_feast_spec = [
                f"{get_lookup_feature_table_name(lookup_step.id)}:{lookup_feature_name}"
            ]
            entity_lookup_result = feast_store.get_online_features(
                entity_lookup_feast_spec, entity_lookup_rows
            ).to_df()
            df_entity_rows[lookup_feature_name] = entity_lookup_result[lookup_feature_name].values
            added_column_names.append(lookup_feature_name)

        request_data = df_entity_rows.to_dict(orient="records")
        return request_data, added_column_names

    async def _process_request_columns(
        self,
        request_data: List[Dict[str, Any]],
        feature_ids: List[PydanticObjectId],
        feature_cluster: FeatureCluster,
        added_column_names: List[str],
    ) -> RequestColumnsMetadata:
        """
        Perform additional handling on the provided columns in the request data to prepare it for
        feast get_online_features().

        - Add point in time column if required
        - Composite entities are combined into a single column
        - Remove columns that are not required since feast is strict and will complain about them

        Parameters
        ----------
        request_data : List[Dict[str, Any]]
            Request data to be processed
        feature_ids : List[PydanticObjectId]
            List of feature ids in the feature list
        feature_cluster : FeatureCluster
            Feature cluster in the feature list
        added_column_names : List[str]
            List of column names that were added to the request data. Will be updated in-place.

        Returns
        -------
        RequestColumnsMetadata
        """
        # Include point in time column if it is required
        is_point_in_time_column_required = self._require_point_in_time_request_column(
            feature_cluster
        )
        if is_point_in_time_column_required:
            point_in_time_value = datetime.utcnow().isoformat()
            for row in request_data:
                row[SpecialColumnName.POINT_IN_TIME] = point_in_time_value

        # Get required serving names and composite serving names that need further processing
        offline_store_table_docs = (
            await self.offline_store_feature_table_service.list_documents_as_dict(
                query_filter={"feature_ids": {"$in": feature_ids}},
                project_name={"serving_names"},
            )
        )
        required_serving_names = set()
        composite_serving_names = set()
        for offline_store_table_doc in offline_store_table_docs["data"]:
            serving_names = tuple(offline_store_table_doc["serving_names"])
            if len(serving_names) == 1:
                required_serving_names.add(serving_names[0])
            elif len(serving_names) > 1:
                composite_serving_names.add(serving_names)

        # Add concatenated composite serving names
        df_request_data = pd.DataFrame(request_data)
        if composite_serving_names:
            for serving_names in composite_serving_names:
                combined_serving_names_col = get_combined_serving_names(list(serving_names))
                df_request_data[combined_serving_names_col] = get_combined_serving_names_pandas(
                    [df_request_data[serving_name] for serving_name in serving_names]
                )
                added_column_names.append(combined_serving_names_col)

        # Get exactly the columns that are required by feast
        needed_columns = list(required_serving_names) + added_column_names
        if is_point_in_time_column_required:
            needed_columns.append(SpecialColumnName.POINT_IN_TIME.value)

        # Remove columns that are not required to be added back later to the result
        extra_columns = [col for col in df_request_data.columns if col not in needed_columns]
        if extra_columns:
            df_extra_columns = df_request_data[extra_columns]
        else:
            df_extra_columns = None

        df_request_data = df_request_data[needed_columns]
        request_data = df_request_data.to_dict(orient="records")

        return RequestColumnsMetadata(
            updated_request_data=request_data,
            df_extra_columns=df_extra_columns,
        )

    @staticmethod
    def _require_point_in_time_request_column(feature_cluster: FeatureCluster) -> bool:
        for node in feature_cluster.nodes:
            for node in feature_cluster.graph.iterate_nodes(node, node_type=None):
                if isinstance(node, RequestColumnNode):
                    if node.parameters.column_name == SpecialColumnName.POINT_IN_TIME:
                        return True

                if isinstance(node, GroupByNode):
                    # TTL handling requires point in time column
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
