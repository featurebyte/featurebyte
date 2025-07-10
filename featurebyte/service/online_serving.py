"""
OnlineServingService class
"""

from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional, Union
from unittest.mock import patch

import pandas as pd
from feast import OnDemandFeatureView
from feast.base_feature_view import BaseFeatureView
from feast.feature_store import FeatureStore as FeastFeatureStore
from jinja2 import Template

from featurebyte.enum import SpecialColumnName
from featurebyte.exception import (
    DeploymentNotEnabledError,
    RequiredEntityNotProvidedError,
    UnsupportedRequestCodeTemplateLanguage,
)
from featurebyte.feast.patch import (
    augment_response_with_on_demand_transforms,
    transform_arrow,
    with_projection,
)
from featurebyte.logging import get_logger
from featurebyte.models.base import PydanticObjectId, VersionIdentifier
from featurebyte.models.batch_request_table import BatchRequestTableModel
from featurebyte.models.deployment import DeploymentModel
from featurebyte.models.entity_validation import EntityInfo
from featurebyte.models.feature_list import FeatureListModel
from featurebyte.models.system_metrics import BatchFeaturesMetrics
from featurebyte.query_graph.node.schema import TableDetails
from featurebyte.query_graph.sql.entity import (
    get_combined_serving_names,
    get_combined_serving_names_python,
)
from featurebyte.query_graph.sql.online_serving import get_online_features
from featurebyte.schema.deployment import OnlineFeaturesResponseModel
from featurebyte.schema.info import DeploymentRequestCodeTemplate
from featurebyte.service.column_statistics import ColumnStatisticsService
from featurebyte.service.cron_helper import CronHelper
from featurebyte.service.deployed_tile_table import DeployedTileTableService
from featurebyte.service.entity import EntityService
from featurebyte.service.entity_serving_names import EntityServingNamesService
from featurebyte.service.entity_validation import EntityValidationService
from featurebyte.service.feature import FeatureService
from featurebyte.service.feature_list import FeatureListService
from featurebyte.service.feature_list_namespace import FeatureListNamespaceService
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.service.offline_store_feature_table import OfflineStoreFeatureTableService
from featurebyte.service.online_store_table_version import OnlineStoreTableVersionService
from featurebyte.service.session_manager import SessionManagerService
from featurebyte.service.system_metrics import SystemMetricsService
from featurebyte.service.table import TableService
from featurebyte.session.session_helper import SessionHandler

logger = get_logger(__name__)


@dataclass
class RequestColumnsMetadata:
    """
    Metadata about the request columns
    """

    updated_request_data: List[Dict[str, Any]]
    df_extra_columns: Optional[pd.DataFrame]


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
        feature_service: FeatureService,
        entity_service: EntityService,
        table_service: TableService,
        offline_store_feature_table_service: OfflineStoreFeatureTableService,
        entity_serving_names_service: EntityServingNamesService,
        feature_list_service: FeatureListService,
        cron_helper: CronHelper,
        system_metrics_service: SystemMetricsService,
        deployed_tile_table_service: DeployedTileTableService,
        column_statistics_service: ColumnStatisticsService,
    ):
        self.feature_store_service = feature_store_service
        self.session_manager_service = session_manager_service
        self.entity_validation_service = entity_validation_service
        self.online_store_table_version_service = online_store_table_version_service
        self.feature_list_namespace_service = feature_list_namespace_service
        self.feature_service = feature_service
        self.entity_service = entity_service
        self.table_service = table_service
        self.offline_store_feature_table_service = offline_store_feature_table_service
        self.entity_serving_names_service = entity_serving_names_service
        self.feature_list_service = feature_list_service
        self.cron_helper = cron_helper
        self.system_metrics_service = system_metrics_service
        self.deployed_tile_table_service = deployed_tile_table_service
        self.column_statistics_service = column_statistics_service

    async def get_online_features_from_feature_list(
        self,
        feature_list: FeatureListModel,
        request_data: Union[List[Dict[str, Any]], BatchRequestTableModel],
        output_table_details: Optional[TableDetails] = None,
        batch_feature_table_id: Optional[PydanticObjectId] = None,
        point_in_time: Optional[datetime] = None,
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
        batch_feature_table_id: Optional[PydanticObjectId]
            Batch feature table ID to track the time taken for the online serving request
        point_in_time: Optional[datetime]
            Point in time to use for the request. If not provided, the current time will be used.

        Returns
        -------
        Optional[OnlineFeaturesResponseModel]

        Raises
        ------
        RuntimeError
            When the provided FeatureList is not available for online serving
        DeploymentNotEnabledError
            When the provided FeatureList is not online enabled
        """

        if feature_list.feature_clusters is None:
            raise RuntimeError("Online serving not available for this Feature List")

        if not feature_list.deployed:
            raise DeploymentNotEnabledError("Deployment is not enabled")

        _tic = time.time()
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
                feature_list_model=feature_list,
                request_column_names=request_column_names,
                feature_store=feature_store,
            )
        )

        db_session = await self.session_manager_service.get_feature_store_session(
            feature_store=feature_store,
        )
        on_demand_tile_tables = (
            await self.deployed_tile_table_service.get_deployed_tile_table_info()
        ).on_demand_tile_tables
        features = await get_online_features(
            session_handler=SessionHandler(
                session=db_session,
                redis=self.online_store_table_version_service.redis,
                feature_store=feature_store,
                system_metrics_service=self.system_metrics_service,
            ),
            cron_helper=self.cron_helper,
            column_statistics_service=self.column_statistics_service,
            graph=feature_cluster.graph,
            nodes=feature_cluster.nodes,
            request_data=request_input,
            request_timestamp=point_in_time,
            source_info=feature_store.get_source_info(),
            parent_serving_preparation=parent_serving_preparation,
            output_table_details=output_table_details,
            online_store_table_version_service=self.online_store_table_version_service,
            on_demand_tile_tables=on_demand_tile_tables,
        )
        if batch_feature_table_id is not None:
            await self.system_metrics_service.create_metrics(
                BatchFeaturesMetrics(
                    batch_feature_table_id=batch_feature_table_id,
                    total_seconds=time.time() - _tic,
                )
            )
        if features is None:
            return None
        return OnlineFeaturesResponseModel(features=features)

    async def get_online_features_by_feast(
        self,
        feature_list: FeatureListModel,
        deployment: DeploymentModel,
        feast_store: FeastFeatureStore,
        request_data: List[Dict[str, Any]],
    ) -> OnlineFeaturesResponseModel:
        """
        Get online features for a Feature List via feast online store

        Parameters
        ----------
        feature_list: FeatureListModel
            Feature List
        deployment: DeploymentModel
            Deployment model
        feast_store: FeastFeatureStore
            FeastFeatureStore object
        request_data: List[Dict[str, Any]]
            Request data containing entity serving names

        Returns
        -------
        OnlineFeaturesResponseModel

        Raises
        ------
        RequiredEntityNotProvidedError
            If required entities for serving are not provided
        """
        assert feature_list.feature_clusters is not None

        # Original request data to be concatenated with features retrieved from feast
        df_features = [pd.DataFrame(request_data)]

        # Lookup parent entities to retrieve feature list's primary entity. This will validate that
        # the required entities are present.
        request_column_names = set(request_data[0].keys())
        required_entities = await self.entity_service.get_entities(
            set(deployment.serving_entity_ids or feature_list.primary_entity_ids)
        )
        provided_entities = await self.entity_service.get_entities_with_serving_names(
            request_column_names,
        )
        provided_entity_ids = {entity.id for entity in provided_entities}
        if not provided_entity_ids.issuperset([entity.id for entity in required_entities]):
            # Provided entities cannot be served, raise an error message with information
            entity_info = EntityInfo(
                required_entities=required_entities,
                provided_entities=provided_entities,
            )
            raise RequiredEntityNotProvidedError(
                entity_info.format_missing_entities_error([
                    entity.id for entity in entity_info.missing_entities
                ])
            )

        # Map feature names to the original names
        feature_name_map = {}
        feature_id_to_versioned_name = {}
        async for feature_doc in self.feature_service.list_documents_as_dict_iterator(
            query_filter={"_id": {"$in": feature_list.feature_ids}},
            projection={"name": 1, "version": 1},
        ):
            feature_name = feature_doc["name"]
            feature_version = VersionIdentifier(**feature_doc["version"]).to_str()
            feature_name_map[f"{feature_name}_{feature_version}"] = feature_name
            feature_id_to_versioned_name[feature_doc["_id"]] = f"{feature_name}_{feature_version}"

        if feast_store.list_on_demand_feature_views():
            # if the feast store contains on-demand feature views, point in time is always required
            point_in_time_value = datetime.utcnow().isoformat()
        else:
            point_in_time_value = None

        tic = time.time()
        df_feast_online_features = await self._get_online_features_feast(
            feast_store=feast_store,
            feast_service_name=feature_list.versioned_name,
            feature_id_to_versioned_name=feature_id_to_versioned_name,
            point_in_time_value=point_in_time_value,
            request_data=request_data,
        )
        df_features.append(df_feast_online_features)
        logger.debug("Feast get_online_features took %f seconds", time.time() - tic)

        online_features_df = pd.concat(df_features, axis=1)
        online_features_df.rename(columns=feature_name_map, inplace=True)

        features = online_features_df.to_dict(orient="records")
        return OnlineFeaturesResponseModel(features=features)

    async def _get_online_features_feast(
        self,
        feast_store: FeastFeatureStore,
        feast_service_name: str,
        feature_id_to_versioned_name: Dict[PydanticObjectId, str],
        request_data: List[Dict[str, Any]],
        point_in_time_value: Optional[str],
    ) -> pd.DataFrame:
        """
        Perform additional handling on the request data:

        - Add point in time column if required
        - Composite entities are combined into a single column
        - Remove columns that are not required since feast is strict and will complain about them

        and call feast get_online_features(). The returned DataFrame consists of only the features
        (without the serving names).

        Parameters
        ----------
        feast_store: FeastFeatureStore
            FeastFeatureStore object
        feast_service_name: str
            Name of the feast feature service to use
        request_data: List[Dict[str, Any]]
            Request data with all the entities available
        feature_id_to_versioned_name: Dict[PydanticObjectId, str]
            Mapping from feature id to feature's versioned name
        point_in_time_value: Optional[str]
            Point in time value to use if the feature service requires point in time request column

        Returns
        -------
        DataFrame
        """
        # Include point in time column if it is required
        if point_in_time_value is not None:
            for row in request_data:
                row[SpecialColumnName.POINT_IN_TIME] = point_in_time_value

        # Get required serving names and composite serving names that need further processing
        offline_store_table_docs = (
            await self.offline_store_feature_table_service.list_documents_as_dict(
                query_filter={},
                projection={"serving_names": 1},
            )
        )
        composite_serving_names = set()
        for offline_store_table_doc in offline_store_table_docs["data"]:
            serving_names = tuple(offline_store_table_doc["serving_names"])
            if len(serving_names) > 1 and all(
                serving_name in request_data[0] for serving_name in serving_names
            ):
                composite_serving_names.add(serving_names)

        # Add concatenated composite serving names
        required_feast_entity_columns = {entity.name for entity in feast_store.list_entities()}
        added_column_names = []
        if composite_serving_names:
            for serving_names in composite_serving_names:
                combined_serving_names_col = get_combined_serving_names(list(serving_names))
                if combined_serving_names_col in required_feast_entity_columns:
                    for row in request_data:
                        row[combined_serving_names_col] = get_combined_serving_names_python([
                            row[serving_name] for serving_name in serving_names
                        ])
                    added_column_names.append(combined_serving_names_col)

        # Get exactly the columns that are required by feast
        needed_columns = list(required_feast_entity_columns) + added_column_names
        if point_in_time_value:
            needed_columns.append(SpecialColumnName.POINT_IN_TIME.value)

        updated_request_data = []
        for row in request_data:
            updated_request_data.append({k: v for (k, v) in row.items() if k in needed_columns})
        versioned_feature_names = [
            feature_id_to_versioned_name[feature_id]
            for feature_id in feature_id_to_versioned_name.keys()
        ]

        # FIXME: This is a temporary fix to avoid the bug in feast 0.40.0
        with patch(
            "feast.utils._augment_response_with_on_demand_transforms"
        ) as mock_augment_response_with_on_demand_transforms:
            mock_augment_response_with_on_demand_transforms.side_effect = (
                augment_response_with_on_demand_transforms
            )

            # FIXME: This is a temporary fix to performance issues due to original Feast implementation
            # make multiples table column copies during ODFV transformation
            with patch.object(
                OnDemandFeatureView,
                "transform_arrow",
                new=transform_arrow,
            ):
                # FIXME: This is a temporary fix to avoid O(N^2) complexity in with_projection method
                with patch.object(
                    BaseFeatureView,
                    "with_projection",
                    new=with_projection,
                ):
                    feature_service = feast_store.get_feature_service(feast_service_name)
                    df_feast_online_features = feast_store.get_online_features(
                        feature_service,
                        updated_request_data,
                    ).to_df()[versioned_feature_names]
                    return df_feast_online_features

    async def get_request_code_template(
        self,
        deployment: DeploymentModel,
        language: str,
    ) -> DeploymentRequestCodeTemplate:
        """
        Get request code template for a deployment

        Parameters
        ----------
        deployment: DeploymentModel
            Deployment model
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
        if deployment.serving_entity_ids is None:
            entity_serving_names = await self.feature_list_service.get_sample_entity_serving_names(
                feature_list_id=deployment.feature_list_id,
                count=1,
            )
        else:
            entity_serving_names = (
                await self.entity_serving_names_service.get_sample_entity_serving_names(
                    entity_ids=deployment.serving_entity_ids,
                    table_ids=None,
                    count=1,
                )
            )

        # construct serving url
        headers = {
            "Content-Type": "application/json",
            "active-catalog-id": str(deployment.catalog_id),
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
                header_params=" \\\n    ".join([
                    f"-H '{key}: {value}'" for key, value in headers.items()
                ]),
                serving_url=f"<FEATUREBYTE_SERVICE_URL>/deployment/{deployment.id}/online_features",
                entity_serving_names=json.dumps(entity_serving_names),
            ),
            entity_serving_names=entity_serving_names,
            language=language,
        )
