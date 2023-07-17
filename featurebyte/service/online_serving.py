"""
OnlineServingService class
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Union

import pandas as pd

from featurebyte.exception import FeatureListNotOnlineEnabledError
from featurebyte.models.batch_request_table import BatchRequestTableModel
from featurebyte.models.feature_list import FeatureListModel
from featurebyte.query_graph.node.schema import TableDetails
from featurebyte.query_graph.sql.online_serving import get_online_features
from featurebyte.schema.deployment import OnlineFeaturesResponseModel
from featurebyte.service.entity_validation import EntityValidationService
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.service.online_store_table_version import OnlineStoreTableVersionService
from featurebyte.service.session_manager import SessionManagerService


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
    ):
        self.feature_store_service = feature_store_service
        self.session_manager_service = session_manager_service
        self.entity_validation_service = entity_validation_service
        self.online_store_table_version_service = online_store_table_version_service

    async def get_online_features_from_feature_list(
        self,
        feature_list: FeatureListModel,
        request_data: Union[List[Dict[str, Any]], BatchRequestTableModel],
        get_credential: Any,
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
        get_credential: Any
            Get credential handler
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
            get_credential=get_credential,
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
