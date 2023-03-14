"""
OnlineServingService class
"""
from __future__ import annotations

from typing import Any, Dict, List

import time

import pandas as pd
from bson import ObjectId

from featurebyte.common.utils import prepare_dataframe_for_json
from featurebyte.exception import FeatureListNotOnlineEnabledError
from featurebyte.logger import logger
from featurebyte.models.feature_list import FeatureListModel
from featurebyte.persistent import Persistent
from featurebyte.query_graph.sql.dataframe import construct_dataframe_sql_expr
from featurebyte.query_graph.sql.online_serving import get_online_store_retrieval_sql
from featurebyte.schema.feature_list import OnlineFeaturesResponseModel
from featurebyte.service.base_service import BaseService
from featurebyte.service.entity_validation import EntityValidationService
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.service.session_manager import SessionManagerService


class OnlineServingService(BaseService):
    """
    OnlineServingService is responsible for retrieving features from online store
    """

    def __init__(
        self,
        user: Any,
        persistent: Persistent,
        catalog_id: ObjectId,
        session_manager_service: SessionManagerService,
        entity_validation_service: EntityValidationService,
    ):
        super().__init__(user, persistent, catalog_id)
        self.feature_store_service = FeatureStoreService(
            user=user, persistent=persistent, catalog_id=catalog_id
        )
        self.session_manager_service = session_manager_service
        self.entity_validation_service = entity_validation_service

    async def get_online_features_from_feature_list(
        self,
        feature_list: FeatureListModel,
        entity_serving_names: List[Dict[str, Any]],
        get_credential: Any,
    ) -> OnlineFeaturesResponseModel:
        """
        Get online features for a Feature List given a list of entity serving names

        Parameters
        ----------
        feature_list: FeatureListModel
            Feature List
        entity_serving_names: List[Dict[str, Any]]
            Entity serving names
        get_credential: Any
            Get credential handler

        Returns
        -------
        OnlineFeaturesResponseModel

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

        tic = time.time()
        feature_cluster = feature_list.feature_clusters[0]

        feature_store = await self.feature_store_service.get_document(
            document_id=feature_cluster.feature_store_id
        )
        parent_serving_preparation = (
            await self.entity_validation_service.validate_entities_or_prepare_for_parent_serving(
                graph=feature_cluster.graph,
                nodes=feature_cluster.nodes,
                request_column_names=set(entity_serving_names[0].keys()),
                feature_store=feature_store,
            )
        )

        df_request_table = pd.DataFrame(entity_serving_names)
        df_expr = construct_dataframe_sql_expr(df_request_table, date_cols=[])

        retrieval_sql = get_online_store_retrieval_sql(
            feature_cluster.graph,
            feature_cluster.nodes,
            source_type=feature_store.type,
            request_table_columns=df_request_table.columns.tolist(),
            request_table_expr=df_expr,
            parent_serving_preparation=parent_serving_preparation,
        )
        logger.debug(f"OnlineServingService sql prep elapsed: {time.time() - tic:.6f}s")

        tic = time.time()
        session = await self.session_manager_service.get_feature_store_session(
            feature_store, get_credential
        )
        logger.debug(f"OnlineServingService get session elapsed: {time.time() - tic:.6f}s")
        df_features = await session.execute_query(retrieval_sql)
        assert df_features is not None

        features = []
        prepare_dataframe_for_json(df_features)
        for _, row in df_features.iterrows():
            features.append(row.to_dict())
        result = OnlineFeaturesResponseModel(features=features)

        return result
