"""
PreviewService class
"""
from __future__ import annotations

from typing import Any, AsyncGenerator, cast

from decimal import Decimal

import pandas as pd

from featurebyte.enum import SpecialColumnName
from featurebyte.models.feature_store import FeatureStoreModel
from featurebyte.query_graph.enum import NodeType
from featurebyte.query_graph.node.generic import GroupbyNode
from featurebyte.query_graph.sql.feature_historical import get_historical_features
from featurebyte.query_graph.sql.feature_preview import get_feature_preview_sql
from featurebyte.query_graph.sql.interpreter import GraphInterpreter
from featurebyte.schema.feature import FeaturePreview
from featurebyte.schema.feature_list import FeatureListGetHistoricalFeatures, FeatureListPreview
from featurebyte.schema.feature_store import FeatureStorePreview
from featurebyte.service.mixin import OpsServiceMixin


class PreviewService(OpsServiceMixin):
    """
    PreviewService class
    """

    def __init__(self, user: Any, **kwargs: Any) -> None:  # pylint: disable=unused-argument
        self.user = user

    def _convert_dataframe_as_json(self, dataframe: pd.DataFrame) -> str:
        """
        Comvert pandas dataframe to json

        Parameters
        ----------
        dataframe: pd.DataFrame
            Dataframe object

        Returns
        -------
        str
            JSON string
        """
        dataframe.reset_index(drop=True, inplace=True)
        for name in dataframe.columns:
            # Decimal with integer values becomes float during conversion to json
            if (
                dataframe[name].dtype == object
                and isinstance(dataframe[name].iloc[0], Decimal)
                and (dataframe[name] % 1 == 0).all()
            ):
                dataframe[name] = dataframe[name].astype(int)
        return str(dataframe.to_json(orient="table", date_unit="ns", double_precision=15))

    async def preview(self, preview: FeatureStorePreview, limit: int, get_credential: Any) -> str:
        """
        Preview a QueryObject that is not a Feature (e.g. DatabaseTable, EventData, EventView, etc)

        Parameters
        ----------
        preview: FeatureStorePreview
            FeatureStorePreview object
        limit: int
            Row limit on preview results
        get_credential: Any
            Get credential handler function

        Returns
        -------
        str
            Dataframe converted to json string
        """
        feature_store_dict = preview.graph.get_input_node(
            preview.node_name
        ).parameters.feature_store_details.dict()
        feature_store = FeatureStoreModel(**feature_store_dict, name=preview.feature_store_name)
        db_session = await self._get_feature_store_session(
            feature_store=feature_store,
            get_credential=get_credential,
        )

        preview_sql = GraphInterpreter(
            preview.graph, source_type=feature_store.type
        ).construct_preview_sql(node_name=preview.node_name, num_rows=limit)
        result = await db_session.execute_query(preview_sql, timeout=180)
        return self._convert_dataframe_as_json(result)

    async def preview_feature(self, feature_preview: FeaturePreview, get_credential: Any) -> str:
        """
        Preview a Feature

        Parameters
        ----------
        feature_preview: FeaturePreview
            FeaturePreview object
        get_credential: Any
            Get credential handler function

        Returns
        -------
        str
            Dataframe converted to json string

        Raises
        ------
        KeyError
            Invalid point_in_time_and_serving_name payload
        """
        graph = feature_preview.graph
        feature_node = graph.get_node_by_name(feature_preview.node_name)
        point_in_time_and_serving_name = feature_preview.point_in_time_and_serving_name

        if SpecialColumnName.POINT_IN_TIME not in point_in_time_and_serving_name:
            raise KeyError(f"Point in time column not provided: {SpecialColumnName.POINT_IN_TIME}")

        serving_names = []
        for node in graph.iterate_nodes(target_node=feature_node, node_type=NodeType.GROUPBY):
            serving_names.extend(cast(GroupbyNode, node).parameters.serving_names)

        for col in sorted(set(serving_names)):
            if col not in point_in_time_and_serving_name:
                raise KeyError(f"Serving name not provided: {col}")

        feature_store_dict = graph.get_input_node(
            feature_preview.node_name
        ).parameters.feature_store_details.dict()
        feature_store = FeatureStoreModel(
            **feature_store_dict, name=feature_preview.feature_store_name
        )
        db_session = await self._get_feature_store_session(
            feature_store=feature_store,
            get_credential=get_credential,
        )
        preview_sql = get_feature_preview_sql(
            graph=graph,
            nodes=[feature_node],
            point_in_time_and_serving_name=feature_preview.point_in_time_and_serving_name,
            source_type=feature_store.type,
        )
        result = await db_session.execute_query(preview_sql, timeout=180)
        return self._convert_dataframe_as_json(result)

    async def preview_featurelist(
        self, featurelist_preview: FeatureListPreview, get_credential: Any
    ) -> str:
        """
        Preview a FeatureList

        Parameters
        ----------
        featurelist_preview: FeatureListPreview
            FeatureListPreview object
        get_credential: Any
            Get credential handler function

        Returns
        -------
        str
            Dataframe converted to json string

        Raises
        ------
        KeyError
            Invalid point_in_time_and_serving_name payload
        """
        point_in_time_and_serving_name = featurelist_preview.point_in_time_and_serving_name
        if SpecialColumnName.POINT_IN_TIME not in point_in_time_and_serving_name:
            raise KeyError(f"Point in time column not provided: {SpecialColumnName.POINT_IN_TIME}")

        result: pd.DataFrame = None
        group_join_keys = list(point_in_time_and_serving_name.keys())
        for feature_cluster in featurelist_preview.feature_clusters:
            feature_store_dict = feature_cluster.graph.get_input_node(
                feature_cluster.node_names[0]
            ).parameters.feature_store_details.dict()
            feature_store = FeatureStoreModel(
                **feature_store_dict, name=feature_cluster.feature_store_name
            )
            db_session = await self._get_feature_store_session(
                feature_store=feature_store,
                get_credential=get_credential,
            )
            preview_sql = get_feature_preview_sql(
                graph=feature_cluster.graph,
                nodes=feature_cluster.nodes,
                point_in_time_and_serving_name=point_in_time_and_serving_name,
                source_type=feature_store.type,
            )
            _result = await db_session.execute_query(preview_sql, timeout=180)
            if result is None:
                result = _result
            else:
                result = result.merge(_result, on=group_join_keys)

        return self._convert_dataframe_as_json(result)

    async def get_historical_features(
        self,
        training_events: pd.DataFrame,
        featurelist_get_historical_features: FeatureListGetHistoricalFeatures,
        get_credential: Any,
    ) -> AsyncGenerator[bytes, None]:
        """
        Get historical features for Feature List

        Parameters
        ----------
        training_events: pd.DataFrame
            Training events data
        featurelist_get_historical_features: FeatureListGetHistoricalFeatures
            FeatureListGetHistoricalFeatures object
        get_credential: Any
            Get credential handler function

        Returns
        -------
        AsyncGenerator[bytes, None]
            Asynchronous bytes generator
        """
        # multiple feature stores not supported
        feature_clusters = featurelist_get_historical_features.feature_clusters
        assert len(feature_clusters) == 1

        feature_cluster = feature_clusters[0]
        feature_store_dict = feature_cluster.graph.get_input_node(
            feature_cluster.node_names[0]
        ).parameters.feature_store_details.dict()
        db_session = await self._get_feature_store_session(
            feature_store=FeatureStoreModel(
                **feature_store_dict, name=feature_cluster.feature_store_name
            ),
            get_credential=get_credential,
        )

        return await get_historical_features(
            session=db_session,
            graph=feature_cluster.graph,
            nodes=feature_cluster.nodes,
            training_events=training_events,
            serving_names_mapping=featurelist_get_historical_features.serving_names_mapping,
        )
