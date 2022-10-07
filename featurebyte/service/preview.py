"""
PreviewService class
"""
from __future__ import annotations

from typing import Any

from decimal import Decimal

import pandas as pd

from featurebyte.enum import SpecialColumnName
from featurebyte.models.feature_store import FeatureStoreModel
from featurebyte.query_graph.node.generic import GroupbyNode, InputNode
from featurebyte.query_graph.sql.feature_preview import get_feature_preview_sql
from featurebyte.query_graph.sql.interpreter import GraphInterpreter
from featurebyte.schema.feature import FeaturePreview
from featurebyte.schema.feature_list import FeatureListPreview
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
        input_node = preview.graph.nodes_map["input_1"]
        assert isinstance(input_node, InputNode)
        feature_store_dict = input_node.parameters.feature_store_details.dict()
        db_session = await self._get_feature_store_session(
            feature_store=FeatureStoreModel(**feature_store_dict, name=preview.feature_store_name),
            get_credential=get_credential,
        )

        preview_sql = GraphInterpreter(preview.graph).construct_preview_sql(
            node_name=preview.node.name, num_rows=limit
        )
        result = await db_session.execute_async_query(preview_sql)
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
        graph = feature_preview.feature.graph
        point_in_time_and_serving_name = feature_preview.point_in_time_and_serving_name

        if SpecialColumnName.POINT_IN_TIME not in point_in_time_and_serving_name:
            raise KeyError(f"Point in time column not provided: {SpecialColumnName.POINT_IN_TIME}")

        inception_node = graph.get_node_by_name(feature_preview.feature.row_index_lineage[0])
        assert isinstance(inception_node, GroupbyNode)
        serving_names = inception_node.parameters.serving_names
        if serving_names is not None:
            for col in serving_names:
                if col not in point_in_time_and_serving_name:
                    raise KeyError(f"Serving name not provided: {col}")

        input_node = graph.nodes_map["input_1"]
        assert isinstance(input_node, InputNode)
        feature_store_dict = input_node.parameters.feature_store_details.dict()
        db_session = await self._get_feature_store_session(
            feature_store=FeatureStoreModel(
                **feature_store_dict, name=feature_preview.feature_store_name
            ),
            get_credential=get_credential,
        )
        preview_sql = get_feature_preview_sql(
            graph=graph,
            nodes=[feature_preview.feature.node],
            point_in_time_and_serving_name=feature_preview.point_in_time_and_serving_name,
        )
        result = await db_session.execute_async_query(preview_sql)
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
        for preview_group in featurelist_preview.preview_groups:
            input_node = preview_group.graph.nodes_map["input_1"]
            assert isinstance(input_node, InputNode)
            feature_store_dict = input_node.parameters.feature_store_details.dict()
            db_session = await self._get_feature_store_session(
                feature_store=FeatureStoreModel(
                    **feature_store_dict, name=preview_group.feature_store_name
                ),
                get_credential=get_credential,
            )
            preview_sql = get_feature_preview_sql(
                graph=preview_group.graph,
                nodes=[
                    preview_group.graph.get_node_by_name(name) for name in preview_group.node_names
                ],
                point_in_time_and_serving_name=point_in_time_and_serving_name,
            )
            _result = await db_session.execute_async_query(preview_sql)
            if result is None:
                result = _result
            else:
                result = result.merge(_result, on=group_join_keys)

        return self._convert_dataframe_as_json(result)
