"""
On demand feature view related classes and functions.
"""
from typing import Dict, List, Optional, Union, cast

import importlib
import os
from unittest.mock import patch

from bson import ObjectId
from feast import FeatureView, Field, RequestSource
from feast.feature_view_projection import FeatureViewProjection
from feast.on_demand_feature_view import OnDemandFeatureView, on_demand_feature_view

from featurebyte.common.string import sanitize_identifier
from featurebyte.enum import DBVarType, SpecialColumnName
from featurebyte.feast.enum import to_feast_primitive_type
from featurebyte.models.feature import FeatureModel


def create_feast_on_demand_feature_view(
    definition: str,
    function_name: str,
    sources: List[Union[FeatureView, RequestSource, FeatureViewProjection]],
    schema: List[Field],
    on_demand_feature_view_dir: str,
) -> OnDemandFeatureView:
    """
    Create a Feast OnDemandFeatureView from a function definition.

    Parameters
    ----------
    definition: str
        Definition of the function to be used to create the OnDemandFeatureView
    function_name: str
        Name of the function to be used to create the OnDemandFeatureView
    sources: List[Union[FeatureView, RequestSource]]
        List of FeatureViews and RequestSources to be used as sources for the OnDemandFeatureView
    schema: List[Field]
        List of Fields to be used as schema for the OnDemandFeatureView
    on_demand_feature_view_dir: str
        Directory to write the on demand feature view code to

    Returns
    -------
    OnDemandFeatureView
        The created OnDemandFeatureView
    """
    module_name = f"on_demand_feature_view_{ObjectId()}"
    with open(
        os.path.join(on_demand_feature_view_dir, f"{module_name}.py"), "w", encoding="utf-8"
    ) as file_handle:
        file_handle.write(definition)

    module = importlib.import_module(module_name)
    func = getattr(module, function_name)
    try:
        output = on_demand_feature_view(sources=sources, schema=schema)(func)
    except IOError:
        # dill.source.getsource fails to find the source code of the function in some environments
        # since we already have the source code, we can just use that instead
        with patch("dill.source.getsource", return_value=definition):
            output = on_demand_feature_view(sources=sources, schema=schema)(func)

    return cast(OnDemandFeatureView, output)


class OnDemandFeatureViewConstructor:
    """
    Class to construct OnDemandFeatureView from a feature's offlineStoreInfo.
    """

    @classmethod
    def create(
        cls,
        feature_model: FeatureModel,
        name_to_feast_feature_view: Dict[str, FeatureView],
        name_to_feast_request_source: Dict[str, RequestSource],
        on_demand_feature_view_dir: str,
    ) -> OnDemandFeatureView:
        """
        Create a Feast OnDemandFeatureView from an offline store info.

        Parameters
        ----------
        feature_model: FeatureModel
            FeatureModel to be used to create the OnDemandFeatureView
        name_to_feast_feature_view: Dict[str, FeatureView]
            Dict of FeatureView names to FeatureViews to be used as sources for the OnDemandFeatureView
        name_to_feast_request_source: Dict[str, RequestSource]
            Dict of RequestSource names to RequestSources to be used as sources for the OnDemandFeatureView
        on_demand_feature_view_dir: str
            Directory to write the on demand feature view code to

        Returns
        -------
        OnDemandFeatureView
            The created OnDemandFeatureView or None if the offline store info does not result in an
            OnDemandFeatureView
        """
        offline_store_info = feature_model.offline_store_info
        assert feature_model.name is not None, "FeatureModel does not have a name"

        sources: List[Union[FeatureView, RequestSource, FeatureViewProjection]] = []
        has_point_in_time = False
        ttl_seconds: Optional[int] = None
        if offline_store_info.is_decomposed:
            for (
                ingest_query_graph
            ) in offline_store_info.extract_offline_store_ingest_query_graphs():
                fv_source = name_to_feast_feature_view[ingest_query_graph.offline_store_table_name]
                sources.append(fv_source)
                if fv_source.ttl is not None:
                    if ttl_seconds is None:
                        ttl_seconds = fv_source.ttl.seconds
                    elif fv_source.ttl.seconds < ttl_seconds:
                        ttl_seconds = fv_source.ttl.seconds

            for request_node in feature_model.extract_request_column_nodes():
                req_source = name_to_feast_request_source[request_node.parameters.column_name]
                sources.append(req_source)
                if request_node.parameters.column_name == SpecialColumnName.POINT_IN_TIME.value:
                    has_point_in_time = True
        else:
            assert (
                offline_store_info.metadata is not None
            ), "OfflineStoreInfo does not have metadata"
            assert offline_store_info.metadata.has_ttl, "FeatureModel does not have TTL"
            fv_source = name_to_feast_feature_view[
                offline_store_info.metadata.offline_store_table_name
            ]
            if fv_source.ttl is not None:
                ttl_seconds = fv_source.ttl.seconds
            sources.append(fv_source)

        if not has_point_in_time and ttl_seconds:
            # add point in time request source if the feature has TTL
            req_source = name_to_feast_request_source[SpecialColumnName.POINT_IN_TIME.value]
            sources.append(req_source)

        function_name = (
            f"compute_feature_{sanitize_identifier(feature_model.name)}_{feature_model.id}"
        )
        codes = offline_store_info.generate_on_demand_feature_view_code(
            feature_name_version=feature_model.name_version,
            input_df_name="inputs",
            output_df_name="df",
            function_name=function_name,
            ttl_seconds=ttl_seconds,
        )
        feature_view = create_feast_on_demand_feature_view(
            definition=codes,
            function_name=function_name,
            sources=sources,
            schema=[
                Field(
                    name=feature_model.name_version,
                    dtype=to_feast_primitive_type(DBVarType(feature_model.dtype)),
                )
            ],
            on_demand_feature_view_dir=on_demand_feature_view_dir,
        )
        return feature_view
