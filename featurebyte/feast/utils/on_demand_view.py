"""
On demand feature view related classes and functions.
"""

from typing import Any, Dict, List, Optional, Union, cast
from unittest.mock import patch

from feast import FeatureView, Field, RequestSource
from feast.feature_view_projection import FeatureViewProjection
from feast.on_demand_feature_view import OnDemandFeatureView, on_demand_feature_view

from featurebyte.enum import DBVarType, SpecialColumnName
from featurebyte.feast.enum import to_feast_primitive_type
from featurebyte.models.feature import FeatureModel


def create_feast_on_demand_feature_view(
    definition: str,
    function_name: str,
    sources: List[Union[FeatureView, RequestSource, FeatureViewProjection]],
    schema: List[Field],
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

    Returns
    -------
    OnDemandFeatureView
        The created OnDemandFeatureView
    """
    # get the function from the definition
    locals_namespace: Dict[str, Any] = {}
    exec(definition, locals_namespace)  # nosec
    func = locals_namespace[function_name]

    # dill.source.getsource fails to find the source code of the function
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
        ttl_seconds: Optional[float] = None
        if offline_store_info.is_decomposed:
            for (
                ingest_query_graph
            ) in offline_store_info.extract_offline_store_ingest_query_graphs():
                fv_source = name_to_feast_feature_view[ingest_query_graph.offline_store_table_name]
                sources.append(fv_source)
                if fv_source.ttl is not None:
                    if ttl_seconds is None:
                        ttl_seconds = fv_source.ttl.total_seconds()
                    elif fv_source.ttl.total_seconds() < ttl_seconds:
                        ttl_seconds = fv_source.ttl.total_seconds()

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
            sources.append(fv_source)

        if not has_point_in_time:
            # add point in time request source if not already present
            req_source = name_to_feast_request_source[SpecialColumnName.POINT_IN_TIME.value]
            sources.append(req_source)

        assert offline_store_info.odfv_info is not None, "OfflineStoreInfo does not have ODFV info"
        feature_view = create_feast_on_demand_feature_view(
            definition=offline_store_info.odfv_info.codes,
            function_name=offline_store_info.odfv_info.function_name,
            sources=sources,
            schema=[
                Field(
                    name=feature_model.versioned_name,
                    dtype=to_feast_primitive_type(DBVarType(feature_model.dtype)),
                )
            ],
        )
        return feature_view
