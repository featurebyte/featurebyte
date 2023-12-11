"""
On demand feature view related classes and functions.
"""
from typing import Dict, List, Optional, Union

import importlib
import sys
import tempfile

from feast import FeatureView, Field, RequestSource
from feast.on_demand_feature_view import OnDemandFeatureView, on_demand_feature_view

from featurebyte.models.offline_store_ingest_query import OfflineStoreInfo


def create_feast_on_demand_feature_view(
    definition: str,
    function_name: str,
    sources: List[Union[FeatureView, RequestSource]],
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
    with tempfile.TemporaryDirectory() as repo_dir:
        module_name = "on_demand_feature_view"
        with open(f"{repo_dir}/{module_name}.py", "w", encoding="utf-8") as file_handle:
            file_handle.write(definition)

        # add repo_dir to sys.path so that we can import the module
        sys.path.append(repo_dir)
        module = importlib.import_module(module_name)
        func = getattr(module, function_name)
        return on_demand_feature_view(sources=sources, schema=schema)(func)


class OnDemandFeatureViewConstructor:
    """
    Class to construct OnDemandFeatureView from a feature's offlineStoreInfo.
    """

    @classmethod
    def create_or_none(
        cls,
        offline_store_info: OfflineStoreInfo,
        name_to_feast_feature_view: Dict[str, FeatureView],
        name_to_feast_request_source: Dict[str, RequestSource],
    ) -> Optional[OnDemandFeatureView]:
        """
        Create a Feast OnDemandFeatureView from an offline store info.

        Parameters
        ----------
        offline_store_info: OfflineStoreInfo
            Feature offlineStoreInfo to be used as offline store for the OnDemandFeatureView
        name_to_feast_feature_view: Dict[str, FeatureView]
            Dict of FeatureView names to FeatureViews to be used as sources for the OnDemandFeatureView
        name_to_feast_request_source: Dict[str, RequestSource]
            Dict of RequestSource names to RequestSources to be used as sources for the OnDemandFeatureView

        Returns
        -------
        Optional[OnDemandFeatureView]
            The created OnDemandFeatureView or None if the offline store info does not result in an
            OnDemandFeatureView
        """
        _ = name_to_feast_feature_view, name_to_feast_request_source
        if not offline_store_info.is_decomposed:
            return None

        output = offline_store_info.extract_on_demand_feature_view_code_generation(
            input_df_name="input_df",
            output_df_name="output_df",
            function_name="on_demand_feature_view_func",
        )
        codes = output.generate_code()
        import pdb

        pdb.set_trace()
