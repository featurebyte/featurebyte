"""
Test catalog module
"""
from inspect import signature
from unittest.mock import patch

from featurebyte import (
    Data,
    DimensionData,
    Entity,
    EventData,
    Feature,
    FeatureJobSettingAnalysis,
    FeatureStore,
    ItemData,
    PeriodicTask,
    Relationship,
    SlowlyChangingData,
    Workspace,
)
from featurebyte.api.catalog import Catalog
from featurebyte.api.feature import FeatureNamespace
from featurebyte.api.feature_list import FeatureList, FeatureListNamespace


def test_list_methods_have_same_parameters_as_delegated_list_method_call():
    """
    Test catalog list methods have same parameters as underlying methods.

    This will help to ensure that the Catalog list APIs are consistent with their API object List methods.
    """
    method_pairs = [
        (Catalog.list_features, Feature),
        (Catalog.list_feature_namespaces, FeatureNamespace),
        (Catalog.list_feature_list_namespaces, FeatureListNamespace),
        (Catalog.list_feature_lists, FeatureList),
        (Catalog.list_data, Data),
        (Catalog.list_dimension_data, DimensionData),
        (Catalog.list_item_data, ItemData),
        (Catalog.list_event_data, EventData),
        (Catalog.list_scd_data, SlowlyChangingData),
        (Catalog.list_relationships, Relationship),
        (Catalog.list_feature_job_setting_analysis, FeatureJobSettingAnalysis),
        (Catalog.list_workspaces, Workspace),
        (Catalog.list_feature_stores, FeatureStore),
        (Catalog.list_entities, Entity),
        (Catalog.list_periodic_tasks, PeriodicTask),
    ]
    for catalog_list_method, underlying_class in method_pairs:
        # Check that the signatures match
        catalog_list_method_signature = signature(catalog_list_method)
        underlying_class_list_method_signature = signature(underlying_class.list)
        assert (
            catalog_list_method_signature.parameters.keys()
            == underlying_class_list_method_signature.parameters.keys()
        ), f"catalog method: {catalog_list_method}, underlying_class {underlying_class}"

        # Assert that the delegated list method is called
        with patch.object(underlying_class, "list") as mocked_list:
            catalog_list_method()
            mocked_list.assert_called()
