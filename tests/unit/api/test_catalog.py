"""
Test catalog module
"""
from inspect import signature

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


def test_list_methods_have_same_parameters():
    """
    Test catalog list methods have same parameters as underlying methods.

    This will help to ensure that the Catalog list APIs are consistent with their API object List methods.
    """
    method_pairs = [
        (Catalog.list_features, Feature.list),
        (Catalog.list_feature_namespaces, FeatureNamespace.list),
        (Catalog.list_feature_list_namespaces, FeatureListNamespace.list),
        (Catalog.list_feature_lists, FeatureList.list),
        (Catalog.list_data, Data.list),
        (Catalog.list_dimension_data, DimensionData.list),
        (Catalog.list_item_data, ItemData.list),
        (Catalog.list_event_data, EventData.list),
        (Catalog.list_scd_data, SlowlyChangingData.list),
        (Catalog.list_relationships, Relationship.list),
        (Catalog.list_feature_job_setting_analysis, FeatureJobSettingAnalysis.list),
        (Catalog.list_workspaces, Workspace.list),
        (Catalog.list_feature_stores, FeatureStore.list),
        (Catalog.list_entities, Entity.list),
        (Catalog.list_periodic_tasks, PeriodicTask.list),
    ]
    for method, method_2 in method_pairs:
        sig = signature(method)
        sig_2 = signature(method_2)
        assert (
            sig.parameters.keys() == sig_2.parameters.keys()
        ), f"method: {method}, method_2 {method_2}"
