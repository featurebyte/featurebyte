"""
Test catalog module
"""
from typing import Any, Callable, Optional

from dataclasses import dataclass
from inspect import signature
from unittest.mock import patch

import pytest

from featurebyte.api.api_object import ApiObject, SavableApiObject
from featurebyte.api.base_data import DataApiObject, DataListMixin
from featurebyte.api.catalog import Catalog
from featurebyte.api.data import Data
from featurebyte.api.dimension_data import DimensionData
from featurebyte.api.entity import Entity
from featurebyte.api.event_data import EventData
from featurebyte.api.feature import Feature, FeatureNamespace
from featurebyte.api.feature_job import FeatureJobMixin
from featurebyte.api.feature_job_setting_analysis import FeatureJobSettingAnalysis
from featurebyte.api.feature_list import FeatureList, FeatureListNamespace
from featurebyte.api.feature_store import FeatureStore
from featurebyte.api.item_data import ItemData
from featurebyte.api.periodic_task import PeriodicTask
from featurebyte.api.relationship import Relationship
from featurebyte.api.scd_data import SlowlyChangingData
from featurebyte.api.workspace import Workspace


@dataclass
class ListMethodMetadata:
    """
    Data class to keep track of list methods to test.
    """

    # Catalog method to test
    catalog_method: Callable
    # API object which has the list method we delegate to
    class_object: Any
    # List method override if we don't delegate to a `list` method.
    list_method_override: Optional[str] = None
    # List method to patch
    list_method_to_patch: Optional[str] = None


def get_methods_to_test():
    return [
        ListMethodMetadata(Catalog.list_features, Feature, "_list_versions", "list_versions"),
        ListMethodMetadata(Catalog.list_feature_namespaces, FeatureNamespace),
        ListMethodMetadata(Catalog.list_feature_list_namespaces, FeatureListNamespace),
        ListMethodMetadata(
            Catalog.list_feature_lists, FeatureList, "_list_versions", "list_versions"
        ),
        ListMethodMetadata(Catalog.list_tables, Data),
        ListMethodMetadata(Catalog.list_dimension_tables, DimensionData),
        ListMethodMetadata(Catalog.list_item_tables, ItemData),
        ListMethodMetadata(Catalog.list_event_tables, EventData),
        ListMethodMetadata(Catalog.list_scd_tables, SlowlyChangingData),
        ListMethodMetadata(Catalog.list_relationships, Relationship),
        ListMethodMetadata(Catalog.list_feature_job_setting_analyses, FeatureJobSettingAnalysis),
        ListMethodMetadata(Catalog.list_workspaces, Workspace),
        ListMethodMetadata(Catalog.list_feature_stores, FeatureStore),
        ListMethodMetadata(Catalog.list_entities, Entity),
        ListMethodMetadata(Catalog.list_periodic_tasks, PeriodicTask),
    ]


@pytest.fixture(name="methods_to_test")
def methods_to_test_fixture():
    return get_methods_to_test()


def _inheritors(class_obj):
    """
    Helper method to find all children of a class.
    """
    subclasses = set()
    work = [class_obj]
    while work:
        parent = work.pop()
        for child in parent.__subclasses__():
            if child not in subclasses:
                subclasses.add(child)
                work.append(child)
    return subclasses


def test_all_list_methods_are_exposed_in_catalog(methods_to_test):
    """
    Test that all inherited list methods are exposed in catalog.

    This will help to ensure that new API objects that have a list method are added to the Catalog.
    If we don't want to add them, we can add them to the excluded_children set.
    """
    api_object_children = _inheritors(ApiObject)
    excluded_children = {SavableApiObject, DataListMixin, FeatureJobMixin, DataApiObject}
    assert len(api_object_children) == len(methods_to_test) + len(excluded_children)

    for method_item in methods_to_test:
        delegated_class = method_item.class_object
        assert delegated_class in api_object_children


@pytest.mark.parametrize(
    "method_item",
    get_methods_to_test(),
)
def test_list_methods_have_same_parameters_as_delegated_list_method_call(method_item):
    """
    Test catalog list methods have same parameters as underlying methods.

    This will help to ensure that the Catalog list APIs are consistent with their API object List methods.
    """
    catalog_list_method, underlying_class = method_item.catalog_method, method_item.class_object
    # Check that the signatures match
    catalog_list_method_signature = signature(catalog_list_method)
    underlying_class_method = underlying_class.list
    if method_item.list_method_override:
        underlying_class_method = getattr(underlying_class, method_item.list_method_override)
    underlying_class_list_method_signature = signature(underlying_class_method)
    assert (
        catalog_list_method_signature.parameters.keys()
        == underlying_class_list_method_signature.parameters.keys()
    ), f"catalog method: {catalog_list_method}, underlying_class {underlying_class}"


@pytest.mark.parametrize(
    "method_item",
    get_methods_to_test(),
)
def test_list_methods_call_the_correct_delegated_method(method_item):
    """
    Test catalog list methods call the correct delegated method.
    """
    # Assert that the delegated list method is called
    method_name = method_item.list_method_to_patch or "list"
    with patch.object(method_item.class_object, method_name) as mocked_list:
        method_item.catalog_method()
        mocked_list.assert_called()
