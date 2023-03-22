"""
Pytest configuration file for doctest
"""
import pandas
import pytest

import featurebyte


@pytest.fixture(autouse=True)
def add_imports(doctest_namespace):
    """
    Add default imports to doctest namespace
    """
    doctest_namespace["fb"] = featurebyte
    doctest_namespace["pd"] = pandas


@pytest.fixture(autouse=True)
def activate_playground_catalog():
    """
    Activate the playground catalog automatically
    """
    featurebyte.Catalog.activate("grocery")
