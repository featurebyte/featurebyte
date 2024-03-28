"""
Pytest configuration file for doctest
"""

import pandas
import pytest
from alive_progress import config_handler

import featurebyte


@pytest.fixture(autouse=True, scope="session")
def init_conftest():
    """
    Removes all alive_progress bars from the console for test-docs
    """
    config_handler.set_global(disable=True)


@pytest.fixture(autouse=True)
def add_imports(doctest_namespace):
    """
    Add default imports to doctest namespace
    """
    doctest_namespace["fb"] = featurebyte
    doctest_namespace["pd"] = pandas

    # catalog
    doctest_namespace["catalog"] = featurebyte.Catalog.get("grocery")

    # get entity id
    grocery_customer_entity = featurebyte.Entity.get("grocerycustomer")
    doctest_namespace["grocery_customer_entity_id"] = grocery_customer_entity.id

    # get feature id
    feature = featurebyte.Feature.get("InvoiceCount_60days")
    doctest_namespace["invoice_count_60_days_feature_id"] = feature.id


@pytest.fixture(autouse=True)
def pandas_options():
    """
    Set pandas options
    """
    pandas.set_option("display.max_rows", 500)
    pandas.set_option("display.max_columns", 500)
    pandas.set_option("display.width", 1000)


@pytest.fixture(autouse=True)
def activate_playground_catalog():
    """
    Activate the playground catalog automatically
    """
    featurebyte.Catalog.activate("grocery")
