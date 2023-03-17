import pandas
import pytest

import featurebyte


@pytest.fixture(autouse=True)
def add_imports(doctest_namespace):
    doctest_namespace["fb"] = featurebyte
    doctest_namespace["pd"] = pandas


@pytest.fixture(autouse=True)
def activate_playground_catalog():
    featurebyte.Catalog.activate("grocery")
