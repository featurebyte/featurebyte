import pytest

import featurebyte


@pytest.fixture(autouse=True)
def add_fb(doctest_namespace):
    doctest_namespace["fb"] = featurebyte


@pytest.fixture(autouse=True)
def activate_playground_catalog():
    featurebyte.Catalog.activate("quick start feature engineering 20230317:2005")
