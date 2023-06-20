"""
Fixtures for use in notebook tests.
"""
import pathlib

import pytest

NOTEBOOKS_PATH = pathlib.Path("./notebooks")


@pytest.fixture(params=list(NOTEBOOKS_PATH.glob("quick*.ipynb")))
def quick_start_notebooks(request):
    """
    Fixture for the path to the notebooks.
    """
    return request.param


@pytest.fixture(params=list(NOTEBOOKS_PATH.glob("deep*.ipynb")))
def deep_dive_notebooks(request):
    """
    Fixture for the path to the notebooks.
    """
    return request.param


@pytest.fixture(params=list(NOTEBOOKS_PATH.glob("playground*.ipynb")))
def playground_notebooks(request):
    """
    Fixture for the path to the notebooks.
    """
    return request.param
