import pathlib
import subprocess

import pytest

NOTEBOOKS_PATH = pathlib.Path("./notebooks")


def notebook_runner(notebook_path):
    jupyter_cmd = f"jupyter nbconvert --to notebook --execute --ExecutePreprocessor.kernel_name=python3 --output /tmp/output.ipynb './{notebook_path}'"
    return subprocess.run(jupyter_cmd, shell=True)


def idfn(param):
    """Return the name of the notebook being tested as the id"""
    if isinstance(param, pathlib.PosixPath):
        return str(param).split("/")[1]


@pytest.mark.parametrize(
    "quick_start_notebooks", list(NOTEBOOKS_PATH.glob("quick*.ipynb")), ids=idfn
)
def test_quick_start_notebooks(quick_start_notebooks):
    result = notebook_runner(quick_start_notebooks)
    assert result.returncode == 0


@pytest.mark.parametrize("deep_dive_notebooks", list(NOTEBOOKS_PATH.glob("deep*.ipynb")), ids=idfn)
def test_deep_dive_notebooks(deep_dive_notebooks):
    result = notebook_runner(deep_dive_notebooks)
    assert result.returncode == 0


@pytest.mark.parametrize(
    "playground_notebooks", list(NOTEBOOKS_PATH.glob("playground*.ipynb")), ids=idfn
)
def test_playground_notebooks(playground_notebooks):
    result = notebook_runner(playground_notebooks)
    assert result.returncode == 0
