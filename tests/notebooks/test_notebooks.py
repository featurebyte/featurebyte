import subprocess


def notebook_runner(notebook_path):
    jupyter_cmd = f"jupyter nbconvert --to notebook --execute --ExecutePreprocessor.kernel_name=python3 --output /tmp/output.ipynb './{notebook_path}'"
    return subprocess.run(jupyter_cmd, shell=True)


def test_quick_start_notebooks(quick_start_notebooks):
    result = notebook_runner(quick_start_notebooks)
    assert result.returncode == 0


def test_deep_dive_notebooks(deep_dive_notebooks):
    result = notebook_runner(deep_dive_notebooks)
    assert result.returncode == 0


def test_playground_notebooks(playground_notebooks):
    result = notebook_runner(playground_notebooks)
    assert result.returncode == 0
