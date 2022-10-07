"""
Tests functions in common/env_util.py module
"""

from unittest.mock import patch

from featurebyte.common.env_util import get_alive_bar_additional_params


@patch("featurebyte.common.env_util.is_notebook")
def test_get_alive_bar_additional_params(mock_is_notebook):
    """Test get_alive_bar_additional_params function"""
    mock_is_notebook.return_value = True
    assert get_alive_bar_additional_params() == {"force_tty": True}

    mock_is_notebook.return_value = False
    assert get_alive_bar_additional_params() == {"dual_line": True}
