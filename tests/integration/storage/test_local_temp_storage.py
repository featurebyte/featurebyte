"""
Test LocalStorage class
"""
import tempfile
from unittest.mock import patch

import pytest

from featurebyte.storage import LocalTempStorage
from tests.integration.storage.base import BaseStorageTestSuite


class TestLocalStorageSuite(BaseStorageTestSuite):
    """
    Test suite for LocalStorage class
    """

    @pytest.fixture(name="test_storage")
    def storage_fixture(self):
        """
        Storage object fixture
        """
        with tempfile.TemporaryDirectory() as tempdir:
            with patch("tempfile.gettempdir") as mock_gettempdir:
                mock_gettempdir.return_value = tempdir
                yield LocalTempStorage()
