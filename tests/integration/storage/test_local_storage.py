"""
Test LocalStorage class
"""
import tempfile

import pytest

from featurebyte.storage import LocalStorage
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
            yield LocalStorage(base_path=tempdir)
