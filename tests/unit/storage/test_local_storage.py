"""
Test LocalStorage class
"""
import tempfile

import pytest

from featurebyte.storage import LocalStorage
from tests.unit.storage.base import BaseStorageTestSuite


class TestLocalStorageSuite(BaseStorageTestSuite):
    @pytest.fixture(name="storage")
    def storage_fixture(self):
        """
        Storage object fixture
        """
        with tempfile.TemporaryDirectory() as tempdir:
            yield LocalStorage(base_path=tempdir)
