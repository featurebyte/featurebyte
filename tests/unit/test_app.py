"""
Test FastAPI app
"""
from unittest.mock import patch

from bson.objectid import ObjectId

from featurebyte.app import get_credential
from featurebyte.config import Configurations


def test_get_credential():
    """
    Test get_credential works as expected
    """
    config = Configurations("tests/fixtures/sample_config.yaml")
    db_source = list(config.credentials.keys())[0]

    with patch("featurebyte.app.Configurations") as mock_config:
        mock_config.return_value = config
        credential = get_credential(user_id=ObjectId(), db_source=db_source)
    assert credential == config.credentials[db_source]
