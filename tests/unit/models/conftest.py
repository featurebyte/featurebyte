"""
Common fixtures used in tests/unit/models directory
"""
import pytest

from featurebyte.enum import SourceType
from featurebyte.models.feature_store import FeatureStoreModel, SnowflakeDetails


@pytest.fixture(name="snowflake_feature_store_model")
def snowflake_feature_store_model_fixture():
    """Fixture for a Snowflake source"""
    snowflake_details = SnowflakeDetails(
        account="account",
        warehouse="warehouse",
        database="database",
        sf_schema="schema",
    )
    snowflake_source = FeatureStoreModel(
        name="sf_featurestore", type=SourceType.SNOWFLAKE, details=snowflake_details
    )
    return snowflake_source
