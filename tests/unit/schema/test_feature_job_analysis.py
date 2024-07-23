"""Test schema for feature job analysis"""

from datetime import datetime

import pandas as pd
import pytest
from pydantic import BaseModel, ConfigDict

from featurebyte.schema.feature_job_setting_analysis import PandasTimestamp


class TestModel(BaseModel):
    """Test model for timestamp"""

    # add this to fix PytestCollectionWarning
    __test__ = False

    timestamp: PandasTimestamp

    # pydantic configuration
    model_config = ConfigDict(arbitrary_types_allowed=True)


@pytest.mark.parametrize(
    "timestamp",
    [
        "2021-01-01 00:00:00",
        datetime(2021, 1, 1, 0, 0, 0),
        pd.Timestamp("2021-01-01 00:00:00"),
    ],
)
def test_pandas_timestamp(timestamp):
    """Test pandas timestamp"""
    model = TestModel(timestamp=timestamp)
    assert model.timestamp == pd.Timestamp("2021-01-01 00:00:00")
