"""
Tests for historical features
"""

import threading
from queue import Queue

import pandas as pd
import pytest

import featurebyte as fb
from tests.util.helper import create_observation_table_from_dataframe


@pytest.mark.asyncio
async def test_get_historical_feature_tables_parallel(session, event_view, data_source):
    """
    Test get historical feature tables in parallel on the same observation table
    """
    # Create feature lists
    num_features = 3
    features_mapping = {}
    for i in range(num_features):
        # TODO: create mix of same features and different features
        feature_name = f"my_feature_{i}"
        feature = event_view.groupby("ÜSER ID").aggregate_over(
            method="count",
            windows=["24h"],
            feature_names=[feature_name],
        )[feature_name]
        features_mapping[i] = fb.FeatureList([feature], name=feature_name)

    # Create observation table
    df_observation_set = pd.DataFrame(
        {
            "POINT_IN_TIME": pd.to_datetime(["2001-01-02 10:00:00", "2001-01-02 12:00:00"] * 5),
            "üser id": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        }
    )
    observation_table = await create_observation_table_from_dataframe(
        session,
        df_observation_set,
        data_source,
    )

    def run_get_historical_features_table(index, out):
        """
        Run compute_historical_feature_table
        """
        try:
            feature_list = features_mapping[i]
            feature_table_name = f"my_feature_table_{index}"
            feature_list.compute_historical_feature_table(observation_table, feature_table_name)
        except Exception as e:  # pylint: disable=broad-exception-caught
            out.put(e)

    # Create feature tables in parallel
    out = Queue()
    threads = []
    for i in range(num_features):
        t = threading.Thread(target=run_get_historical_features_table, args=(i, out))
        threads.append(t)
        t.start()
    for t in threads:
        t.join()
    if not out.empty():
        raise out.get()

    # Check that the feature tables were created correctly
    for i in range(num_features):
        feature_table_name = f"my_feature_table_{i}"
        feature_table = fb.HistoricalFeatureTable.get(feature_table_name)
        df_preview = feature_table.preview()
        assert df_preview.columns.tolist() == ["POINT_IN_TIME", "üser id", f"my_feature_{i}"]
