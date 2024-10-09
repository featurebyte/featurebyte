"""
Tests for historical features
"""

import threading
from queue import Queue

import pandas as pd
import pytest
from sqlglot import parse_one

import featurebyte as fb
from featurebyte.query_graph.sql.common import sql_to_string
from tests.util.helper import create_observation_table_from_dataframe


@pytest.mark.asyncio
async def test_get_historical_feature_tables_parallel(
    session, event_view, data_source, feature_table_cache_metadata_service
):
    """
    Test get historical feature tables in parallel on the same observation table
    """
    # Create feature lists
    num_features = 4
    num_hashes_expected = 0
    features_mapping = {}
    for i in range(num_features):
        event_view["derived_value_column"] = (10.0 + i) * event_view["ÀMOUNT"]
        if i < 2:
            # Duplicate features with different names but the same definition hash
            kwargs = {"method": "count", "value_column": None}
            if i == 0:
                num_hashes_expected += 1
        else:
            kwargs = {"method": "sum", "value_column": "derived_value_column"}
            num_hashes_expected += 1
        feature_name = f"my_feature_{i}"
        feature = event_view.groupby("ÜSER ID").aggregate_over(
            windows=["24h"],
            feature_names=[feature_name],
            **kwargs,
        )[feature_name]
        features_mapping[i] = fb.FeatureList([feature], name=feature_name)

    # Create observation table
    df_observation_set = pd.DataFrame({
        "POINT_IN_TIME": pd.to_datetime(["2001-01-02 10:00:00", "2001-01-02 12:00:00"] * 5),
        "üser id": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
    })
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

    # Check feature cache metadata and table are expected
    cached_definitions = await feature_table_cache_metadata_service.get_cached_definitions(
        observation_table_id=observation_table.id,
    )
    assert len(cached_definitions) == num_hashes_expected
    cached_tables = list({cached_def.table_name for cached_def in cached_definitions})
    assert len(cached_tables) == 1
    query = sql_to_string(
        parse_one(
            f"""
            SELECT * FROM "{session.database_name}"."{session.schema_name}"."{cached_tables[0]}"
            """
        ),
        source_type=session.source_type,
    )
    df = await session.execute_query(query)
    expected_columns = {feature_def.feature_name for feature_def in cached_definitions}
    assert expected_columns.issubset(df.columns)
