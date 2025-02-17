"""
Tests for historical features
"""

import threading
from queue import Queue
from unittest.mock import patch

import pandas as pd
import pytest
from sqlglot import expressions, parse_one

import featurebyte as fb
from featurebyte.query_graph.sql.common import sql_to_string
from featurebyte.query_graph.sql.feature_compute import FeatureExecutionPlan
from tests.integration.api.test_event_view_operations import get_training_events_and_expected_result
from tests.util.helper import create_observation_table_from_dataframe, fb_assert_frame_equal


@pytest.mark.asyncio
async def test_get_historical_feature_tables_parallel(
    session, event_view, data_source, feature_table_cache_metadata_service
):
    """
    Test get historical feature tables in parallel on the same observation table
    """
    # Create feature lists
    num_features = 4
    features_mapping = {}

    common_feature = event_view.groupby("ÜSER ID").aggregate_over(
        value_column=None,
        method="count",
        windows=["24h"],
        feature_names=["common_feature"],
    )["common_feature"]
    num_hashes_expected = 1

    # Create feature list with overlapping features
    for i in range(num_features):
        event_view["derived_value_column"] = (10.0 + i) * event_view["ÀMOUNT"]
        kwargs = {"method": "sum", "value_column": "derived_value_column"}
        num_hashes_expected += 1
        feature_name = f"my_feature_{i}"
        feature = event_view.groupby("ÜSER ID").aggregate_over(
            windows=["24h"],
            feature_names=[feature_name],
            **kwargs,
        )[feature_name]
        features_mapping[i] = fb.FeatureList([common_feature, feature], name=feature_name)

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
        assert df_preview.columns.tolist() == [
            "POINT_IN_TIME",
            "üser id",
            "common_feature",
            f"my_feature_{i}",
        ]

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


def test_historical_feature_query_dynamic_batching(feature_list_with_combined_feature_groups):
    """
    Test historical feature query dynamic batching
    """
    original_construct_combined_sql = FeatureExecutionPlan.construct_combined_sql

    num_patched_queries = {"count": 0}

    def patched_construct_combined_sql(*args, **kwargs):
        """
        Patch construct_combined_sql to deliberately cause error when there are more than 6 columns
        in the select statement. This is to test dynamic batching of historical feature queries.
        """
        select_expr = original_construct_combined_sql(*args, **kwargs)
        if kwargs.get("exclude_post_aggregation", False):
            # Online serving related query, skip
            return select_expr
        if len(select_expr.expressions) > 6:
            select_expr = select_expr.select(
                expressions.alias_(
                    expressions.Anonymous(this="FAIL_NOW"), alias="_debug_col", quoted=True
                )
            )
            num_patched_queries["count"] += 1
        return select_expr

    df_training_events, df_historical_expected = get_training_events_and_expected_result()

    with patch.object(
        FeatureExecutionPlan,
        "construct_combined_sql",
        new=patched_construct_combined_sql,
    ):
        df_historical_features = (
            feature_list_with_combined_feature_groups.compute_historical_features(
                df_training_events,
            )
        )

    # Check that the patched function was called
    assert num_patched_queries["count"] > 0

    fb_assert_frame_equal(
        df_historical_features,
        df_historical_expected,
        dict_like_columns=["COUNT_BY_ACTION_24h"],
    )
