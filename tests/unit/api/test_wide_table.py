"""Test wide table to check performance of operations involving many columns."""

import time

from featurebyte import EventTable


def test_wide_table_performance(
    snowflake_event_table_5k_columns, snowflake_event_table, mock_api_object_cache
):
    """Test performance of operations involving a wide table with 5000 columns."""

    start_time = time.time()
    _ = EventTable.get(snowflake_event_table_5k_columns.name)
    load_time_wide = time.time() - start_time
    print(f"Loading wide table took {load_time_wide:.5f} seconds")

    # Test accessing all columns
    start_time = time.time()
    _ = EventTable.get(snowflake_event_table.name)
    load_time_normal = time.time() - start_time
    print(f"Accessing wide table took {load_time_normal:.5f} seconds")

    ratio = load_time_wide / load_time_normal if load_time_normal > 0 else float("inf")
    print(f"Wide table load time is {ratio:.2f} times that of normal")
