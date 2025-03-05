"""
Test forward_aggregate_asat
"""

import numpy as np
import pandas as pd

from tests.util.helper import tz_localize_if_needed


def test_forward_aggregate_asat(scd_table, scd_dataframe, source_type):
    """
    Test using forward_aggregate_asat to create Target on SCDView
    """
    scd_view = scd_table.get_view()
    target = scd_view.groupby("User Status").forward_aggregate_asat(
        value_column=None,
        method="count",
        target_name="Future Number of Users With This Status",
        offset="10d",
        fill_value=None,
    )

    # check preview but provides children id
    df = target.preview(
        pd.DataFrame([
            {
                "POINT_IN_TIME": "2001-10-15 10:00:00",
                "üser id": 1,
            }
        ])
    )
    # databricks return POINT_IN_TIME with "Etc/UTC" timezone
    tz_localize_if_needed(df, source_type)
    expected = {
        "POINT_IN_TIME": pd.Timestamp("2001-10-15 10:00:00"),
        "üser id": 1,
        "Future Number of Users With This Status": 1,
    }
    assert df.iloc[0].to_dict() == expected

    # check historical features
    target.save()
    observations_set = pd.DataFrame({
        "POINT_IN_TIME": pd.date_range("2001-01-01 10:00:00", periods=10, freq="1d"),
        "user_status": ["STÀTUS_CODE_47"] * 10,
    })
    expected = observations_set.copy()
    expected["Future Number of Users With This Status"] = [
        1,
        2,
        2,
        1,
        1,
        np.nan,
        np.nan,
        np.nan,
        np.nan,
        np.nan,
    ]
    df = target.compute_targets(observations_set)
    # databricks return POINT_IN_TIME with "Etc/UTC" timezone
    tz_localize_if_needed(df, source_type)
    pd.testing.assert_frame_equal(df, expected, check_dtype=False)
