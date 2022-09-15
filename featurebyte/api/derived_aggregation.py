"""
Implementation of derived aggregations (complex aggregations that combine multiple Features)
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Dict, List, Optional, cast

from featurebyte.api.feature import Feature
from featurebyte.api.feature_list import BaseFeatureGroup, FeatureGroup
from featurebyte.enum import AggFunc

if TYPE_CHECKING:
    from featurebyte.api.groupby import EventViewGroupBy


def std_aggregation(
    groupby_obj: EventViewGroupBy,
    value_column: Optional[str],
    windows: Optional[List[str]],
    feature_names: Optional[List[str]],
    timestamp_column: Optional[str],
    feature_job_setting: Optional[Dict[str, str]],
) -> FeatureGroup:
    """Computes standard deviation aggregation by combining two average features

    Using the formula:
        Stddev(x) = sqrt(E[x^2] - E[x]^2)

    This allows tiles to be shared with other features when possible, especially the tiles required
    to compute E[x] (i.e. Avg(x))

    Parameters
    ----------
    groupby_obj: EventViewGroupBy
        EventViewGroupBy object that called this function
    value_column: str
        Column to be aggregated
    windows: List[str]
        List of aggregation window sizes
    feature_names: List[str]
        Output feature names
    timestamp_column: Optional[str]
        Timestamp column used to specify the window (if not specified, event data timestamp is used)
    feature_job_setting: Optional[Dict[str, str]]
        Dictionary contains `blind_spot`, `frequency` and `time_modulo_frequency` keys which are
        feature job setting parameters

    Returns
    -------
    FeatureGroup

    Raises
    ------
    ValueError
        if category parameter was specified in groupby
    """
    # pylint: disable=too-many-locals
    assert value_column is not None
    assert feature_names is not None

    if groupby_obj.category is not None:
        raise ValueError("category parameter is not supported for std aggregation method")

    temp_view = groupby_obj.obj.copy()
    temp_view["_value_squared"] = temp_view[value_column] * temp_view[value_column]  # type: ignore[operator]
    temp_view_grouped = temp_view.groupby(groupby_obj.keys, groupby_obj.category)
    temp_feature_names = [f"_value_squared_avg_{i}" for i in range(len(feature_names))]
    expected_x2_features = temp_view_grouped.aggregate(
        "_value_squared",
        method=AggFunc.AVG,
        windows=windows,
        feature_names=temp_feature_names,
        timestamp_column=timestamp_column,
        feature_job_setting=feature_job_setting,
    )
    temp_feature_names = [f"_value_avg_{i}" for i in range(len(feature_names))]
    expected_x_features = temp_view_grouped.aggregate(
        value_column,
        method=AggFunc.AVG,
        windows=windows,
        feature_names=temp_feature_names,
        timestamp_column=timestamp_column,
        feature_job_setting=feature_job_setting,
    )

    features: list[Feature | BaseFeatureGroup] = []
    for name_x2, name_x, feature_name in zip(
        expected_x2_features.feature_names,
        expected_x_features.feature_names,
        feature_names,
    ):
        feature = (
            expected_x2_features[name_x2]  # type: ignore[operator]
            - (expected_x_features[name_x] * expected_x_features[name_x])  # type: ignore[operator]
        ).sqrt()
        feature.name = feature_name
        feature = cast(Feature, feature)
        features.append(feature)

    feature_group = FeatureGroup(items=features)
    return feature_group
