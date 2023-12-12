"""
Helper function for feature materialization
"""
from typing import Any, List, Optional

import copy
from datetime import datetime
from unittest.mock import patch

from feast import FeatureStore, FeatureView, utils
from tqdm import tqdm

DEFAULT_MATERIALIZE_START_DATE = datetime(1970, 1, 1)


def _filter_by_name(obj_list: List[Any], columns: List[str]) -> List[Any]:
    """
    Filter a list of objects by the name attribute

    Parameters
    ----------
    obj_list : List[Any]
        List of objects to filter. Each object must have a name attribute.
    columns : List[str]
        List of names to filter by

    Returns
    -------
    List[Any]
    """
    return [obj for obj in obj_list if obj.name in columns]


def materialize_partial(
    feature_store: FeatureStore,
    feature_view: FeatureView,
    columns: List[str],
    end_date: datetime,
    start_date: Optional[datetime] = None,
) -> None:
    """
    Materialize a FeatureView partially for only the selected columns

    Parameters
    ----------
    feature_store : FeatureStore
        FeatureStore object
    feature_view : FeatureView
        FeatureView to materialize
    columns : List[str]
        List of column names to materialize
    end_date : datetime
        End date of materialization
    start_date : Optional[datetime]
        Start date of materialization
    """
    if start_date is None:
        start_date = DEFAULT_MATERIALIZE_START_DATE

    start_date = utils.make_tzaware(start_date)
    end_date = utils.make_tzaware(end_date)

    assert start_date < end_date

    provider = feature_store._get_provider()

    def silent_tqdm_builder(length):
        return tqdm(total=length, ncols=100, disable=True)

    partial_feature_view = copy.deepcopy(feature_view)
    partial_feature_view.features = _filter_by_name(feature_view.features, columns)
    partial_feature_view.projection.features = _filter_by_name(
        feature_view.projection.features, columns
    )

    with patch("google.protobuf.timestamp_pb2.Timestamp.ParseFromString"):
        provider.materialize_single_feature_view(
            config=feature_store.config,
            feature_view=partial_feature_view,
            start_date=start_date,
            end_date=end_date,
            registry=feature_store._registry,
            project=feature_store.project,
            tqdm_builder=silent_tqdm_builder,
        )
