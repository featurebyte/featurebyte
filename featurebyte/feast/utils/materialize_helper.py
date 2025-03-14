"""
Helper function for feature materialization
"""

from __future__ import annotations

import copy
from datetime import datetime
from typing import Any, List, Optional
from unittest.mock import patch

from feast import FeatureStore, FeatureView, utils
from tqdm import tqdm

from featurebyte.enum import InternalName
from featurebyte.exception import FeatureMaterializationError
from featurebyte.session.base import (
    LONG_RUNNING_EXECUTE_QUERY_TIMEOUT_SECONDS,
    BaseSession,
    to_thread,
)

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


async def materialize_partial(
    session: BaseSession,
    feature_store: FeatureStore,
    feature_view: FeatureView,
    columns: List[str],
    end_date: datetime,
    start_date: Optional[datetime] = None,
    with_feature_timestamp: bool = False,
) -> None:
    """
    Materialize a FeatureView partially for only the selected columns

    Parameters
    ----------
    session : BaseSession
        Session object
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
    with_feature_timestamp : bool
        Whether to include the feature timestamp in the materialization

    Raises
    ------
    FeatureMaterializationError
        If materialization fails for any reason
    """
    if start_date is None:
        start_date = DEFAULT_MATERIALIZE_START_DATE

    if with_feature_timestamp and InternalName.FEATURE_TIMESTAMP_COLUMN not in columns:
        columns = list(columns) + [InternalName.FEATURE_TIMESTAMP_COLUMN.value]

    start_date = utils.make_tzaware(start_date)
    end_date = utils.make_tzaware(end_date)

    assert start_date < end_date

    provider = feature_store._get_provider()

    def silent_tqdm_builder(length: int) -> tqdm:
        return tqdm(total=length, ncols=100, disable=True)

    partial_feature_view = copy.deepcopy(feature_view)
    partial_feature_view.features = _filter_by_name(feature_view.features, columns)
    partial_feature_view.projection.features = _filter_by_name(
        feature_view.projection.features, columns
    )

    # FIXME: This patch will reuse the Snowflake connection from the session object for
    # FEAST online store materialization
    if feature_store.config.offline_store.type == "snowflake.offline":
        snowflake_session_cache = {feature_store.config.offline_store.type: session.connection}
    else:
        snowflake_session_cache = {}

    # FIXME: This patch is related to an implementation detail of RedisOnlineStore: it checks
    # whether the feature table's stored feature timestamp is the same as current feature timestamp,
    # and if so skip the update. This doesn't work for partial materialization because a feature
    # table's stored feature timestamp is always up-to-date except on the first materialization run.
    # Patching this effectively skips the check, but a better solution might be to override the
    # implementation of RedisOnlineStore.online_write_batch().
    with (
        patch("google.protobuf.timestamp_pb2.Timestamp.ParseFromString"),
        patch("feast.infra.utils.snowflake.snowflake_utils._cache", snowflake_session_cache),
    ):
        try:
            await to_thread(
                provider.materialize_single_feature_view,
                LONG_RUNNING_EXECUTE_QUERY_TIMEOUT_SECONDS,
                None,
                config=feature_store.config,
                feature_view=partial_feature_view,
                start_date=start_date,
                end_date=end_date,
                registry=feature_store._registry,
                project=feature_store.project,
                tqdm_builder=silent_tqdm_builder,
            )
        except Exception as exc:
            # add more context to the exception
            raise FeatureMaterializationError(
                f"Failed to materialize {partial_feature_view.name}: {partial_feature_view.features}"
            ) from exc
