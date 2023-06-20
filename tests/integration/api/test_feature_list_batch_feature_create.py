"""
Test feature list & feature group creation with batch feature creation
"""
import pandas as pd
import pytest

from featurebyte.api.feature_list import FeatureList
from featurebyte.exception import RecordCreationException


@pytest.fixture(name="feature_group")
def fixture_feature_group(event_table):
    """Feature group fixture"""
    event_view = event_table.get_view()
    feature_group = event_view.groupby("ÜSER ID").aggregate_over(
        method="count",
        windows=["2h", "24h"],
        feature_names=["COUNT_2h", "COUNT_24h"],
    )
    return feature_group


@pytest.mark.parametrize("source_type", ["snowflake"], indirect=True)
def test_feature_group__saving_with_conflict_resolution(feature_group, source_type):
    """Test feature group saving"""
    _ = source_type

    # list feature list before saving feature group
    feature_list_before = FeatureList.list(include_id=True)

    # save feature group
    feature_group.save()
    feature_id = feature_group["COUNT_2h"].id

    # modify existing feature & save again
    feature_group["COUNT_2h"] = feature_group["COUNT_2h"] + 1
    new_feature_id = feature_group["COUNT_2h"].id
    assert new_feature_id != feature_id

    # feature group creation failure requires mongo transaction rollback,
    # this requires actual mongo connection (do not move this test to unit test).
    with pytest.raises(RecordCreationException) as exc:
        feature_group.save()
    expected_msg = (
        'FeatureNamespace (name: "COUNT_2h") already exists. '
        'Please rename object (name: "COUNT_2h") to something else.'
    )
    assert expected_msg in str(exc.value)
    assert feature_group["COUNT_2h"].id == new_feature_id

    # save the feature group with retrieve conflict resolution strategy
    # check that the COUNT_2h feature is reverted to the original state
    feature_group.save(conflict_resolution="retrieve")
    assert feature_group["COUNT_2h"].id == feature_id

    # check feature list after saving feature group, make sure no new feature list is created
    feature_list_after = FeatureList.list(include_id=True)
    pd.testing.assert_frame_equal(feature_list_after, feature_list_before)
