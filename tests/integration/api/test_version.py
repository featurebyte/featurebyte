"""
Test feature & feature list version related logic
"""
import pytest

from featurebyte.api.event_view import EventView
from featurebyte.api.feature_list import FeatureList, FeatureVersionInfo
from featurebyte.common.model_util import get_version
from featurebyte.models.event_data import FeatureJobSetting


@pytest.fixture(name="feature_group")
def feature_group_fixture(event_data):
    """
    Feature group fixture
    """
    event_view = EventView.from_event_data(event_data)
    feature_group = event_view.groupby("USER ID").aggregate(
        value_column="AMOUNT",
        method="sum",
        windows=["30m", "2h", "4h"],
        feature_names=["amount_sum_30m", "amount_sum_2h", "amount_sum_4h"],
    )
    return feature_group


def test_feature_and_feature_list_version(feature_group):
    """
    Test feature & feature list version logic
    """
    # create a feature list & save
    feature_list = FeatureList([feature_group], name="my_special_fl")
    feature_list.save()
    assert feature_list.saved is True
    assert feature_list.version.to_str() == get_version()
    assert feature_list.feature_list_namespace.default_feature_list_id == feature_list.id

    # create a new feature version
    amt_sum_30m = feature_list["amount_sum_30m"]
    assert amt_sum_30m.feature_namespace.default_feature_id == amt_sum_30m.id
    amt_sum_30m_v1 = amt_sum_30m.create_new_version(
        feature_job_setting=FeatureJobSetting(
            blind_spot="75m", frequency="30m", time_modulo_frequency="15m"
        )
    )
    assert amt_sum_30m_v1.version.to_str() == f"{get_version()}_1"
    assert amt_sum_30m.feature_namespace.default_feature_id == amt_sum_30m_v1.id

    # create a new feature list version (auto)
    feature_list_v1 = feature_list.create_new_version("auto")
    assert set(feature_list_v1.feature_ids) == {
        amt_sum_30m_v1.id,
        feature_group["amount_sum_2h"].id,
        feature_group["amount_sum_4h"].id,
    }
    assert feature_list_v1.version.to_str() == f"{get_version()}_1"
    assert feature_list.feature_list_namespace.default_feature_list_id == feature_list_v1.id

    # create a new feature list version (manual)
    feature_list_v2 = feature_list.create_new_version(
        "manual",
        features=[FeatureVersionInfo(name=amt_sum_30m_v1.name, version=amt_sum_30m_v1.version)],
    )
    assert set(feature_list_v2.feature_ids) == set(feature_list_v1.feature_ids)
    assert feature_list_v2.version.to_str() == f"{get_version()}_2"
    assert feature_list.feature_list_namespace.default_feature_list_id == feature_list_v2.id

    # create a new feature list version (semi-auto)
    amt_sum_2h = feature_group["amount_sum_2h"]
    feature_list_v3 = feature_list.create_new_version(
        "semi_auto",
        features=[FeatureVersionInfo(name=amt_sum_2h.name, version=amt_sum_2h.version)],
    )
    assert set(feature_list_v3.feature_ids) == set(feature_list_v2.feature_ids)
    assert feature_list_v3.version.to_str() == f"{get_version()}_3"
    assert feature_list.feature_list_namespace.default_feature_list_id == feature_list_v3.id

    # check feature list ids in feature list namespace
    assert set(feature_list.feature_list_namespace.feature_list_ids) == {
        feature_list.id,
        feature_list_v1.id,
        feature_list_v2.id,
        feature_list_v3.id,
    }
