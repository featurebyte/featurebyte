"""
Test feature & feature list version related logic
"""
import pytest

from featurebyte.api.event_view import EventView
from featurebyte.api.feature_list import FeatureList, FeatureVersionInfo
from featurebyte.common.model_util import get_version
from featurebyte.models.event_data import FeatureJobSetting
from featurebyte.models.feature_list import FeatureListNewVersionMode


@pytest.fixture(name="feature_group")
def feature_group_fixture(
    snowflake_feature_store,
    snowflake_event_data_with_entity,
    mock_insert_feature_registry,
):
    """
    Feature group fixture
    """
    snowflake_feature_store.save()
    snowflake_event_data_with_entity.save()

    event_view = EventView.from_event_data(snowflake_event_data_with_entity)
    feature_group = event_view.groupby("cust_id").aggregate(
        value_column="col_float",
        method="sum",
        windows=["30m", "2h", "4h"],
        feature_names=["amt_sum_30m", "amt_sum_2h", "amt_sum_4h"],
        feature_job_setting={
            "blind_spot": "10m",
            "frequency": "30m",
            "time_modulo_frequency": "5m",
        },
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
    amt_sum_30m = feature_list["amt_sum_30m"]
    assert amt_sum_30m.feature_namespace.default_feature_id == amt_sum_30m.id
    amt_sum_30m_v1 = amt_sum_30m.create_new_version(
        feature_job_setting=FeatureJobSetting(
            blind_spot="75m", frequency="30m", time_modulo_frequency="15m"
        )
    )
    assert amt_sum_30m_v1.version.to_str() == f"{get_version()}_1"
    assert amt_sum_30m.feature_namespace.default_feature_id == amt_sum_30m_v1.id

    # create a new feature list version (auto)
    feature_list_v1 = feature_list.create_new_version(FeatureListNewVersionMode.AUTO)
    assert set(feature_list_v1.feature_ids) == {
        amt_sum_30m_v1.id,
        feature_group["amt_sum_2h"].id,
        feature_group["amt_sum_4h"].id,
    }
    assert feature_list_v1.version.to_str() == f"{get_version()}_1"
    assert feature_list.feature_list_namespace.default_feature_list_id == feature_list_v1.id

    # create a new feature list version (manual)
    feature_list_v2 = feature_list.create_new_version(
        "manual",
        features=[
            FeatureVersionInfo(name=amt_sum_30m_v1.name, version=amt_sum_30m_v1.version.to_str())
        ],
    )
    assert set(feature_list_v2.feature_ids) == set(feature_list_v1.feature_ids)
    assert feature_list_v2.version.to_str() == f"{get_version()}_2"
    assert feature_list.feature_list_namespace.default_feature_list_id == feature_list_v2.id
    assert feature_list.is_default is False
    assert feature_list_v2.is_default is True

    # create a new feature list version (semi-auto)
    amt_sum_2h = feature_group["amt_sum_2h"]
    feature_list_v3 = feature_list.create_new_version(
        "semi_auto",
        features=[FeatureVersionInfo(name=amt_sum_2h.name, version=amt_sum_2h.version)],
    )
    assert set(feature_list_v3.feature_ids) == set(feature_list_v2.feature_ids)
    assert feature_list_v3.version.to_str() == f"{get_version()}_3"
    assert feature_list.feature_list_namespace.default_feature_list_id == feature_list_v3.id
    assert feature_list.is_default is False
    assert feature_list_v2.is_default is False
    assert feature_list_v3.is_default is True
    assert set(feat.id for feat in feature_list_v3.feature_objects.values()) == set(
        feature_list_v3.feature_ids
    )
    assert len(feature_list.items) == len(feature_list.feature_objects)

    # check feature list ids in feature list namespace
    assert set(feature_list.feature_list_namespace.feature_list_ids) == {
        feature_list.id,
        feature_list_v1.id,
        feature_list_v2.id,
        feature_list_v3.id,
    }
