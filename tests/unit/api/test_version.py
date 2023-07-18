"""
Test feature & feature list version related logic
"""
import pytest

from featurebyte.api.feature_list import FeatureList
from featurebyte.common.model_util import get_version
from featurebyte.exception import RecordUpdateException
from featurebyte.models.feature_namespace import DefaultVersionMode
from featurebyte.query_graph.model.feature_job_setting import (
    FeatureJobSetting,
    TableFeatureJobSetting,
)
from featurebyte.schema.feature_list import FeatureVersionInfo


@pytest.fixture(name="feature_group")
def feature_group_fixture(
    snowflake_event_table_with_entity,
):
    """
    Feature group fixture
    """
    event_view = snowflake_event_table_with_entity.get_view()
    feature_group = event_view.groupby("cust_id").aggregate_over(
        value_column="col_float",
        method="sum",
        windows=["30m", "2h", "4h"],
        feature_names=["amt_sum_30m", "amt_sum_2h", "amt_sum_4h"],
        feature_job_setting=FeatureJobSetting(
            blind_spot="10m",
            frequency="30m",
            time_modulo_frequency="5m",
        ),
    )
    return feature_group


def test_feature_and_feature_list_version(feature_group, mock_api_object_cache):
    """
    Test feature & feature list version logic
    """
    _ = mock_api_object_cache

    # create a feature list & save
    feature_list = FeatureList([feature_group], name="my_special_fl")
    feature_list.save()
    assert feature_list.saved is True
    assert feature_list.version == get_version()
    assert feature_list.feature_list_namespace.default_feature_list_id == feature_list.id

    # check default feature fraction
    assert feature_list.info()["default_feature_fraction"] == {"this": 1.0, "default": 1.0}

    # create a new feature version
    amt_sum_30m = feature_list["amt_sum_30m"]
    assert amt_sum_30m.feature_namespace.default_feature_id == amt_sum_30m.id
    amt_sum_30m_v1 = amt_sum_30m.create_new_version(
        table_feature_job_settings=[
            TableFeatureJobSetting(
                table_name="sf_event_table",
                feature_job_setting=FeatureJobSetting(
                    blind_spot="75m", frequency="30m", time_modulo_frequency="15m"
                ),
            )
        ],
        table_cleaning_operations=None,
    )
    assert amt_sum_30m_v1.version == f"{get_version()}_1"
    assert amt_sum_30m.feature_namespace.default_feature_id == amt_sum_30m_v1.id

    # create a new feature list version without specifying features
    feature_list_v1 = feature_list.create_new_version()
    assert set(feature_list_v1.feature_ids) == {
        amt_sum_30m_v1.id,
        feature_group["amt_sum_2h"].id,
        feature_group["amt_sum_4h"].id,
    }
    assert feature_list_v1.version == f"{get_version()}_1"
    assert feature_list.feature_list_namespace.default_feature_list_id == feature_list_v1.id

    # check default feature fraction
    assert feature_list.info()["default_feature_fraction"] == {"this": 2 / 3, "default": 1.0}
    assert feature_list_v1.info()["default_feature_fraction"] == {"this": 1.0, "default": 1.0}
    assert feature_list.default_feature_fraction == 2 / 3
    assert feature_list_v1.default_feature_fraction == 1.0

    # create a new feature list version by specifying features
    feature_list_v2 = feature_list.create_new_version(
        features=[FeatureVersionInfo(name=amt_sum_30m_v1.name, version=amt_sum_30m_v1.version)],
    )
    assert set(feature_list_v2.feature_ids) == set(feature_list_v1.feature_ids)
    assert feature_list_v2.version == f"{get_version()}_2"
    assert feature_list.feature_list_namespace.default_feature_list_id == feature_list_v2.id
    assert feature_list.is_default is False
    assert feature_list_v2.is_default is True

    # check default feature fraction
    assert feature_list_v1.feature_ids == feature_list_v2.feature_ids
    assert feature_list.info()["default_feature_fraction"] == {"this": 2 / 3, "default": 1.0}
    assert feature_list_v1.info()["default_feature_fraction"] == {"this": 1.0, "default": 1.0}
    assert feature_list_v2.info()["default_feature_fraction"] == {"this": 1.0, "default": 1.0}
    assert feature_list.default_feature_fraction == 2 / 3
    assert feature_list_v1.default_feature_fraction == 1.0
    assert feature_list_v2.default_feature_fraction == 1.0

    # check feature list ids in feature list namespace
    assert set(feature_list.feature_list_namespace.feature_list_ids) == {
        feature_list.id,
        feature_list_v1.id,
        feature_list_v2.id,
    }


def test_feature_list__as_default_version(feature_group):
    """Test feature list as_default_version method"""
    feature_list = FeatureList([feature_group], name="my_special_fl")
    feature_list.save()

    # check default feature fraction
    assert feature_list.info()["default_feature_fraction"] == {"this": 1.0, "default": 1.0}

    # create new feature version
    feature_list["amt_sum_30m"].create_new_version(
        table_feature_job_settings=[
            TableFeatureJobSetting(
                table_name="sf_event_table",
                feature_job_setting=FeatureJobSetting(
                    blind_spot="75m", frequency="30m", time_modulo_frequency="15m"
                ),
            )
        ],
        table_cleaning_operations=None,
    )

    # create new feature list version
    new_feature_list_version = feature_list.create_new_version()
    assert new_feature_list_version.is_default is True

    # check default feature fraction
    assert feature_list.info()["default_feature_fraction"] == {"this": 2 / 3, "default": 1.0}
    assert new_feature_list_version.info()["default_feature_fraction"] == {
        "this": 1.0,
        "default": 1.0,
    }
    assert feature_list.default_feature_fraction == 2 / 3
    assert new_feature_list_version.default_feature_fraction == 1.0

    # check setting default version fails when default version mode is not MANUAL
    with pytest.raises(RecordUpdateException) as exc:
        feature_list.as_default_version()
    expected = "Cannot set default feature list ID when default version mode is not MANUAL."
    assert expected in str(exc.value)

    # check get by name use the default version
    assert FeatureList.get(name=feature_list.name) == new_feature_list_version

    # check setting default version manually
    assert new_feature_list_version.is_default is True
    assert feature_list.is_default is False
    feature_list.update_default_version_mode(DefaultVersionMode.MANUAL)
    feature_list.as_default_version()
    assert new_feature_list_version.is_default is False
    assert feature_list.is_default is True

    # check get by name use the default version
    assert FeatureList.get(name=feature_list.name) == feature_list

    # check get by name and version
    assert (
        FeatureList.get(
            name=feature_list.name,
            version=new_feature_list_version.version,
        )
        == new_feature_list_version
    )


def test_create_new_version_on_item_view_aggregate_over_feature(
    snowflake_item_table, transaction_entity
):
    """This is a test for creating a new feature version on an item view feature"""
    snowflake_item_table.event_id_col.as_entity(transaction_entity.name)
    item_view = snowflake_item_table.get_view(event_suffix="_event_table")
    feat = item_view.groupby("cust_id_event_table").aggregate_over(
        value_column="item_amount",
        method="sum",
        feature_names=["amt_sum_30m"],
        windows=["30m"],
    )["amt_sum_30m"]
    feat.save()

    # test create new version with timestamp column on the right table of a join
    new_feat = feat.create_new_version(
        table_feature_job_settings=[
            TableFeatureJobSetting(
                table_name="sf_event_table",
                feature_job_setting=FeatureJobSetting(
                    blind_spot="75m", frequency="30m", time_modulo_frequency="15m"
                ),
            )
        ],
    )
    setting = new_feat.info()["table_feature_job_setting"]["this"]
    assert setting == [
        {
            "table_name": "sf_event_table",
            "feature_job_setting": {
                "blind_spot": "4500s",
                "frequency": "1800s",
                "time_modulo_frequency": "900s",
            },
        }
    ]
