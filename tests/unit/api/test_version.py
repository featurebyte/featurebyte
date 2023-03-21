"""
Test feature & feature list version related logic
"""
import pytest

from featurebyte import DefaultVersionMode, MissingValueImputation
from featurebyte.api.feature_list import FeatureList
from featurebyte.common.model_util import get_version
from featurebyte.exception import RecordUpdateException
from featurebyte.models.feature import FeatureReadiness
from featurebyte.models.feature_list import FeatureListNewVersionMode
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
    snowflake_event_table_with_entity.save()

    event_view = snowflake_event_table_with_entity.get_view()
    feature_group = event_view.groupby("cust_id").aggregate_over(
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


@pytest.fixture(name="feature_latest_item_count")
def feature_latest_item_count(
    snowflake_event_table, snowflake_item_table, cust_id_entity, transaction_entity
):
    """
    Feature latest item count
    """
    # label entities, update critical data info and create views
    snowflake_event_table.cust_id.as_entity(cust_id_entity.name)
    snowflake_event_table.col_int.as_entity(transaction_entity.name)
    snowflake_event_view = snowflake_event_table.get_view()

    snowflake_item_table.event_id_col.as_entity(transaction_entity.name)
    for col in snowflake_item_table.columns:
        snowflake_item_table[col].update_critical_data_info(
            cleaning_operations=[MissingValueImputation(imputed_value=0)]
        )
    snowflake_item_view = snowflake_item_table.get_view(event_suffix="_event_table")

    # save table
    snowflake_item_table.save()

    # feature creation
    item_count = snowflake_item_view.groupby("event_id_col").aggregate(
        None,
        method="count",
        feature_name="item_count",
        fill_value=0,
    )
    snowflake_event_view.add_feature("item_count", item_count, entity_column="col_int")
    feature_group = snowflake_event_view.groupby("cust_id").aggregate_over(
        value_column="item_count",
        method="latest",
        feature_names=["latest_item_count"],
        windows=["30d"],
    )
    latest_item_count = feature_group["latest_item_count"]
    return latest_item_count


def test_feature_and_feature_list_version(feature_group, mock_api_object_cache):
    """
    Test feature & feature list version logic
    """
    _ = mock_api_object_cache

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


def test_feature_list__as_default_version(feature_group):
    """Test feature list as_default_version method"""
    feature_list = FeatureList([feature_group], name="my_special_fl")
    feature_list.save()

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
    new_feature_list_version = feature_list.create_new_version(FeatureListNewVersionMode.AUTO)
    assert new_feature_list_version.is_default is True

    # check setting default version fails when default version mode is not MANUAL
    with pytest.raises(RecordUpdateException) as exc:
        feature_list.as_default_version()
    expected = "Cannot set default feature list ID when default version mode is not MANUAL"
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
            version=new_feature_list_version.version.to_str(),
        )
        == new_feature_list_version
    )


def test_create_new_version__on_latest_item_count_feature(feature_latest_item_count):
    """Test create new version on latest item count feature"""
    from featurebyte.query_graph.graph import QueryGraph

    feature_latest_item_count.save()
    feature_latest_item_count.update_readiness(FeatureReadiness.PRODUCTION_READY)
    _ = feature_latest_item_count.definition

    graph_dict = feature_latest_item_count.dict()["graph"]
    query_graph = QueryGraph(**graph_dict)
