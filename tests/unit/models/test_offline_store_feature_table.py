"""
Test OfflineStoreFeatureTable class
"""
import pytest
from bson import ObjectId

from featurebyte import FeatureJobSetting
from featurebyte.models.base_feature_or_target_table import FeatureCluster
from featurebyte.models.entity_universe import EntityUniverseModel
from featurebyte.models.offline_store_feature_table import OfflineStoreFeatureTableModel
from featurebyte.models.sqlglot_expression import SqlglotExpressionModel
from featurebyte.query_graph.graph import QueryGraph


@pytest.fixture(name="common_params")
def fixture_common_params():
    """Common parameters for OfflineStoreFeatureTable"""
    return {
        "catalog_id": ObjectId("65a66b90afb61f8eddd27243"),
        "feature_ids": [],
        "primary_entity_ids": [],
        "serving_names": ["cust_id"],
        "feature_job_setting": None,
        "has_ttl": False,
        "last_materialized_at": None,
        "feature_cluster": FeatureCluster(
            feature_store_id=ObjectId(),
            graph=QueryGraph(),
            node_names=[],
        ),
        "output_column_names": [],
        "output_dtypes": [],
        "entity_universe": EntityUniverseModel(
            query_template=SqlglotExpressionModel(formatted_expression="")
        ),
    }


@pytest.mark.parametrize(
    "serving_names,feature_job_setting,expected",
    [
        (["cust_id"], None, "cust_id"),
        (["_cust_id"], None, "cust_id"),
        ([], None, "_no_entity"),
        (
            ["cust_id", "order_id"],
            FeatureJobSetting(frequency="1d", time_modulo_frequency="1h", blind_spot="1d"),
            "cust_id_1d",
        ),
        (
            ["a_very_long_serving_name_that_is_longer_than_20_characters"],
            FeatureJobSetting(frequency="1d", time_modulo_frequency="1h", blind_spot="1d"),
            "a_very_long__1d",
        ),
        (
            ["a_very_long_serving_name_that_is_longer_than_20_characters"],
            None,
            "a_very_long_ser",
        ),
        (
            ["a_very_long_serving_name_that_is_longer_than_20_characters"],
            FeatureJobSetting(
                frequency="100d21h59m59s", time_modulo_frequency="1h", blind_spot="1d"
            ),
            "a_very_lon_100d",
        ),
        (
            ["a_very_long_serving_name_that_is_longer_than_20_characters"],
            FeatureJobSetting(frequency="1d21h59m59s", time_modulo_frequency="1h", blind_spot="1d"),
            "a_very_lo_1d21h",
        ),
    ],
)
def test_get_basename(serving_names, feature_job_setting, common_params, expected):
    """
    Test get_basename
    """
    feature_table = OfflineStoreFeatureTableModel(
        **{
            **common_params,
            "serving_names": serving_names,
            "feature_job_setting": feature_job_setting,
        }
    )
    base_name = feature_table.get_basename()
    assert len(base_name) <= 20
    assert base_name == expected
