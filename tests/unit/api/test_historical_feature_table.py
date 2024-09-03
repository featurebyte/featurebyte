"""
Unit tests for HistoricalFeatureTable class
"""

from typing import Any
from unittest import mock

import pytest

from featurebyte import FeatureList
from featurebyte.api.historical_feature_table import HistoricalFeatureTable
from featurebyte.exception import RecordCreationException
from featurebyte.schema.constant import MAX_BATCH_FEATURE_ITEM_COUNT
from tests.unit.api.base_materialize_table_test import BaseMaterializedTableApiTest


class TestHistoricalFeatureTable(BaseMaterializedTableApiTest):
    """
    Test historical feature table
    """

    table_type = HistoricalFeatureTable

    def assert_info_dict(self, info_dict: dict[str, Any]) -> None:
        assert info_dict["table_details"]["table_name"].startswith("HISTORICAL_FEATURE_TABLE_")
        assert isinstance(info_dict["feature_list_version"], str)
        assert info_dict == {
            "name": "my_historical_feature_table",
            "feature_list_name": "feature_list_for_historical_feature_table",
            "feature_list_version": info_dict["feature_list_version"],
            "observation_table_name": "observation_table_from_source_table",
            "table_details": {
                "database_name": "sf_database",
                "schema_name": "sf_schema",
                "table_name": info_dict["table_details"]["table_name"],
            },
            "created_at": info_dict["created_at"],
            "updated_at": None,
            "description": None,
        }

    def test_associated_attributes(self, table_under_test):
        """Test attributes associated with the historical feature table"""
        assert table_under_test.observation_table.name == "observation_table_from_source_table"
        assert table_under_test.feature_list.name == "feature_list_for_historical_feature_table"
        assert table_under_test.feature_names == ["sum_1d"]
        assert table_under_test.target_name is None
        assert table_under_test.list_deployments().shape[0] == 0

    def test_get_with_mlflow(self, table_under_test, caplog):
        """Test get with mlflow installed"""
        original_import = __import__
        mlflow_mock = mock.Mock()

        def import_mock(name, *args, **kwargs):
            if name == "mlflow":
                return mlflow_mock
            return original_import(name, *args, **kwargs)

        with mock.patch("builtins.__import__", side_effect=import_mock):
            HistoricalFeatureTable.get(name=table_under_test.name)

        mlflow_mock.log_param.assert_called_once_with(
            "fb_training_data",
            {
                "catalog_id": table_under_test.catalog_id,
                "feature_list_name": "feature_list_for_historical_feature_table",
                "target_name": None,
                "dataset_name": "my_historical_feature_table",
                "primary_entity": ["customer"],
            },
        )

        # check when log_param raises an exception
        mlflow_mock.log_param.side_effect = Exception("Random error")
        with mock.patch("builtins.__import__", side_effect=import_mock):
            HistoricalFeatureTable.get(name=table_under_test.name)

        last_log = caplog.records[-1]
        assert (
            last_log.msg
            == "Failed to log featurebyte training data information to mlflow: Random error"
        )


@pytest.mark.usefixtures("patched_observation_table_service")
def test_historical_feature_table_validation(catalog, snowflake_event_table, cust_id_entity):
    """Test historical feature table validation"""
    snowflake_event_table.col_int.as_entity(cust_id_entity.name)
    event_view = snowflake_event_table.get_view()
    feature = event_view.col_float.as_feature("feat")
    features = []
    for i in range(MAX_BATCH_FEATURE_ITEM_COUNT + 1):
        feat = feature + i
        feat.name = f"feat_{i}"
        features.append(feat)

    feature_list = FeatureList(features, name="test feature list")

    table_details = snowflake_event_table.tabular_source.table_details
    observation_table = (
        catalog.get_data_source()
        .get_source_table(
            table_name=table_details.table_name,
            schema_name=table_details.schema_name,
            database_name=table_details.database_name,
        )
        .create_observation_table(
            name="observation_table",
            primary_entities=[cust_id_entity.name],
            columns=["event_timestamp", "cust_id"],
            columns_rename_mapping={"event_timestamp": "POINT_IN_TIME"},
        )
    )

    expected_error = (
        "Number of features exceeds the limit of 500, please reduce the number of features or save the "
        "features in a feature list and try again."
    )
    with pytest.raises(RecordCreationException, match=expected_error):
        feature_list.compute_historical_feature_table(
            observation_set=observation_table,
            historical_feature_table_name="historical_feature_table",
        )
