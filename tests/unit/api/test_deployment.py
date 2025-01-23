"""
Unit tests for Deployment class
"""

import textwrap
from unittest.mock import patch

import pandas as pd
import pytest
from freezegun import freeze_time
from pandas.testing import assert_frame_equal

import featurebyte as fb
from featurebyte import FeatureList
from featurebyte.api.deployment import Deployment
from featurebyte.config import Configurations
from featurebyte.exception import (
    FeatureListNotOnlineEnabledError,
    RecordCreationException,
    RecordRetrievalException,
)
from featurebyte.query_graph.node.nested import AggregationNodeInfo


@pytest.fixture(name="mock_warehouse_update_for_deployment", autouse=True)
def mock_warehouse_update_for_deployment_fixture(
    mock_update_data_warehouse,
    mock_offline_store_feature_manager_dependencies,
):
    """
    Mocks the warehouse update for deployment
    """
    _ = mock_update_data_warehouse, mock_offline_store_feature_manager_dependencies
    yield


def test_get(deployment):
    """
    Test retrieving a Deployment object by name
    """
    retrieved_deployment = Deployment.get(deployment.name)
    assert retrieved_deployment == deployment


def test_list(deployment):
    """
    Test listing Deployment objects
    """
    df = deployment.list()
    expected = pd.DataFrame([
        {
            "id": str(deployment.id),
            "name": deployment.name,
            "feature_list_name": "my_feature_list",
            "feature_list_version": f'V{pd.Timestamp.now().strftime("%y%m%d")}',
            "num_feature": 1,
            "enabled": False,
        }
    ])
    pd.testing.assert_frame_equal(df, expected[df.columns.tolist()])


def test_info(deployment):
    """Test get deployment info"""
    info_dict = deployment.info()
    expected_version = f'V{pd.Timestamp.now().strftime("%y%m%d")}'
    assert info_dict == {
        "name": f"Deployment with my_feature_list_{expected_version}",
        "feature_list_name": "my_feature_list",
        "feature_list_version": expected_version,
        "num_feature": 1,
        "enabled": False,
        "serving_endpoint": None,
        "created_at": info_dict["created_at"],
        "updated_at": None,
        "description": None,
        "use_case_name": "test_use_case",
    }


def test_get_online_serving_code_not_deployed(deployment):
    """Test feature get_online_serving_code on un-deployed feature list"""
    with pytest.raises(FeatureListNotOnlineEnabledError) as exc:
        deployment.get_online_serving_code()
    assert "Deployment is not enabled." in str(exc.value)


def test_get_online_serving_code_unsupported_language(deployment):
    """Test feature get_online_serving_code with unsupported language"""
    deployment.enable()
    assert deployment.enabled is True
    with pytest.raises(NotImplementedError) as exc:
        deployment.get_online_serving_code(language="java")
    assert "Supported languages: 'python' or 'sh'" in str(exc.value)


def test_list_deployment(deployment, snowflake_feature_store):
    """
    Test summarizing Deployment objects
    """
    config = Configurations()
    client = config.get_client()

    # enable deployment
    deployment.enable()

    original_catalog = fb.Catalog.get_active()
    response = client.get("/deployment/summary/")
    assert response.status_code == 200
    assert response.json() == {"num_feature_list": 1, "num_feature": 1}

    # make sure deployment can be retrieved in different catalog
    catalog = fb.Catalog.create("another_catalog", feature_store_name=snowflake_feature_store.name)
    fb.Catalog.activate(catalog.name)
    response = client.get("/deployment/summary/")
    assert response.json() == {"num_feature_list": 1, "num_feature": 1}

    # change back to original catalog
    fb.Catalog.activate(original_catalog.name)
    response = client.get("/deployment/summary/")
    assert response.json() == {"num_feature_list": 1, "num_feature": 1}


def test_get_online_serving_code(deployment, catalog, config_file):
    """Test feature get_online_serving_code"""
    # Use config
    Configurations(config_file, force=True)

    deployment.enable()
    assert deployment.enabled is True
    url = f"http://localhost:8080/deployment/{deployment.id}/online_features"

    with patch(
        "featurebyte.service.entity_serving_names.EntityServingNamesService.get_table_column_unique_values"
    ) as mock_preview:
        mock_preview.return_value = ["sample_cust_id"]
        assert (
            deployment.get_online_serving_code().strip()
            == textwrap.dedent(
                f'''
                from typing import Any, Dict

                import pandas as pd
                import requests


                def request_features(entity_serving_names: Dict[str, Any]) -> pd.DataFrame:
                    """
                    Send POST request to online serving endpoint

                    Parameters
                    ----------
                    entity_serving_names: Dict[str, Any]
                        Entity serving name values to used for serving request

                    Returns
                    -------
                    pd.DataFrame
                    """
                    response = requests.post(
                        url="{url}",
                        headers={{"Content-Type": "application/json", "active-catalog-id": "{catalog.id}", "Authorization": "Bearer token"}},
                        json={{"entity_serving_names": entity_serving_names}},
                    )
                    assert response.status_code == 200, response.json()
                    return pd.DataFrame.from_dict(response.json()["features"])


                request_features([{{"cust_id": "sample_cust_id"}}])
                '''
            ).strip()
        )
        assert (
            deployment.get_online_serving_code(language="sh").strip()
            == textwrap.dedent(
                f"""
                #!/bin/sh

                curl -X POST \\
                    -H 'Content-Type: application/json' \\
                    -H 'active-catalog-id: {catalog.id}' \\
                    -H 'Authorization: Bearer token' \\
                    -d '{{"entity_serving_names": [{{"cust_id": "sample_cust_id"}}]}}' \\
                    {url}
                """
            ).strip()
        )


@freeze_time("2023-01-20 03:20:00")
def test_get_feature_jobs_status(deployment, feature_job_logs):
    """Test get feature job status"""
    with patch(
        "featurebyte.service.tile_job_log.TileJobLogService.get_logs_dataframe"
    ) as mock_get_jobs_dataframe:
        mock_get_jobs_dataframe.return_value = feature_job_logs
        feature_job_status = deployment.get_feature_jobs_status(
            job_history_window=24, job_duration_tolerance=1700
        )

    fixture_path = "tests/fixtures/feature_job_status/expected_session_logs.parquet"
    expected_session_logs = pd.read_parquet(fixture_path)

    # NOTE: Parquet serialization and deserialization converts NaT to None
    # converting the problematic values to None before comparing
    session_logs = feature_job_status.job_session_logs.copy()
    session_logs.loc[session_logs["ERROR"].isna(), "ERROR"] = None
    assert_frame_equal(session_logs, expected_session_logs)


def test_delete_deployment(deployment):
    """Test delete deployment"""
    deployment_id = deployment.id
    deployment.delete()

    with pytest.raises(RecordRetrievalException):
        Deployment.get_by_id(deployment_id)


def test_associated_object_list_deployment(deployment, historical_feature_table):
    """Test associated object list deployment"""
    feature_list = FeatureList.get_by_id(deployment.feature_list_id)
    list_deployments = feature_list.list_deployments()
    assert list_deployments.shape[0] == 1
    assert list_deployments.name.iloc[0] == deployment.name

    # check historical feature table
    list_deployments = historical_feature_table.list_deployments()
    assert list_deployments.shape[0] == 0

    # create another feature list and deploy
    another_feature_list = historical_feature_table.feature_list
    another_deployment = another_feature_list.deploy()
    list_deployments = another_feature_list.list_deployments()
    assert list_deployments.shape[0] == 1
    assert list_deployments.name.iloc[0] == another_deployment.name


def test_deployment_creation__primary_entity_validation(
    latest_event_timestamp_overall_feature, use_case
):
    """Test deployment creation with primary entity validation"""
    feature_list = FeatureList([latest_event_timestamp_overall_feature], name="my_feature_list")
    feature_list.save()

    expected_error = (
        "Primary entity of the use case is not in the feature list's supported serving entities."
    )
    with pytest.raises(RecordCreationException, match=expected_error):
        feature_list.deploy(
            make_production_ready=True, ignore_guardrails=True, use_case_name=use_case.name
        )


def test_deployment_with_unbounded_window(
    snowflake_event_view_with_entity, cust_id_entity, feature_group_feature_job_setting
):
    """Test deployment with unbounded window"""
    _ = cust_id_entity
    feat_latest = snowflake_event_view_with_entity.groupby("cust_id").aggregate_over(
        value_column="col_float",
        method="latest",
        windows=[None],
        feature_names=["feat_latest"],
        feature_job_setting=feature_group_feature_job_setting,
    )["feat_latest"]

    feat_latest_bounded = snowflake_event_view_with_entity.groupby("cust_id").aggregate_over(
        value_column="col_float",
        method="latest",
        windows=["7d"],
        feature_names=["feat_latest_bounded"],
        feature_job_setting=feature_group_feature_job_setting,
    )["feat_latest_bounded"]
    feat_latest_combined = feat_latest + feat_latest_bounded
    feat_latest_combined.name = "feat_latest_combined"

    feature_list = FeatureList([feat_latest, feat_latest_combined], name="my_feature_list")
    feature_list.save()

    deployment = feature_list.deploy(make_production_ready=True, ignore_guardrails=True)
    deployment.enable()

    feat_latest_model = feat_latest.cached_model
    feat_latest_combined_model = feat_latest_combined.cached_model

    # Check latest without window has no associated aggregation result names (not precomputed in
    # internal online store)
    assert feat_latest_model.aggregation_result_names == []
    assert feat_latest_model.online_store_table_names == []

    # Check latest with window has associated aggregation result names
    assert feat_latest_combined_model.aggregation_result_names == [
        "_fb_internal_cust_id_window_w604800_latest_ac7aa941d28f489e56c9ab50a583a8c6c88eebe5"
    ]
    assert feat_latest_combined_model.online_store_table_names == [
        "ONLINE_STORE_377553E5920DD2DB8B17F21DDD52F8B1194A780C"
    ]

    offline_store_info = feat_latest.cached_model.offline_store_info
    assert offline_store_info.metadata.has_ttl is False
    assert offline_store_info.metadata.feature_job_setting == feature_group_feature_job_setting
    assert offline_store_info.metadata.aggregation_nodes_info == [
        AggregationNodeInfo(node_type="groupby", input_node_name="graph_1", node_name="groupby_1")
    ]
    assert offline_store_info.odfv_info is None

    offline_store_info_combined = feat_latest_combined.cached_model.offline_store_info
    assert offline_store_info_combined.metadata.has_ttl is True
    assert (
        offline_store_info_combined.metadata.feature_job_setting
        == feature_group_feature_job_setting
    )
    assert offline_store_info_combined.metadata.aggregation_nodes_info == [
        AggregationNodeInfo(node_type="groupby", input_node_name="graph_1", node_name="groupby_1"),
        AggregationNodeInfo(node_type="groupby", input_node_name="graph_1", node_name="groupby_2"),
    ]

    version = feat_latest_combined.version
    expected_odfv_codes = f"""
    import datetime
    import json
    import numpy as np
    import pandas as pd
    import scipy as sp


    def odfv_feat_latest_combined_{version.lower()}_{feat_latest_combined.id}(
        inputs: pd.DataFrame,
    ) -> pd.DataFrame:
        df = pd.DataFrame()
        # Time-to-live (TTL) handling to clean up expired data
        request_time = pd.to_datetime(inputs["POINT_IN_TIME"], utc=True)
        cutoff = request_time - pd.Timedelta(seconds=3600)
        feature_timestamp = pd.to_datetime(
            inputs["feat_latest_combined_{version}__ts"], unit="s", utc=True
        )
        mask = (feature_timestamp >= cutoff) & (feature_timestamp <= request_time)
        inputs.loc[~mask, "feat_latest_combined_{version}"] = np.nan
        df["feat_latest_combined_{version}"] = inputs["feat_latest_combined_{version}"]
        df.fillna(np.nan, inplace=True)
        return df
    """
    assert (
        offline_store_info_combined.odfv_info.codes.strip()
        == textwrap.dedent(expected_odfv_codes).strip()
    )

    deployment.disable()


def test_get_online_serving_code_uses_deployment_entities(deployment, config_file):
    """Test feature get_online_serving_code"""
    # Use config
    Configurations(config_file, force=True)

    deployment.enable()
    assert deployment.enabled is True

    with (
        patch("featurebyte.service.deployment.DeploymentService.get_document") as mock_get_document,
        patch(
            "featurebyte.service.entity_serving_names.EntityServingNamesService.get_sample_entity_serving_names"
        ) as mock_get_sample_entity_serving_names,
    ):
        mock_get_sample_entity_serving_names.return_value = [{"cust_id": "sample_cust_id"}]
        deployment.get_online_serving_code()
        deployment = mock_get_document.return_value
        mock_get_sample_entity_serving_names.assert_called_once_with(
            entity_ids=deployment.serving_entity_ids,
            table_ids=None,
            count=1,
        )
