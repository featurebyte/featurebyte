"""
Test updating online store for a catalog
"""

import os
from datetime import datetime
from unittest.mock import patch

import pandas as pd
import pytest
import pytest_asyncio
from sqlglot import parse_one

import featurebyte as fb
from featurebyte.enum import DBVarType, TargetType
from featurebyte.query_graph.sql.common import sql_to_string
from featurebyte.schema.feature_list import OnlineFeaturesRequestPayload
from tests.source_types import SNOWFLAKE_SPARK_DATABRICKS_UNITY


@pytest.fixture(name="always_enable_feast_integration", scope="module", autouse=True)
def always_enable_feast_integration_fixture():
    """
    Enable feast integration for all tests in this module
    """
    with patch.dict(
        os.environ,
        {
            "FEATUREBYTE_GRAPH_CLEAR_PERIOD": "1000",
        },
    ):
        yield


@pytest.fixture(name="always_patch_app_get_storage", scope="module", autouse=True)
def always_patch_app_get_storage_fixture(storage):
    """
    Patch app.get_storage for all tests in this module
    """
    with patch("featurebyte.app.get_storage", return_value=storage):
        yield


@pytest_asyncio.fixture(name="catalog_online_store_disabled", scope="module")
def catalog_online_store_disabled_fixture(catalog):
    """
    Catalog with online store disabled
    """
    catalog.update_online_store(None)
    return catalog


@pytest.fixture(name="order_use_case", scope="module")
def order_use_case_fixture(order_entity):
    """
    Fixture for order level use case
    """
    target = fb.TargetNamespace.create(
        "update_online_store_order_target",
        primary_entity=[order_entity.name],
        dtype=DBVarType.FLOAT,
        target_type=TargetType.REGRESSION,
    )
    context = fb.Context.create(
        name="update_online_store_order_context",
        primary_entity=[order_entity.name],
    )
    use_case = fb.UseCase.create(
        "update_online_store_order_use_case",
        target.name,
        context.name,
        "order_description",
    )
    return use_case


@pytest.fixture(name="simple_feature", scope="module")
def simple_feature_fixture(event_table, scd_table):
    """
    Fixture for a simple feature
    """
    _ = event_table  # to establish relationships
    scd_view = scd_table.get_view()
    feature = scd_view["User Status"].as_feature("Simple User Status Feature")
    return feature


@pytest_asyncio.fixture(name="deployed_feature_list_without_online_store", scope="module")
async def deployed_features_list_without_online_store_fixture(
    catalog_online_store_disabled, simple_feature, order_use_case
):
    """
    Fixture for deployed feature list
    """
    _ = catalog_online_store_disabled

    # Disable any existing deployments first for a clean state
    for deployment_name in fb.Deployment.list()["name"]:
        deployment = fb.Deployment.get(deployment_name)
        deployment.disable()

    features = [simple_feature]
    feature_list = fb.FeatureList(features, name="FL_ONLINE_STORE_UPDATE")
    feature_list.save()
    with patch(
        "featurebyte.service.feature_manager.get_next_job_datetime",
        return_value=pd.Timestamp("2001-01-02 12:00:00").to_pydatetime(),
    ):
        deployment = feature_list.deploy(
            make_production_ready=True, use_case_name=order_use_case.name
        )
        with patch(
            "featurebyte.service.feature_materialize.datetime", autospec=True
        ) as mock_datetime:
            mock_datetime.utcnow.return_value = datetime(2001, 1, 2, 12)
            deployment.enable()
    yield deployment
    deployment.disable()


@pytest.mark.order(98)
@pytest.mark.parametrize("source_type", SNOWFLAKE_SPARK_DATABRICKS_UNITY, indirect=True)
@pytest.mark.asyncio
async def test_offline_feature_store_tables_without_online_store(
    catalog,
    session,
    app_container,
    deployed_feature_list_without_online_store,
):
    """
    Test offline feature tables are empty because:

    - online store was not enabled during deployment
    - populate_offline_feature_tables was not set to True
    """
    async for (
        feature_table_model
    ) in app_container.offline_store_feature_table_service.list_documents_iterator({}):
        df = await session.execute_query(
            sql_to_string(
                parse_one(f'SELECT * FROM "{feature_table_model.name}"'),
                session.source_type,
            )
        )
        assert df.shape[0] == 0, (
            f"Offline feature table {feature_table_model.name} is expected to be empty"
        )


@pytest.mark.order(99)
@pytest.mark.parametrize("source_type", SNOWFLAKE_SPARK_DATABRICKS_UNITY, indirect=True)
@pytest.mark.asyncio
async def test_catalog_update_online_store(
    config,
    catalog,
    online_store,
    deployed_feature_list_without_online_store,
):
    """
    Test the workflow of updating online store for catalog

    Ordered to run later than other feature store integration test to reduce coupling.
    """
    # Enable online store after deployment is made
    with patch("featurebyte.service.feature_materialize.datetime", autospec=True) as mock_datetime:
        mock_datetime.utcnow.return_value = datetime(2001, 1, 2, 12)
        catalog.update_online_store(online_store.name)

    # Check get_online_features() produces correct result
    client = config.get_client()
    deployment = deployed_feature_list_without_online_store
    entity_serving_names = [
        {
            "order_id": "T364",
        }
    ]
    data = OnlineFeaturesRequestPayload(entity_serving_names=entity_serving_names)
    with patch("featurebyte.service.online_serving.datetime", autospec=True) as mock_datetime:
        mock_datetime.utcnow.return_value = datetime(2001, 1, 2, 12)
        res = client.post(
            f"/deployment/{deployment.id}/online_features",
            json=data.json_dict(),
        )
    assert res.status_code == 200
    feat_dict = res.json()["features"][0]
    assert feat_dict == {"order_id": "T364", "Simple User Status Feature": "STÀTUS_CODE_37"}

    # Trigger update online store again (should not error even if there are no new rows in offline
    # feature tables). Unset and set again to trigger the update.
    catalog.update_online_store(None)
    with patch("featurebyte.service.feature_materialize.datetime", autospec=True) as mock_datetime:
        mock_datetime.utcnow.return_value = datetime(2001, 1, 2, 12)
        catalog.update_online_store(online_store.name)
