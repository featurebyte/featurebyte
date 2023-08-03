"""
Integration test for online enabling features
"""
import asyncio
import os
import subprocess
import tempfile
import time
from textwrap import dedent

import pytest
import pytest_asyncio
import requests

from featurebyte import FeatureList
from featurebyte.logging import get_logger
from featurebyte.schema.feature_list import OnlineFeaturesRequestPayload
from tests.integration.conftest import MONGO_CONNECTION

logger = get_logger(__name__)


@pytest_asyncio.fixture(scope="session", name="app_service")
async def app_service_fixture(persistent):
    """
    Start featurebyte service for testing
    """
    # use the same database as persistent fixture
    env = os.environ.copy()
    env.update({"MONGODB_URI": MONGO_CONNECTION, "MONGODB_DB": persistent._database})
    with subprocess.Popen(
        [
            "uvicorn",
            "featurebyte.app:app",
            "--port=8080",
        ],
        env=env,
    ) as proc:
        try:
            # wait for service to start
            start = time.time()
            while time.time() - start < 60:
                if proc.poll() is not None:
                    raise RuntimeError(f"uvicorn exited with code {proc.returncode}")
                try:
                    response = requests.get("http://localhost:8080/status", timeout=5)
                    if response.status_code == 200:
                        logger.info("service started")
                        yield persistent
                        return
                except requests.exceptions.ConnectionError:
                    pass
                logger.info("waiting for service to be started...")
                await asyncio.sleep(1)
            raise TimeoutError("service did not start")
        finally:
            proc.kill()


@pytest.fixture(name="online_enabled_feature_list_and_deployment", scope="module")
def online_enabled_feature_list_and_deployment_fixture(event_table, config):
    """
    Fixture for an online enabled feature

    To avoid side effects, this should not be shared with other tests.
    """

    event_view = event_table.get_view()
    event_view["ÀMOUNT"] = event_view["ÀMOUNT"] + 12345

    # Aggregate using a different entity than "ÜSER ID". Otherwise, it will be creating a feature
    # with the same online store table as the feature used in
    # tests/integration/query_graph/test_online_serving.py. That will cause that test to fail.
    feature_group = event_view.groupby("PRODUCT_ACTION").aggregate_over(
        "ÀMOUNT",
        method="sum",
        windows=["24h"],
        feature_names=["FEATURE_FOR_ONLINE_ENABLE_TESTING"],
    )
    features = [feature_group["FEATURE_FOR_ONLINE_ENABLE_TESTING"]]
    for feature in features:
        feature.save()

    feature_list = FeatureList(
        features, name="My Feature List (tests/integration/api/test_feature.py)"
    )
    feature_list.save()
    deployment = feature_list.deploy(make_production_ready=True)
    deployment.enable()

    yield feature_list, deployment

    deployment.disable()


@pytest.mark.parametrize("source_type", ["snowflake"], indirect=True)
@pytest.mark.asyncio
async def test_online_enable_non_time_aware_feature(item_table, config):
    """
    Test online enabling a non-time aware feature
    """
    item_view = item_table.get_view()
    feature = item_view.groupby("order_id").aggregate(
        method="count", feature_name="my_item_feature_for_online_enable_test"
    )
    feature_list = FeatureList([feature], "my_non_time_aware_list")
    feature_list.save()
    deployment = None

    try:
        deployment = feature_list.deploy(make_production_ready=True)
        deployment.enable()

        # Check feature request
        client = config.get_client()
        entity_serving_names = [{"order_id": "T1"}]
        data = OnlineFeaturesRequestPayload(entity_serving_names=entity_serving_names)
        res = client.post(
            f"/deployment/{deployment.id}/online_features",
            json=data.json_dict(),
        )
    finally:
        if deployment:
            deployment.disable()

    assert res.status_code == 200
    assert res.json() == {
        "features": [{"order_id": "T1", "my_item_feature_for_online_enable_test": 3}]
    }


@pytest.mark.parametrize("source_type", ["snowflake"], indirect=True)
def test_get_online_serving_code(online_enabled_feature_list_and_deployment, app_service):
    """
    Get the code for online serving
    """
    _, deployment = online_enabled_feature_list_and_deployment

    with tempfile.TemporaryDirectory() as tempdir:
        # test python serving code
        code_content = deployment.get_online_serving_code("python")
        code_path = os.path.join(tempdir, "online_serving.py")
        with open(code_path, "w", encoding="utf8") as file_obj:
            file_obj.write(code_content)
            last_line = code_content.split("\n")[-1]
            file_obj.write(f"\nprint({last_line})")

        logger.info(code_content)
        result = subprocess.check_output(["python", code_path], stderr=subprocess.STDOUT)
        assert dedent(result.decode("utf8")) == (
            "  PRODUCT_ACTION FEATURE_FOR_ONLINE_ENABLE_TESTING\n"
            "0         detail                              None\n"
        )

        # test shell serving code
        code_content = deployment.get_online_serving_code("sh")
        code_path = os.path.join(tempdir, "online_serving.sh")
        with open(code_path, "w", encoding="utf8") as file_obj:
            file_obj.write(code_content)

        logger.info(code_content)
        result = subprocess.check_output(["sh", code_path], stderr=subprocess.STDOUT)
        assert result.decode("utf8").endswith(
            '{"features":[{"PRODUCT_ACTION":"detail","FEATURE_FOR_ONLINE_ENABLE_TESTING":null}]}'
        )
