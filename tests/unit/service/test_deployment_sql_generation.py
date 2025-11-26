"""
Tests for DeploymentSqlGenerationService
"""

import os
from pathlib import Path

import pytest
import pytest_asyncio
from bson import ObjectId

from featurebyte.models.deployment_sql import DeploymentSqlModel
from tests.util.helper import (
    assert_equal_json_fixture,
    deploy_feature,
)


@pytest.fixture(name="deployment_sql_generation_service")
def deployment_sql_generation_service_fixture(app_container):
    """
    Placeholder for DeploymentSqlGenerationService tests
    """
    return app_container.deployment_sql_generation_service


@pytest.fixture(name="deployment_id")
def deployment_id_fixture():
    """
    Deployment id fixture
    """
    return ObjectId()


@pytest_asyncio.fixture
async def deployed_float_feature_list_cust_id_use_case(
    app_container,
    float_feature,
    deployment_id,
    mock_update_data_warehouse,
    mock_offline_store_feature_manager_dependencies,
):
    """
    Fixture for deployed float feature for custotmer use case
    """
    _ = mock_update_data_warehouse
    feature_list = await deploy_feature(
        app_container,
        float_feature,
        return_type="feature_list",
        deployment_id=deployment_id,
    )
    return feature_list


@pytest_asyncio.fixture
async def deployed_scd_lookup_feature_list(
    app_container,
    scd_lookup_feature,
    deployment_id,
    mock_update_data_warehouse,
    mock_offline_store_feature_manager_dependencies,
):
    """
    Fixture for deployed scd lookup feature
    """
    _ = mock_update_data_warehouse
    _ = mock_offline_store_feature_manager_dependencies
    feature_list = await deploy_feature(
        app_container,
        scd_lookup_feature,
        return_type="feature_list",
        deployment_id=deployment_id,
    )
    return feature_list


def check_deployment_sql(actual: DeploymentSqlModel, fixture_dir, update_fixtures):
    """
    Check deployment SQL against fixture
    """
    actual_dict = actual.model_dump(by_alias=True, include={"feature_table_sqls"})

    # Sanitize dynamic fields
    actual_sql_codes = []
    for idx, feature_table_sql in enumerate(actual_dict["feature_table_sqls"]):
        feature_table_sql.pop("feature_ids")
        actual_sql_codes.append(feature_table_sql["sql_code"])
        expected_sql_filename = f"{idx}.sql"
        feature_table_sql["sql_code"] = f"<redacted: see {expected_sql_filename}>"

    # Check or update overall dict
    expected_dict_filename = os.path.join(fixture_dir, "deployment_sql.json")
    assert_equal_json_fixture(actual_dict, expected_dict_filename, update_fixtures)

    # Check or update SQL code
    expected_sql_codes_dir = os.path.join(fixture_dir, "sql_codes")
    if update_fixtures:
        Path(expected_sql_codes_dir).mkdir(parents=True, exist_ok=True)
        for filename in os.listdir(expected_sql_codes_dir):
            os.remove(os.path.join(expected_sql_codes_dir, filename))
        for idx, sql_code in enumerate(actual_sql_codes):
            with open(os.path.join(expected_sql_codes_dir, f"{idx}.sql"), "w") as file:
                file.write(sql_code)
    else:
        expected_sql_codes = []
        for filename in sorted(
            os.listdir(expected_sql_codes_dir), key=lambda x: int(x.split(".")[0])
        ):
            with open(os.path.join(expected_sql_codes_dir, filename), "r") as file:
                expected_sql = file.read()
            expected_sql_codes.append(expected_sql)
        if len(expected_sql_codes) != len(actual_sql_codes):
            raise AssertionError(
                f"Number of SQL codes mismatch: expected {len(expected_sql_codes)}, "
                f"got {len(actual_sql_codes)}"
            )
        for expected_sql, actual_sql in zip(expected_sql_codes, actual_sql_codes):
            assert actual_sql.strip() == expected_sql.strip()


@pytest.fixture
def setup_deployment_case(request, test_case_name):
    """
    Fixture to setup deployment case. Deliberately not async to allow parametrization.
    """
    test_case_mapping = {
        "float_feature": "deployed_float_feature_list_cust_id_use_case",
        "scd_lookup_feature": "deployed_scd_lookup_feature_list",
    }
    fixture_name = test_case_mapping[test_case_name]
    # This can be an async fixture, pytest-asyncio will handle it here safely
    request.getfixturevalue(fixture_name)


@pytest.mark.parametrize(
    "test_case_name",
    [
        "float_feature",
        "scd_lookup_feature",
    ],
)
@pytest.mark.asyncio
async def test_deployment_sql(
    test_case_name,
    setup_deployment_case,
    deployment_sql_generation_service,
    deployment_id,
    update_fixtures,
):
    """
    Test single feature deployment SQL generation
    """
    _ = setup_deployment_case
    deployment_sql = await deployment_sql_generation_service.generate_deployment_sql(deployment_id)
    check_deployment_sql(
        deployment_sql,
        f"tests/fixtures/deployment_sql/{test_case_name}",
        update_fixtures,
    )
