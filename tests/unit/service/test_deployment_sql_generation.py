"""
Tests for DeploymentSqlGenerationService
"""

import os
from contextlib import nullcontext
from pathlib import Path
from typing import NamedTuple, Optional
from unittest import mock

import pytest
import pytest_asyncio
from bson import ObjectId

from featurebyte.enum import SourceType
from featurebyte.exception import DeploymentSqlGenerationError
from featurebyte.models.deployment_sql import DeploymentSqlModel
from tests.util.helper import (
    assert_equal_json_fixture,
    deploy_feature,
    deploy_features,
    replace_objectid_suffix,
)


class TestCase(NamedTuple):
    """Test case configuration for deployment SQL generation tests."""

    fixture_name: str
    source_type: Optional[SourceType] = None


TEST_CASES_MAPPING = {
    "float_feature": TestCase("deployed_float_feature_list_cust_id_use_case"),
    "scd_lookup_feature": TestCase("deployed_scd_lookup_feature_list"),
    "snapshots_lookup_feature": TestCase("deployed_snapshot_feature_list"),
    "time_series_feature": TestCase("deployed_time_series_feature_list"),
    "time_since_latest_event_timestamp_feature": TestCase(
        "deployed_time_since_latest_event_timestamp_feature_list"
    ),
    "float_feature_via_transaction": TestCase("deployed_float_feature_list_transaction_use_case"),
    "aggregate_asat_feature": TestCase("deployed_aggregate_asat_feature_list"),
    "aggregate_asat_offset_feature": TestCase("deployed_aggregate_asat_offset_feature_list"),
    "snapshots_aggregate_asat_feature": TestCase("deployed_snapshots_aggregate_asat_feature_list"),
    "multiple_feature_tables": TestCase("deployed_multiple_feature_tables_feature_list"),
    "internal_parent_child_relationships": TestCase(
        "deployed_feature_with_internal_parent_child_relationships"
    ),
    "cosine_similarity_feature": TestCase("deployed_cosine_similarity_feature_list"),
    "cosine_similarity_feature_spark": TestCase(
        "deployed_cosine_similarity_feature_list", SourceType.SPARK
    ),
    "same_entity_different_sources": TestCase(
        "deployed_same_entity_different_sources_feature_list"
    ),
}


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
async def deployed_float_feature_list_transaction_use_case(
    app_container,
    float_feature,
    deployment_id,
    mock_update_data_warehouse,
    mock_offline_store_feature_manager_dependencies,
    transaction_entity,
):
    """
    Fixture for deployed float feature for transaction use case
    """
    _ = mock_update_data_warehouse
    feature_list = await deploy_feature(
        app_container,
        float_feature,
        return_type="feature_list",
        deployment_id=deployment_id,
        context_primary_entity_ids=[transaction_entity.id],
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


@pytest_asyncio.fixture
async def deployed_snapshot_feature_list(
    app_container,
    snapshots_lookup_feature,
    deployment_id,
    mock_update_data_warehouse,
    mock_offline_store_feature_manager_dependencies,
):
    """
    Fixture for deployed snapshots lookup feature
    """
    _ = mock_update_data_warehouse
    _ = mock_offline_store_feature_manager_dependencies
    feature_list = await deploy_feature(
        app_container,
        snapshots_lookup_feature,
        return_type="feature_list",
        deployment_id=deployment_id,
    )
    return feature_list


@pytest_asyncio.fixture
async def deployed_time_series_feature_list(
    app_container,
    ts_window_aggregate_feature,
    deployment_id,
    mock_update_data_warehouse,
    mock_offline_store_feature_manager_dependencies,
):
    """
    Fixture for deployed snapshots lookup feature
    """
    _ = mock_update_data_warehouse
    _ = mock_offline_store_feature_manager_dependencies
    feature_list = await deploy_feature(
        app_container,
        ts_window_aggregate_feature,
        return_type="feature_list",
        deployment_id=deployment_id,
    )
    return feature_list


@pytest_asyncio.fixture
async def deployed_time_since_latest_event_timestamp_feature_list(
    app_container,
    time_since_latest_event_timestamp_feature,
    deployment_id,
    mock_update_data_warehouse,
    mock_offline_store_feature_manager_dependencies,
):
    """
    Fixture for deployed time since latest event timestamp feature
    """
    _ = mock_update_data_warehouse
    _ = mock_offline_store_feature_manager_dependencies
    feature_list = await deploy_feature(
        app_container,
        time_since_latest_event_timestamp_feature,
        return_type="feature_list",
        deployment_id=deployment_id,
    )
    return feature_list


@pytest_asyncio.fixture
async def deployed_aggregate_asat_feature_list(
    app_container,
    aggregate_asat_feature,
    deployment_id,
    mock_update_data_warehouse,
    mock_offline_store_feature_manager_dependencies,
):
    """
    Fixture for deployed aggregate asat feature
    """
    _ = mock_update_data_warehouse
    _ = mock_offline_store_feature_manager_dependencies
    feature_list = await deploy_feature(
        app_container,
        aggregate_asat_feature,
        return_type="feature_list",
        deployment_id=deployment_id,
    )
    return feature_list


@pytest_asyncio.fixture
async def deployed_aggregate_asat_offset_feature_list(
    app_container,
    aggregate_asat_offset_feature,
    deployment_id,
    mock_update_data_warehouse,
    mock_offline_store_feature_manager_dependencies,
):
    """
    Fixture for deployed aggregate asat feature with offset
    """
    _ = mock_update_data_warehouse
    _ = mock_offline_store_feature_manager_dependencies
    feature_list = await deploy_feature(
        app_container,
        aggregate_asat_offset_feature,
        return_type="feature_list",
        deployment_id=deployment_id,
    )
    return feature_list


@pytest_asyncio.fixture
async def deployed_snapshots_aggregate_asat_feature_list(
    app_container,
    snapshots_aggregate_asat_feature,
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
        snapshots_aggregate_asat_feature,
        return_type="feature_list",
        deployment_id=deployment_id,
    )
    return feature_list


@pytest_asyncio.fixture
async def deployed_multiple_feature_tables_feature_list(
    app_container,
    time_since_latest_event_timestamp_feature,
    ts_window_aggregate_feature,
    deployment_id,
    mock_update_data_warehouse,
    mock_offline_store_feature_manager_dependencies,
):
    """
    Fixture for a feature list that requires multiple feature tables
    """
    _ = mock_update_data_warehouse
    _ = mock_offline_store_feature_manager_dependencies
    feature_list = await deploy_features(
        app_container,
        [time_since_latest_event_timestamp_feature, ts_window_aggregate_feature],
        feature_list_name=f"feature_list_{ObjectId()}",
        deployment_id=deployment_id,
    )
    return feature_list


@pytest_asyncio.fixture
async def deployed_feature_with_internal_parent_child_relationships(
    app_container,
    feature_with_internal_parent_child_relationships,
    deployment_id,
    mock_update_data_warehouse,
    mock_offline_store_feature_manager_dependencies,
):
    """
    Fixture for a feature list that requires multiple feature tables
    """
    _ = mock_update_data_warehouse
    _ = mock_offline_store_feature_manager_dependencies
    feature_list = await deploy_features(
        app_container,
        [feature_with_internal_parent_child_relationships],
        feature_list_name=f"feature_list_{ObjectId()}",
        deployment_id=deployment_id,
    )
    return feature_list


@pytest_asyncio.fixture
async def deployed_cosine_similarity_feature_list(
    app_container,
    count_per_category_feature_group,
    deployment_id,
    mock_update_data_warehouse,
    mock_offline_store_feature_manager_dependencies,
):
    """
    Fixture for deployed cosine similarity feature (uses UDFs)
    """
    _ = mock_update_data_warehouse
    _ = mock_offline_store_feature_manager_dependencies
    counts_30m = count_per_category_feature_group["counts_30m"]
    counts_1d = count_per_category_feature_group["counts_1d"]
    cosine_sim_feature = counts_30m.cd.cosine_similarity(counts_1d)
    cosine_sim_feature.name = "cosine_similarity_30m_vs_1d"
    feature_list = await deploy_feature(
        app_container,
        cosine_sim_feature,
        return_type="feature_list",
        deployment_id=deployment_id,
    )
    return feature_list


@pytest_asyncio.fixture
async def deployed_same_entity_different_sources_feature_list(
    app_container,
    snowflake_event_table_with_entity,
    feature_group_feature_job_setting,
    deployment_id,
    mock_update_data_warehouse,
    mock_offline_store_feature_manager_dependencies,
):
    """
    Fixture for features with same entity but different aggregation sources
    """
    _ = mock_update_data_warehouse
    _ = mock_offline_store_feature_manager_dependencies
    view = snowflake_event_table_with_entity.get_view()
    view_1 = view[view["col_float"] > 100]
    view_2 = view[view["col_float"] < 10]

    def _get_feature(feature_view, feature_name):
        return feature_view.groupby("cust_id").aggregate_over(
            value_column="col_float",
            method="sum",
            windows=["1d"],
            feature_job_setting=feature_group_feature_job_setting,
            feature_names=[feature_name],
        )[feature_name]

    feature_1 = _get_feature(view_1, "sum_1d_gt_100")
    feature_2 = _get_feature(view_2, "sum_1d_lt_10")

    feature_list = await deploy_features(
        app_container,
        [feature_1, feature_2],
        feature_list_name=f"feature_list_{ObjectId()}",
        deployment_id=deployment_id,
    )
    return feature_list


@pytest_asyncio.fixture
async def deployed_not_supported_feature_list(
    app_container,
    snapshots_lookup_feature,
    float_feature_composite_entity,
    deployment_id,
    mock_update_data_warehouse,
    mock_offline_store_feature_manager_dependencies,
):
    """
    Fixture for deployed not supported feature
    """
    _ = mock_update_data_warehouse
    _ = mock_offline_store_feature_manager_dependencies
    feature = snapshots_lookup_feature + float_feature_composite_entity
    feature.name = "my_feature"
    feature_list = await deploy_feature(
        app_container,
        feature,
        return_type="feature_list",
        deployment_id=deployment_id,
    )
    return feature_list


def _check_sql_files(
    sql_codes_dir: str,
    actual_sqls: list[str],
    prefix: str,
    label: str,
    update_fixtures: bool,
) -> None:
    """
    Check or update SQL fixture files.

    Parameters
    ----------
    sql_codes_dir: str
        Directory containing SQL fixture files
    actual_sqls: list[str]
        List of actual SQL strings to check/write
    prefix: str
        Filename prefix (e.g., "" for feature SQLs, "udf_" for UDF SQLs)
    label: str
        Label for error messages (e.g., "SQL codes", "UDF registration SQLs")
    update_fixtures: bool
        Whether to update fixtures or check against them
    """
    if update_fixtures:
        for idx, sql in enumerate(actual_sqls):
            with open(os.path.join(sql_codes_dir, f"{prefix}{idx}.sql"), "w") as file:
                file.write(sql)
    else:
        expected_sqls = []
        for filename in sorted(os.listdir(sql_codes_dir)):
            if prefix:
                if not filename.startswith(prefix):
                    continue
            else:
                if filename.startswith("udf_"):
                    continue
            with open(os.path.join(sql_codes_dir, filename), "r") as file:
                expected_sqls.append(file.read())
        if len(expected_sqls) != len(actual_sqls):
            raise AssertionError(
                f"Number of {label} mismatch: expected {len(expected_sqls)}, got {len(actual_sqls)}"
            )
        for expected_sql, actual_sql in zip(expected_sqls, actual_sqls):
            assert actual_sql.strip() == expected_sql.strip()


def check_deployment_sql(actual: DeploymentSqlModel, fixture_dir, update_fixtures):
    """
    Check deployment SQL against fixture
    """
    actual_dict = actual.model_dump(
        by_alias=True, include={"feature_table_sqls", "udf_registration_sqls"}
    )

    # Sanitize dynamic fields
    actual_sql_codes = []
    for idx, feature_table_sql in enumerate(actual_dict["feature_table_sqls"]):
        feature_table_sql.pop("feature_ids")
        actual_sql_codes.append(replace_objectid_suffix(feature_table_sql["sql_code"]))
        expected_sql_filename = f"{idx}.sql"
        feature_table_sql["sql_code"] = f"<redacted: see {expected_sql_filename}>"

    # Handle UDF registration SQLs - store them separately if present
    actual_udf_sqls = actual_dict.get("udf_registration_sqls", [])
    if actual_udf_sqls:
        actual_dict["udf_registration_sqls"] = [
            f"<redacted: see udf_{idx}.sql>" for idx in range(len(actual_udf_sqls))
        ]

    # Check or update overall dict
    expected_dict_filename = os.path.join(fixture_dir, "deployment_sql.json")
    assert_equal_json_fixture(actual_dict, expected_dict_filename, update_fixtures)

    # Check or update SQL files
    expected_sql_codes_dir = os.path.join(fixture_dir, "sql_codes")
    if update_fixtures:
        Path(expected_sql_codes_dir).mkdir(parents=True, exist_ok=True)
        for filename in os.listdir(expected_sql_codes_dir):
            os.remove(os.path.join(expected_sql_codes_dir, filename))

    _check_sql_files(
        sql_codes_dir=expected_sql_codes_dir,
        actual_sqls=actual_sql_codes,
        prefix="",
        label="SQL codes",
        update_fixtures=update_fixtures,
    )
    _check_sql_files(
        sql_codes_dir=expected_sql_codes_dir,
        actual_sqls=actual_udf_sqls,
        prefix="udf_",
        label="UDF registration SQLs",
        update_fixtures=update_fixtures,
    )


@pytest.fixture
def setup_deployment_case(request, test_case_name):
    """
    Fixture to setup deployment case. Deliberately not async to allow parametrization.
    """
    test_case = TEST_CASES_MAPPING[test_case_name]
    # This can be an async fixture, pytest-asyncio will handle it here safely
    request.getfixturevalue(test_case.fixture_name)


def _create_source_type_patch(deployment_sql_generation_service, source_type_override):
    """
    Create a context manager that patches the feature store source type.

    Parameters
    ----------
    deployment_sql_generation_service: DeploymentSqlGenerationService
        The service instance
    source_type_override: Optional[SourceType]
        The source type to patch to, or None for no patching

    Returns
    -------
    ContextManager
        A context manager that patches the source type if needed
    """
    if source_type_override is None:
        # No patching needed, return a no-op context manager
        return nullcontext()

    # Capture the bound method before patching the instance attribute.
    original_get_document = deployment_sql_generation_service.feature_store_service.get_document

    async def patched_get_document(document_id):
        doc = await original_get_document(document_id)
        # Create a copy with the overridden source type
        doc_dict = doc.model_dump()
        doc_dict["type"] = source_type_override
        return doc.__class__(**doc_dict)

    return mock.patch.object(
        deployment_sql_generation_service.feature_store_service,
        "get_document",
        new=mock.AsyncMock(side_effect=patched_get_document),
    )


@pytest.mark.parametrize(
    "test_case_name",
    list(TEST_CASES_MAPPING.keys()),
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
    test_case = TEST_CASES_MAPPING[test_case_name]

    with _create_source_type_patch(deployment_sql_generation_service, test_case.source_type):
        deployment_sql = await deployment_sql_generation_service.generate_deployment_sql(
            deployment_id
        )

    check_deployment_sql(
        deployment_sql,
        f"tests/fixtures/deployment_sql/{test_case_name}",
        update_fixtures,
    )


@pytest.mark.asyncio
async def test_deployment_sql__not_supported(
    deployed_not_supported_feature_list,
    deployment_sql_generation_service,
    deployment_id,
    update_fixtures,
):
    """
    Test single feature deployment SQL generation
    """
    _ = setup_deployment_case
    with pytest.raises(DeploymentSqlGenerationError) as exc:
        _ = await deployment_sql_generation_service.generate_deployment_sql(deployment_id)
    assert "Deployment SQL generation is not supported" in str(exc.value)
