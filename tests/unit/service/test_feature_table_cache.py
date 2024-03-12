"""
Test FeatureTableCacheService
"""
import json
import os
import textwrap
from unittest.mock import patch

import pytest
import pytest_asyncio
from bson import ObjectId
from snowflake.connector import ProgrammingError
from sqlglot import parse_one

from featurebyte.models.observation_table import ObservationTableModel
from featurebyte.models.request_input import SourceTableRequestInput
from featurebyte.query_graph.model.common_table import TabularSource
from featurebyte.query_graph.node.schema import TableDetails
from featurebyte.schema.feature import FeatureServiceCreate
from featurebyte.schema.feature_list import FeatureListServiceCreate


@pytest.fixture(name="auto_mocks", autouse=True)
def auto_mocks_fixture(mock_snowflake_session):
    """
    Patch get_feature_store_session to return a mock session
    """

    with patch(
        "featurebyte.service.feature_table_cache.SessionManagerService.get_feature_store_session"
    ) as session_mock, patch(
        "featurebyte.service.feature_table_cache.ObjectId"
    ) as object_id_mock, patch(
        "featurebyte.service.online_enable.FeatureManagerService.online_enable"
    ):
        session_mock.return_value = mock_snowflake_session
        object_id_mock.return_value = "ObjectId"
        yield


@pytest.fixture(name="mock_get_historical_features")
def mock_get_historical_features_fixture():
    """
    Patch get_historical_features
    """
    with patch("featurebyte.service.feature_table_cache.get_historical_features") as mock:
        yield mock


@pytest.fixture(name="mock_get_target")
def mock_get_target_fixture():
    """
    Patch get_target
    """
    with patch("featurebyte.service.feature_table_cache.get_target") as mock:
        yield mock


@pytest_asyncio.fixture(name="observation_table")
async def observation_table_fixture(event_table, user, observation_table_service):
    """Observation table fixture"""
    request_input = SourceTableRequestInput(source=event_table.tabular_source)
    location = TabularSource(
        **{
            "feature_store_id": event_table.tabular_source.feature_store_id,
            "table_details": {
                "database_name": "fb_database",
                "schema_name": "fb_schema",
                "table_name": "fb_materialized_table",
            },
        }
    )
    observation_table = ObservationTableModel(
        name="observation_table_from_source_table",
        location=location,
        request_input=request_input,
        columns_info=[
            {"name": "cust_id", "dtype": "INT"},
            {"name": "POINT_IN_TIME", "dtype": "TIMESTAMP"},
        ],
        num_rows=1000,
        most_recent_point_in_time="2023-01-15T10:00:00",
        user_id=user.id,
        has_row_index=True,
    )
    return await observation_table_service.create_document(observation_table)


@pytest_asyncio.fixture(name="features")
async def features_fixture(event_table, entity, feature_service, test_dir):
    """Fixture to create features"""
    _ = entity, event_table

    features = []
    for file_name in ["feature_sum_30m.json", "feature_sum_2h.json"]:
        fixture_path = os.path.join(test_dir, "fixtures/request_payloads", file_name)
        with open(fixture_path, encoding="utf") as fhandle:
            payload = json.loads(fhandle.read())
        # Simulate the actual feature saving process
        sanitized_document = await feature_service.prepare_feature_model(
            data=FeatureServiceCreate(**payload),
            sanitize_for_definition=True,
        )
        feature = await feature_service.create_document(
            data=FeatureServiceCreate(**sanitized_document.dict(by_alias=True))
        )
        features.append(feature)
    return features


@pytest_asyncio.fixture(name="production_ready_features")
async def production_ready_features_fixture(features, feature_readiness_service):
    """Fixture to create prodiction ready feaures"""
    prod_features = []
    for feature in features:
        prod_feature = await feature_readiness_service.update_feature(
            feature.id, readiness="PRODUCTION_READY", ignore_guardrails=True
        )
        assert prod_feature.readiness == "PRODUCTION_READY"
        prod_features.append(prod_feature)
    return prod_features


@pytest_asyncio.fixture(name="regular_feature_list")
async def regular_feature_list_fixture(features, feature_list_service):
    """Fixture to create feature list"""
    data = FeatureListServiceCreate(
        name="My Feature List",
        feature_ids=[feature.id for feature in features],
    )
    result = await feature_list_service.create_document(data)
    return result


@pytest_asyncio.fixture(name="deployed_feature_list")
async def deployed_feature_list_fixture(
    production_ready_features, feature_list_service, deploy_service
):
    """Fixture to create deployed feature list"""
    data = FeatureListServiceCreate(
        name="My Deployed Feature List",
        feature_ids=[feature.id for feature in production_ready_features],
    )
    result = await feature_list_service.create_document(data)

    await deploy_service.create_deployment(
        feature_list_id=result.id,
        deployment_id=ObjectId(),
        deployment_name="my-test-deployment",
        to_enable_deployment=True,
    )
    return await feature_list_service.get_document(document_id=result.id)


@pytest.fixture(name="feature_list", params=["regular_feature_list", "deployed_feature_list"])
def feature_list_fixture(request):
    """Feature list fixture"""
    return request.getfixturevalue(request.param)


@pytest.fixture(name="intercepted_definition_hashes_for_nodes")
def intercepted_definition_hashes_for_nodes_fixture(feature_table_cache_service):
    """
    Fixture for intercepted definition_hashes_for_nodes method
    """
    original_method = feature_table_cache_service.definition_hashes_for_nodes

    async def intercepted_method(*args, **kwargs):
        return await original_method(*args, **kwargs)

    with patch.object(
        feature_table_cache_service,
        "definition_hashes_for_nodes",
        side_effect=intercepted_method,
    ) as mocked_method:
        yield mocked_method


@pytest.fixture(name="mock_snowflake_session")
def mock_snowflake_session_fixture(mock_snowflake_session, feature_table_cache_metadata_service):
    """
    Patch session query results
    """

    async def mock_execute_query(query):
        if "LIMIT 1" in query:
            table_name = parse_one(query).args["from"].expressions[0].name
            async for doc in feature_table_cache_metadata_service.list_documents_iterator(
                query_filter={"table_name": table_name}
            ):
                if len(doc.feature_definitions) > 0:
                    return
            raise ProgrammingError("table not found")

    mock_snowflake_session.execute_query.side_effect = mock_execute_query

    yield mock_snowflake_session


@pytest.mark.parametrize("feature_list_id_provided", [True, False])
@pytest.mark.asyncio
async def test_get_feature_definition_hashes(
    feature_list_id_provided,
    feature_table_cache_service,
    regular_feature_list,
    intercepted_definition_hashes_for_nodes,
):
    """Test get_feature_definition_hashes"""
    hashes = await feature_table_cache_service.get_feature_definition_hashes(
        graph=regular_feature_list.feature_clusters[0].graph,
        nodes=regular_feature_list.feature_clusters[0].nodes,
        **{"feature_list_id": regular_feature_list.id if feature_list_id_provided else None},
    )
    if feature_list_id_provided:
        assert intercepted_definition_hashes_for_nodes.call_count == 0
    else:
        assert intercepted_definition_hashes_for_nodes.call_count == 1
    expected = {
        "1032f6901100176e575f87c44398a81f0d5db5c5",
        "ada88371db4be31a4e9c0538fb675d8e573aed24",
    }
    assert set(hashes) == expected


@pytest.mark.asyncio
async def test_create_feature_table_cache(
    feature_store,
    feature_table_cache_service,
    feature_table_cache_metadata_service,
    observation_table,
    feature_list,
    mock_get_historical_features,
    mock_snowflake_session,
    intercepted_definition_hashes_for_nodes,
):
    """Test create feature table cache from scratch"""
    await feature_table_cache_service.create_or_update_feature_table_cache(
        feature_store=feature_store,
        observation_table=observation_table,
        graph=feature_list.feature_clusters[0].graph,
        nodes=feature_list.feature_clusters[0].nodes,
        feature_list_id=feature_list.id,
    )
    assert intercepted_definition_hashes_for_nodes.call_count == 0
    assert mock_get_historical_features.await_count == 1

    params = mock_get_historical_features.await_args.kwargs
    assert params["graph"] == feature_list.feature_clusters[0].graph
    assert params["nodes"] == feature_list.feature_clusters[0].nodes
    assert params["output_table_details"].database_name == "sf_db"
    assert params["output_table_details"].schema_name == "sf_schema"
    assert params["output_table_details"].table_name == "__TEMP__FEATURE_TABLE_CACHE_ObjectId"
    assert params["is_feature_list_deployed"] == feature_list.deployed

    assert mock_snowflake_session.execute_query.await_count == 2

    feature_table_cache = (
        await feature_table_cache_metadata_service.get_or_create_feature_table_cache(
            observation_table_id=observation_table.id,
        )
    )
    assert len(feature_table_cache.feature_definitions) == 2

    sql = mock_snowflake_session.execute_query.await_args_list[0].args[0]
    assert "COUNT(*)" in sql

    sql = mock_snowflake_session.execute_query.await_args_list[1].args[0]
    assert sql == (
        "CREATE TABLE "
        f'"sf_db"."sf_schema"."{feature_table_cache.table_name}" AS\n'
        "SELECT\n"
        '  "__FB_TABLE_ROW_INDEX",\n'
        '  "cust_id",\n'
        '  "POINT_IN_TIME",\n'
        '  "sum_30m" AS "FEATURE_1032f6901100176e575f87c44398a81f0d5db5c5",\n'
        '  "sum_2h" AS "FEATURE_ada88371db4be31a4e9c0538fb675d8e573aed24"\n'
        'FROM "__TEMP__FEATURE_TABLE_CACHE_ObjectId"'
    )


@pytest.mark.asyncio
async def test_update_feature_table_cache(
    feature_store,
    feature_table_cache_service,
    feature_table_cache_metadata_service,
    observation_table,
    feature_list,
    mock_get_historical_features,
    mock_snowflake_session,
):
    """Test update feature table cache non deployed feature list"""
    # create feature table cache
    await feature_table_cache_service.create_or_update_feature_table_cache(
        feature_store=feature_store,
        observation_table=observation_table,
        graph=feature_list.feature_clusters[0].graph,
        nodes=feature_list.feature_clusters[0].nodes[:1],
        feature_list_id=feature_list.id,
    )
    assert mock_get_historical_features.await_count == 1

    params = mock_get_historical_features.await_args.kwargs
    assert params["graph"] == feature_list.feature_clusters[0].graph
    assert params["nodes"] == feature_list.feature_clusters[0].nodes[:1]
    assert mock_snowflake_session.execute_query.await_count == 2

    feature_table_cache = (
        await feature_table_cache_metadata_service.get_or_create_feature_table_cache(
            observation_table_id=observation_table.id,
        )
    )
    assert len(feature_table_cache.feature_definitions) == 1

    sql = mock_snowflake_session.execute_query.await_args_list[0].args[0]
    assert "COUNT(*)" in sql

    # check first create sql
    sql = mock_snowflake_session.execute_query.await_args.args[0]
    assert sql == (
        "CREATE TABLE "
        f'"sf_db"."sf_schema"."{feature_table_cache.table_name}" AS\n'
        "SELECT\n"
        '  "__FB_TABLE_ROW_INDEX",\n'
        '  "cust_id",\n'
        '  "POINT_IN_TIME",\n'
        '  "sum_30m" AS "FEATURE_1032f6901100176e575f87c44398a81f0d5db5c5"\n'
        'FROM "__TEMP__FEATURE_TABLE_CACHE_ObjectId"'
    )

    mock_get_historical_features.reset_mock()
    mock_snowflake_session.reset_mock()

    # update feature table cache by adding one new feature
    await feature_table_cache_service.create_or_update_feature_table_cache(
        feature_store=feature_store,
        observation_table=observation_table,
        graph=feature_list.feature_clusters[0].graph,
        nodes=feature_list.feature_clusters[0].nodes[1:],
        feature_list_id=feature_list.id,
    )
    assert mock_get_historical_features.await_count == 1

    params = mock_get_historical_features.await_args.kwargs
    assert params["graph"] == feature_list.feature_clusters[0].graph
    assert params["nodes"] == feature_list.feature_clusters[0].nodes[1:]
    assert params["output_table_details"].database_name == "sf_db"
    assert params["output_table_details"].schema_name == "sf_schema"
    assert params["output_table_details"].table_name == "__TEMP__FEATURE_TABLE_CACHE_ObjectId"
    assert params["is_feature_list_deployed"] == feature_list.deployed

    assert mock_snowflake_session.execute_query.await_count == 3

    feature_table_cache = (
        await feature_table_cache_metadata_service.get_or_create_feature_table_cache(
            observation_table_id=observation_table.id,
        )
    )
    assert len(feature_table_cache.feature_definitions) == 2

    call_args = mock_snowflake_session.execute_query.await_args_list
    sqls = [arg[0][0] for arg in call_args]

    assert (
        sqls[0]
        == textwrap.dedent(
            f"""
        SELECT
          COUNT(*)
        FROM "{feature_table_cache.table_name}"
        LIMIT 1
        """
        ).strip()
    )
    assert sqls[1] == (
        "ALTER TABLE "
        f'"sf_db"."sf_schema"."{feature_table_cache.table_name}" ADD '
        'COLUMN "FEATURE_ada88371db4be31a4e9c0538fb675d8e573aed24" FLOAT'
    )
    assert sqls[2] == (
        "MERGE INTO "
        f'"sf_db"."sf_schema"."{feature_table_cache.table_name}" AS '
        "feature_table_cache USING "
        '"sf_db"."sf_schema"."__TEMP__FEATURE_TABLE_CACHE_ObjectId" AS '
        'partial_features ON feature_table_cache."__FB_TABLE_ROW_INDEX" = '
        'partial_features."__FB_TABLE_ROW_INDEX"   WHEN MATCHED THEN UPDATE SET '
        'feature_table_cache."FEATURE_ada88371db4be31a4e9c0538fb675d8e573aed24" = '
        'partial_features."sum_2h"'
    )


@pytest.mark.asyncio
async def test_update_feature_table_cache__mix_cached_and_non_cached_features(
    feature_store,
    feature_table_cache_service,
    feature_table_cache_metadata_service,
    observation_table,
    feature_list,
    mock_get_historical_features,
    mock_snowflake_session,
):
    """Test update feature table cache non deployed feature list"""
    # create feature table cache
    await feature_table_cache_service.create_or_update_feature_table_cache(
        feature_store=feature_store,
        observation_table=observation_table,
        graph=feature_list.feature_clusters[0].graph,
        nodes=feature_list.feature_clusters[0].nodes[:1],
        feature_list_id=feature_list.id,
    )
    assert mock_get_historical_features.await_count == 1

    feature_table_cache = (
        await feature_table_cache_metadata_service.get_or_create_feature_table_cache(
            observation_table_id=observation_table.id,
        )
    )
    assert len(feature_table_cache.feature_definitions) == 1

    mock_get_historical_features.reset_mock()
    mock_snowflake_session.reset_mock()

    # update feature table cache by adding one new feature and one cached feature
    await feature_table_cache_service.create_or_update_feature_table_cache(
        feature_store=feature_store,
        observation_table=observation_table,
        graph=feature_list.feature_clusters[0].graph,
        nodes=feature_list.feature_clusters[0].nodes,
        feature_list_id=feature_list.id,
    )
    assert mock_get_historical_features.await_count == 1

    params = mock_get_historical_features.await_args.kwargs
    assert params["graph"] == feature_list.feature_clusters[0].graph
    assert params["nodes"] == feature_list.feature_clusters[0].nodes[1:]
    assert params["output_table_details"].database_name == "sf_db"
    assert params["output_table_details"].schema_name == "sf_schema"
    assert params["output_table_details"].table_name == "__TEMP__FEATURE_TABLE_CACHE_ObjectId"
    assert params["is_feature_list_deployed"] == feature_list.deployed

    assert mock_snowflake_session.execute_query.await_count == 3

    feature_table_cache = (
        await feature_table_cache_metadata_service.get_or_create_feature_table_cache(
            observation_table_id=observation_table.id,
        )
    )
    assert len(feature_table_cache.feature_definitions) == 2

    call_args = mock_snowflake_session.execute_query.await_args_list
    sqls = [arg[0][0] for arg in call_args]

    assert (
        sqls[0]
        == textwrap.dedent(
            f"""
        SELECT
          COUNT(*)
        FROM "{feature_table_cache.table_name}"
        LIMIT 1
        """
        ).strip()
    )
    assert sqls[1] == (
        "ALTER TABLE "
        f'"sf_db"."sf_schema"."{feature_table_cache.table_name}" ADD '
        'COLUMN "FEATURE_ada88371db4be31a4e9c0538fb675d8e573aed24" FLOAT'
    )
    assert sqls[2] == (
        "MERGE INTO "
        f'"sf_db"."sf_schema"."{feature_table_cache.table_name}" AS '
        "feature_table_cache USING "
        '"sf_db"."sf_schema"."__TEMP__FEATURE_TABLE_CACHE_ObjectId" AS '
        'partial_features ON feature_table_cache."__FB_TABLE_ROW_INDEX" = '
        'partial_features."__FB_TABLE_ROW_INDEX"   WHEN MATCHED THEN UPDATE SET '
        'feature_table_cache."FEATURE_ada88371db4be31a4e9c0538fb675d8e573aed24" = '
        'partial_features."sum_2h"'
    )


@pytest.mark.asyncio
async def test_create_view_from_cache__create_cache(
    feature_store,
    feature_table_cache_service,
    feature_table_cache_metadata_service,
    observation_table,
    feature_list,
    mock_get_historical_features,
    mock_snowflake_session,
):
    """Test create feature table cache from scratch"""
    output_view_details = TableDetails(
        database_name=mock_snowflake_session.database_name,
        schema_name=mock_snowflake_session.schema_name,
        table_name="result_view",
    )
    await feature_table_cache_service.create_view_from_cache(
        feature_store=feature_store,
        observation_table=observation_table,
        graph=feature_list.feature_clusters[0].graph,
        nodes=feature_list.feature_clusters[0].nodes,
        output_view_details=output_view_details,
        is_target=False,
        feature_list_id=feature_list.id,
    )

    assert mock_get_historical_features.await_count == 1
    assert mock_snowflake_session.execute_query.await_count == 3

    feature_table_cache = (
        await feature_table_cache_metadata_service.get_or_create_feature_table_cache(
            observation_table_id=observation_table.id,
        )
    )

    call_args = mock_snowflake_session.execute_query.await_args_list
    sqls = [arg[0][0] for arg in call_args]

    assert (
        sqls[0]
        == textwrap.dedent(
            f"""
        SELECT
          COUNT(*)
        FROM "{feature_table_cache.table_name}"
        LIMIT 1
        """
        ).strip()
    )
    assert sqls[1] == (
        "CREATE TABLE "
        f'"sf_db"."sf_schema"."{feature_table_cache.table_name}" AS\n'
        "SELECT\n"
        '  "__FB_TABLE_ROW_INDEX",\n'
        '  "cust_id",\n'
        '  "POINT_IN_TIME",\n'
        '  "sum_30m" AS "FEATURE_1032f6901100176e575f87c44398a81f0d5db5c5",\n'
        '  "sum_2h" AS "FEATURE_ada88371db4be31a4e9c0538fb675d8e573aed24"\n'
        'FROM "__TEMP__FEATURE_TABLE_CACHE_ObjectId"'
    )
    assert sqls[2] == (
        'CREATE VIEW "sf_db"."sf_schema"."result_view" AS\n'
        "SELECT\n"
        '  "__FB_TABLE_ROW_INDEX",\n'
        '  "cust_id",\n'
        '  "POINT_IN_TIME",\n'
        '  "FEATURE_1032f6901100176e575f87c44398a81f0d5db5c5" AS "sum_30m",\n'
        '  "FEATURE_ada88371db4be31a4e9c0538fb675d8e573aed24" AS "sum_2h"\n'
        f'FROM "sf_db"."sf_schema"."{feature_table_cache.table_name}"'
    )


@pytest.mark.asyncio
async def test_create_view_from_cache__update_cache(
    feature_store,
    feature_table_cache_service,
    feature_table_cache_metadata_service,
    observation_table,
    feature_list,
    mock_get_historical_features,
    mock_snowflake_session,
):
    """Test update feature table cache non deployed feature list"""
    # create feature table cache
    output_view_details = TableDetails(
        database_name=mock_snowflake_session.database_name,
        schema_name=mock_snowflake_session.schema_name,
        table_name="result_view",
    )
    await feature_table_cache_service.create_view_from_cache(
        feature_store=feature_store,
        observation_table=observation_table,
        graph=feature_list.feature_clusters[0].graph,
        nodes=feature_list.feature_clusters[0].nodes[:1],
        output_view_details=output_view_details,
        is_target=False,
        feature_list_id=feature_list.id,
    )
    assert mock_get_historical_features.await_count == 1
    assert mock_snowflake_session.execute_query.await_count == 3

    mock_get_historical_features.reset_mock()
    mock_snowflake_session.reset_mock()

    # update feature table cache
    await feature_table_cache_service.create_view_from_cache(
        feature_store=feature_store,
        observation_table=observation_table,
        graph=feature_list.feature_clusters[0].graph,
        nodes=feature_list.feature_clusters[0].nodes,
        output_view_details=output_view_details,
        is_target=False,
        feature_list_id=feature_list.id,
    )
    assert mock_get_historical_features.await_count == 1
    assert mock_snowflake_session.execute_query.await_count == 4

    feature_table_cache = (
        await feature_table_cache_metadata_service.get_or_create_feature_table_cache(
            observation_table_id=observation_table.id,
        )
    )

    call_args = mock_snowflake_session.execute_query.await_args_list
    sqls = [arg[0][0] for arg in call_args]
    assert len(sqls) == 4

    assert (
        sqls[0]
        == textwrap.dedent(
            f"""
        SELECT
          COUNT(*)
        FROM "{feature_table_cache.table_name}"
        LIMIT 1
        """
        ).strip()
    )
    assert sqls[1] == (
        "ALTER TABLE "
        f'"sf_db"."sf_schema"."{feature_table_cache.table_name}" ADD '
        'COLUMN "FEATURE_ada88371db4be31a4e9c0538fb675d8e573aed24" FLOAT'
    )
    assert sqls[2] == (
        "MERGE INTO "
        f'"sf_db"."sf_schema"."{feature_table_cache.table_name}" AS '
        "feature_table_cache USING "
        '"sf_db"."sf_schema"."__TEMP__FEATURE_TABLE_CACHE_ObjectId" AS '
        'partial_features ON feature_table_cache."__FB_TABLE_ROW_INDEX" = '
        'partial_features."__FB_TABLE_ROW_INDEX"   WHEN MATCHED THEN UPDATE SET '
        'feature_table_cache."FEATURE_ada88371db4be31a4e9c0538fb675d8e573aed24" = '
        'partial_features."sum_2h"'
    )
    assert sqls[3] == (
        'CREATE VIEW "sf_db"."sf_schema"."result_view" AS\n'
        "SELECT\n"
        '  "__FB_TABLE_ROW_INDEX",\n'
        '  "cust_id",\n'
        '  "POINT_IN_TIME",\n'
        '  "FEATURE_1032f6901100176e575f87c44398a81f0d5db5c5" AS "sum_30m",\n'
        '  "FEATURE_ada88371db4be31a4e9c0538fb675d8e573aed24" AS "sum_2h"\n'
        f'FROM "sf_db"."sf_schema"."{feature_table_cache.table_name}"'
    )


@pytest.mark.asyncio
async def test_create_feature_table_cache__with_target(
    feature_store,
    feature_table_cache_service,
    feature_table_cache_metadata_service,
    observation_table,
    target,
    mock_get_target,
    mock_snowflake_session,
):
    """Test create feature table cache from scratch"""
    await feature_table_cache_service.create_or_update_feature_table_cache(
        feature_store=feature_store,
        observation_table=observation_table,
        graph=target.graph,
        nodes=[target.node],
        is_target=True,
    )
    assert mock_get_target.await_count == 1

    params = mock_get_target.await_args.kwargs
    assert params["graph"] == target.graph
    assert params["nodes"] == [target.node]
    assert params["output_table_details"].database_name == "sf_db"
    assert params["output_table_details"].schema_name == "sf_schema"
    assert params["output_table_details"].table_name == "__TEMP__FEATURE_TABLE_CACHE_ObjectId"

    assert mock_snowflake_session.execute_query.await_count == 2

    feature_table_cache = (
        await feature_table_cache_metadata_service.get_or_create_feature_table_cache(
            observation_table_id=observation_table.id,
        )
    )
    assert len(feature_table_cache.feature_definitions) == 1

    sql = mock_snowflake_session.execute_query.await_args.args[0]
    assert sql == (
        "CREATE TABLE "
        f'"sf_db"."sf_schema"."{feature_table_cache.table_name}" AS\n'
        "SELECT\n"
        '  "__FB_TABLE_ROW_INDEX",\n'
        '  "cust_id",\n'
        '  "POINT_IN_TIME",\n'
        '  "float_target" AS "FEATURE_dfbe0388b776a0bd4e78b6e538889b9b9eea7bb1"\n'
        'FROM "__TEMP__FEATURE_TABLE_CACHE_ObjectId"'
    )


@pytest.mark.asyncio
async def test_read_from_cache(
    feature_store,
    feature_table_cache_service,
    feature_table_cache_metadata_service,
    observation_table,
    feature_list,
    mock_get_historical_features,
    mock_snowflake_session,
):
    """Test read data from table cache"""
    await feature_table_cache_service.create_or_update_feature_table_cache(
        feature_store=feature_store,
        observation_table=observation_table,
        graph=feature_list.feature_clusters[0].graph,
        nodes=feature_list.feature_clusters[0].nodes,
        feature_list_id=feature_list.id,
    )
    assert mock_get_historical_features.await_count == 1
    assert mock_snowflake_session.execute_query.await_count == 2

    mock_snowflake_session.reset_mock()

    await feature_table_cache_service.read_from_cache(
        feature_store=feature_store,
        observation_table=observation_table,
        graph=feature_list.feature_clusters[0].graph,
        nodes=feature_list.feature_clusters[0].nodes,
    )
    assert mock_snowflake_session.execute_query.await_count == 1

    feature_table_cache = (
        await feature_table_cache_metadata_service.get_or_create_feature_table_cache(
            observation_table_id=observation_table.id,
        )
    )

    call_args = mock_snowflake_session.execute_query.await_args_list
    sqls = [arg[0][0] for arg in call_args]

    assert sqls[0] == (
        "SELECT\n"
        '  "__FB_TABLE_ROW_INDEX",\n'
        '  "FEATURE_1032f6901100176e575f87c44398a81f0d5db5c5" AS "sum_30m",\n'
        '  "FEATURE_ada88371db4be31a4e9c0538fb675d8e573aed24" AS "sum_2h"\n'
        f'FROM "sf_db"."sf_schema"."{feature_table_cache.table_name}"'
    )
