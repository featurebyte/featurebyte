from dataclasses import dataclass
from http import HTTPStatus

import pandas as pd
import pytest
from bson import ObjectId

import featurebyte as fb
from featurebyte.enum import DBVarType, TargetType
from featurebyte.models.deployment_sql import DeploymentSqlModel
from featurebyte.query_graph.sql.ast.literal import make_literal_value
from featurebyte.query_graph.sql.common import sql_to_string
from tests.util.helper import fb_assert_frame_equal


@dataclass
class DeploymentSqlTestCase:
    """
    Deployment SQL test case
    """

    feature_list: fb.FeatureList
    deployment_sql: DeploymentSqlModel
    point_in_time: str
    expected_column_names: list[str]


def make_unique(name):
    """
    Make name unique by appending ObjectId
    """
    return f"{name}_{str(ObjectId())}"


def get_deployment_sql(client, deployment: fb.Deployment) -> DeploymentSqlModel:
    """
    Get deployment SQL
    """
    task_response = client.post("/deployment_sql", json={"deployment_id": str(deployment.id)})
    assert task_response.status_code == HTTPStatus.CREATED, task_response.text
    task_response_dict = task_response.json()
    if task_response_dict["status"] != "SUCCESS":
        traceback = task_response_dict.get("traceback")
        raise AssertionError(
            f"Deployment SQL generation task did not succeed, traceback:\n{traceback}"
        )
    output_path = task_response_dict["payload"]["task_output_path"]
    response = client.get(output_path)
    assert response.status_code == HTTPStatus.OK, response.text
    deployment_sql = DeploymentSqlModel(**response.json())
    return deployment_sql


@pytest.fixture
def event_table_feature_test_case(client, event_table, user_entity):
    """
    Simple event table feature
    """
    event_view = event_table.get_view()
    feature_name = make_unique("event_count_7d")
    feature = event_view.groupby("ÜSER ID").aggregate_over(
        value_column=None,
        method="count",
        windows=["7d"],
        feature_names=[feature_name],
        feature_job_setting=fb.CronFeatureJobSetting(
            crontab="0 10 * * *",
            timezone="UTC",
            blind_spot="1h",
        ),
    )[feature_name]
    feature_list = fb.FeatureList([feature], name=feature_name)
    feature_list.save()
    deployment = feature_list.deploy()
    deployment_sql = get_deployment_sql(client, deployment)
    return DeploymentSqlTestCase(
        feature_list=feature_list,
        deployment_sql=deployment_sql,
        point_in_time="2001-01-15 10:00:00",
        expected_column_names=["POINT_IN_TIME", user_entity.serving_names[0], feature_name],
    )


@pytest.fixture
def user_feature_served_via_transaction_test_case(client, event_table, order_entity):
    """
    User feature as a parent feature served via transaction entity
    """
    event_view = event_table.get_view()
    feature_name = make_unique("event_count_7d")
    feature = event_view.groupby("ÜSER ID").aggregate_over(
        value_column=None,
        method="count",
        windows=["7d"],
        feature_names=[feature_name],
        feature_job_setting=fb.CronFeatureJobSetting(
            crontab="0 10 * * *",
            timezone="UTC",
            blind_spot="1h",
        ),
    )[feature_name]
    feature_list = fb.FeatureList([feature], name=feature_name)
    feature_list.save()
    target = fb.TargetNamespace.create(
        name=make_unique("transaction_target"),
        primary_entity=[order_entity.name],
        dtype=DBVarType.FLOAT,
        target_type=TargetType.REGRESSION,
    )
    context = fb.Context.create(
        name=make_unique("transaction_context"), primary_entity=[order_entity.name]
    )
    use_case = fb.UseCase.create(
        name=make_unique("transaction_use_case"),
        target_name=target.name,
        context_name=context.name,
    )
    deployment = feature_list.deploy(use_case_name=use_case.name)
    deployment_sql = get_deployment_sql(client, deployment)
    return DeploymentSqlTestCase(
        feature_list=feature_list,
        deployment_sql=deployment_sql,
        point_in_time="2001-01-15 10:00:00",
        expected_column_names=["POINT_IN_TIME", order_entity.serving_names[0], feature_name],
    )


@pytest.fixture
def time_series_table_feature_test_case(client, time_series_table, series_entity):
    """
    Simple time series table feature
    """
    view = time_series_table.get_view()
    feature_name = make_unique("value_col_sum_7d_offset_1d")
    feature = view.groupby("series_id_col").aggregate_over(
        value_column="value_col",
        method="sum",
        windows=[fb.CalendarWindow(unit="DAY", size=7)],
        offset=fb.CalendarWindow(unit="DAY", size=1),
        feature_names=[feature_name],
        feature_job_setting=fb.CronFeatureJobSetting(
            crontab="0 8 * * *",
            timezone="Asia/Singapore",
        ),
    )[feature_name]
    feature_list = fb.FeatureList([feature], name=feature_name)
    feature_list.save()
    deployment = feature_list.deploy()
    deployment_sql = get_deployment_sql(client, deployment)
    return DeploymentSqlTestCase(
        feature_list=feature_list,
        deployment_sql=deployment_sql,
        point_in_time="2001-01-10 10:00:00",
        expected_column_names=["POINT_IN_TIME", series_entity.serving_names[0], feature_name],
    )


@pytest.fixture
def snapshots_lookup_feature_test_case(client, snapshots_table, series_entity):
    """
    Simple snapshots table lookup feature
    """
    view = snapshots_table.get_view()
    feature_name = make_unique("snapshot_lookup_feature")
    feature = view["value_col"].as_feature(feature_name)
    feature_list = fb.FeatureList([feature], name=feature_name)
    feature_list.save()
    deployment = feature_list.deploy()
    deployment_sql = get_deployment_sql(client, deployment)
    return DeploymentSqlTestCase(
        feature_list=feature_list,
        deployment_sql=deployment_sql,
        point_in_time="2001-01-10 10:00:00",
        expected_column_names=["POINT_IN_TIME", series_entity.serving_names[0], feature_name],
    )


@pytest.fixture
def scd_lookup_feature_test_case(client, scd_table, user_entity):
    """
    SCD lookup feature test case
    """
    scd_view = scd_table.get_view()
    feature_name = make_unique("some_lookup_feature")
    feature = scd_view["ID"].as_feature(feature_name)
    feature_list = fb.FeatureList([feature], name=feature_name)
    feature_list.save()
    deployment = feature_list.deploy(make_production_ready=True)
    deployment.enable()
    deployment_sql = get_deployment_sql(client, deployment)
    return DeploymentSqlTestCase(
        feature_list=feature_list,
        deployment_sql=deployment_sql,
        point_in_time="2001-01-15 10:00:00",
        expected_column_names=["POINT_IN_TIME", user_entity.serving_names[0], feature_name],
    )


@pytest.fixture
def time_since_last_event_feature_test_case(client, event_table, user_entity):
    """
    Time since last event feature
    """
    event_view = event_table.get_view()
    feature_name = make_unique("days_since_last_event_7d")
    feature = event_view.groupby("ÜSER ID").aggregate_over(
        value_column="ËVENT_TIMESTAMP",
        method="latest",
        windows=["7d"],
        feature_names=[feature_name],
        feature_job_setting=fb.CronFeatureJobSetting(
            crontab="0 10 * * *",
            timezone="UTC",
            blind_spot="1h",
        ),
    )[feature_name]
    feature = (fb.RequestColumn.point_in_time() - feature).dt.day
    feature.name = feature_name
    feature_list = fb.FeatureList([feature], name=feature_name)
    feature_list.save()
    deployment = feature_list.deploy()
    deployment_sql = get_deployment_sql(client, deployment)
    return DeploymentSqlTestCase(
        feature_list=feature_list,
        deployment_sql=deployment_sql,
        point_in_time="2001-01-15 10:00:00",
        expected_column_names=["POINT_IN_TIME", user_entity.serving_names[0], feature_name],
    )


@pytest.fixture
def internal_parent_child_relationship_feature_test_case(client, scd_table, user_entity):
    """
    Time since last event feature
    """
    feature_name = make_unique("complex_parent_child_feature")
    scd_view = scd_table.get_view()
    feature_user_id = scd_view["ID"].as_feature("some_lookup_feature")
    feature_user_id_parent = scd_view.groupby("User Status").aggregate_asat(
        value_column=None,
        method="count",
        feature_name="asat_gender_count",
    )
    feature = feature_user_id + feature_user_id_parent
    feature.name = feature_name
    feature_list = fb.FeatureList([feature], name=feature_name)
    feature_list.save()
    deployment = feature_list.deploy(make_production_ready=True)
    deployment.enable()
    deployment_sql = get_deployment_sql(client, deployment)
    return DeploymentSqlTestCase(
        feature_list=feature_list,
        deployment_sql=deployment_sql,
        point_in_time="2001-01-15 10:00:00",
        expected_column_names=["POINT_IN_TIME", user_entity.serving_names[0], feature_name],
    )


def process_sql(session, sql_code, point_in_time):
    """
    Replace placeholders in SQL code to make it executable
    """
    point_in_time_str = sql_to_string(
        make_literal_value(pd.to_datetime(point_in_time).to_pydatetime(), cast_as_timestamp=True),
        source_type=session.source_type,
    )
    query = sql_code.replace("{{ CURRENT_TIMESTAMP }}", point_in_time_str)
    return query


async def check_deployment_sql(session, test_case):
    """
    Check deployment SQL
    """
    for feature_table_sql in test_case.deployment_sql.feature_table_sqls:
        query = process_sql(
            session=session,
            sql_code=feature_table_sql.sql_code,
            point_in_time=test_case.point_in_time,
        )
        try:
            df = await session.execute_query_long_running(query)
        except Exception as e:
            raise AssertionError("Query execution failed") from e

        assert df.shape[0] > 0, "No data returned from deployment SQL"

        # Check expected columns
        assert sorted(test_case.expected_column_names) == sorted(df.columns)

        # Compute expected values using a temporary feature list via historical features
        non_feature_columns = [
            col for col in df.columns if col not in feature_table_sql.feature_names
        ]
        assert len(non_feature_columns) > 0
        df_request = df[non_feature_columns]
        temp_feature_list = fb.FeatureList(
            [fb.Feature.get(feature_name) for feature_name in feature_table_sql.feature_names],
            name=make_unique("temp_feature_list"),
        )
        temp_feature_list.save()
        df_historical = temp_feature_list.compute_historical_features(df_request)

        # Check equality
        df_historical = df_historical[df.columns]
        df.sort_values(non_feature_columns, inplace=True)
        df_historical.sort_values(non_feature_columns, inplace=True)
        fb_assert_frame_equal(df, df_historical, sort_by_columns=non_feature_columns)


@pytest.mark.parametrize(
    "test_case_name",
    [
        "event_table_feature_test_case",
        "time_series_table_feature_test_case",
        "snapshots_lookup_feature_test_case",
        "scd_lookup_feature_test_case",
        "time_since_last_event_feature_test_case",
        "user_feature_served_via_transaction_test_case",
        "internal_parent_child_relationship_feature_test_case",
    ],
)
@pytest.mark.asyncio
async def test_deployment_sql(session, test_case_name, request):
    """
    Test deployment SQL for simple event table feature
    """
    test_case = request.getfixturevalue(test_case_name)
    await check_deployment_sql(session, test_case)
