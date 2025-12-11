"""
Test module for generating payload fixtures for testing api route
"""

import json
from typing import Any

import pytest
from bson import ObjectId

from featurebyte import (
    AggFunc,
    AssignmentDesign,
    AssignmentSource,
    Configurations,
    FeatureJobSetting,
    FeatureList,
    Propensity,
    TreatmentInterference,
    TreatmentTime,
    TreatmentTimeStructure,
    TreatmentType,
)
from featurebyte.enum import DBVarType
from featurebyte.models.batch_request_table import (
    ManagedViewBatchRequestInput,
    SourceTableBatchRequestInput,
)
from featurebyte.models.credential import UsernamePasswordCredential
from featurebyte.models.observation_table import (
    ManagedViewObservationInput,
    ObservationTableObservationInput,
    SourceTableObservationInput,
)
from featurebyte.models.relationship import RelationshipType
from featurebyte.models.static_source_table import SourceTableStaticSourceInput
from featurebyte.models.user_defined_function import FunctionParameter
from featurebyte.query_graph.graph import QueryGraph
from featurebyte.query_graph.model.column_info import ColumnInfo
from featurebyte.query_graph.model.common_table import TabularSource
from featurebyte.query_graph.node.schema import TableDetails
from featurebyte.schema.batch_feature_table import BatchFeatureTableCreate
from featurebyte.schema.batch_request_table import BatchRequestTableCreate
from featurebyte.schema.catalog import CatalogCreate
from featurebyte.schema.context import ContextCreate
from featurebyte.schema.credential import CredentialCreate
from featurebyte.schema.deployment import DeploymentCreate
from featurebyte.schema.development_dataset import DevelopmentDatasetCreate, DevelopmentTableCreate
from featurebyte.schema.feature_job_setting_analysis import FeatureJobSettingAnalysisCreate
from featurebyte.schema.feature_list import FeatureListGetHistoricalFeatures
from featurebyte.schema.feature_store import FeatureStoreSample
from featurebyte.schema.historical_feature_table import HistoricalFeatureTableCreate
from featurebyte.schema.managed_view import ManagedViewCreate
from featurebyte.schema.observation_table import ObservationTableCreate
from featurebyte.schema.online_store import OnlineStoreCreate
from featurebyte.schema.relationship_info import RelationshipInfoCreate
from featurebyte.schema.static_source_table import StaticSourceTableCreate
from featurebyte.schema.target_namespace import TargetNamespaceCreate
from featurebyte.schema.target_table import TargetTableCreate
from featurebyte.schema.treatment import TreatmentCreate
from featurebyte.schema.user_defined_function import UserDefinedFunctionCreate
from tests.util.helper import iet_entropy


@pytest.fixture(name="request_payload_dir")
def request_payload_dir_fixture():
    """Request payload directory fixture"""
    return "tests/fixtures/request_payloads"


@pytest.fixture(name="reset_configurations")
def reset_configurations_fixture():
    """
    This is required becuase test_config.py sets a global state
    """
    config = Configurations(force=True)
    config.use_profile("local")


def replace_obj_id(obj: Any, obj_id: ObjectId) -> Any:
    """
    Helper function to replace the object ID of the type
    """
    params = obj.model_dump()
    params["_id"] = obj_id
    return type(obj)(**params)


def update_or_check_payload_fixture(request_payload_dir, name, json_payload, update_fixtures):
    """
    Helper function to either update a fixture or check that it is up to date
    """
    filename = f"{request_payload_dir}/{name}.json"
    if update_fixtures:
        with open(filename, "w") as fhandle:
            fhandle.write(json.dumps(json_payload, indent=4, sort_keys=True))
            fhandle.write("\n")
    else:
        with open(filename, "r") as fhandle:
            stored_payload = json.load(fhandle)
        if stored_payload != json_payload:
            raise AssertionError(
                f"Payload fixture {filename} diverges from the code that defines it. Run "
                f"`pytest tests/unit/test_generate_payload_fixtures.py --update-fixtures` to update it."
            )


def test_save_payload_fixtures(
    reset_configurations,
    update_fixtures,
    request_payload_dir,
    snowflake_feature_store,
    snowflake_event_table,
    snowflake_item_table,
    snowflake_dimension_table,
    snowflake_scd_table,
    snowflake_time_series_table,
    snowflake_snapshots_table,
    snowflake_event_view_with_entity,
    float_target,
    bool_target,
    feature_group,
    cust_id_entity,
    transaction_entity,
    mysql_online_store_config,
):
    """
    Write request payload for testing api route
    """

    _ = reset_configurations
    feature_sum_30m = feature_group["sum_30m"]
    feature_sum_30m = replace_obj_id(feature_sum_30m, ObjectId("646f6c1b0ed28a5271fb02c4"))
    feature_sum_2h = feature_group["sum_2h"]
    feature_sum_2h = replace_obj_id(feature_sum_2h, ObjectId("646f6c1b0ed28a5271fb02c5"))
    feature_iet = iet_entropy(
        view=snowflake_event_view_with_entity,
        group_by_col="cust_id",
        window="24h",
        name="iet_entropy_24h",
        feature_job_setting=FeatureJobSetting(period="6h", offset="3h", blind_spot="3h"),
    )
    feature_iet = replace_obj_id(feature_iet, ObjectId("646f6c1c0ed28a5271fb02d0"))
    float_target = replace_obj_id(float_target, ObjectId("64a80107d667dd0c2b13d8cd"))
    bool_target = replace_obj_id(bool_target, ObjectId("64a80107d667dd0c2b13d8ce"))

    snowflake_item_table.event_id_col.as_entity(transaction_entity.name)
    item_view = snowflake_item_table.get_view(event_suffix="_event_table")
    feature_item_event = item_view.groupby("event_id_col").aggregate(
        value_column="cust_id_event_table",
        method=AggFunc.SUM,
        feature_name="item_event_sum_cust_id_feature",
    )
    feature_item_event = replace_obj_id(feature_item_event, ObjectId("646f6c1c0ed28a5271fb02d1"))

    feature_list = FeatureList(
        [feature_sum_30m], name="sf_feature_list", _id="646f6c1c0ed28a5271fb02d2"
    )
    feature_list_repeated = FeatureList(
        [feature_sum_30m], name="sf_feature_list_repeated", _id="6594d7dd2cc1a1b9c7f6c037"
    )
    feature_list_multiple = FeatureList(
        [feature_sum_30m, feature_sum_2h],
        name="sf_feature_list_multiple",
        _id="646f6c1c0ed28a5271fb02d3",
    )
    feature_job_setting_analysis = FeatureJobSettingAnalysisCreate(
        _id="62f301e841b73757c9ff879a",
        user_id="62f302f841b73757c9ff876b",
        name="sample_analysis",
        event_table_id=snowflake_event_table.id,
        analysis_data=None,
        analysis_length=2419200,
        min_featurejob_period=60,
        exclude_late_job=False,
        blind_spot_buffer_setting=5,
        job_time_buffer_setting="auto",
        late_data_allowance=5e-05,
    )
    context = ContextCreate(
        _id="646f6c1c0ed28a5271fb02d5",
        name="transaction_context",
        primary_entity_ids=[cust_id_entity.id],
    )
    deployment = DeploymentCreate(
        _id="646f6c1c0ed28a5271fb02d6", name="my_deployment", feature_list_id=feature_list.id
    )
    relationship_info = RelationshipInfoCreate(
        _id="63f6a145e549df8ccf123456",
        name="child_parent_relationship",
        relationship_type=RelationshipType.CHILD_PARENT,
        entity_id=cust_id_entity.id,
        related_entity_id=transaction_entity.id,
        relation_table_id="6337f9651050ee7d5980660d",
        enabled=True,
        updated_by="63f6a145e549df8ccf123444",
    )
    observation_table = ObservationTableCreate(
        _id="646f6c1c0ed28a5271fb02d7",
        name="observation_table",
        feature_store_id=snowflake_feature_store.id,
        request_input=SourceTableObservationInput(
            source=snowflake_event_table.tabular_source,
        ),
        primary_entity_ids=[cust_id_entity.id],
        purpose="other",
    )
    observation_table_from_obs_table = ObservationTableCreate(
        _id="68ccea50820076b52bbcceb4",
        name="observation_table_from_obs_table",
        feature_store_id=snowflake_feature_store.id,
        request_input=ObservationTableObservationInput(
            observation_table_id=observation_table.id,
        ),
        primary_entity_ids=[cust_id_entity.id],
        purpose="other",
    )
    historical_feature_table = HistoricalFeatureTableCreate(
        _id="646f6c1c0ed28a5271fb02d8",
        name="historical_feature_table",
        feature_store_id=snowflake_feature_store.id,
        observation_table_id=observation_table.id,
        featurelist_get_historical_features=FeatureListGetHistoricalFeatures(
            feature_clusters=feature_list._get_feature_clusters(),
            feature_list_id=feature_list.id,
        ),
    )
    target_namespace = TargetNamespaceCreate(
        _id="64ae4be43a93459ede8c383b",
        name="target_namespace",
        target_ids=[float_target.id],
        default_target_id=float_target.id,
        entity_ids=[cust_id_entity.id],
        window="7d",
        dtype=DBVarType.FLOAT,
        target_type="regression",
    )
    target_table = TargetTableCreate(
        _id="646f6c1c0ed28a5271fb32da",
        name="target_table",
        target_id=float_target.id,
        feature_store_id=snowflake_feature_store.id,
        observation_table_id=observation_table.id,
    )
    batch_request_table = BatchRequestTableCreate(
        _id="646f6c1c0ed28a5271fb02d9",
        name="batch_request_table",
        feature_store_id=snowflake_feature_store.id,
        request_input=SourceTableBatchRequestInput(
            source=snowflake_dimension_table.tabular_source,
        ),
        context_id=context.id,
    )
    batch_feature_table = BatchFeatureTableCreate(
        _id="646f6c1c0ed28a5271fb02da",
        name="batch_feature_table",
        feature_store_id=snowflake_feature_store.id,
        batch_request_table_id=batch_request_table.id,
        deployment_id=deployment.id,
    )
    batch_feature_table_with_request_input = BatchFeatureTableCreate(
        _id="646f6c1c0ed28a5271fb12dc",
        name="batch_feature_table_with_request_input",
        feature_store_id=snowflake_feature_store.id,
        request_input=SourceTableBatchRequestInput(
            source=snowflake_dimension_table.tabular_source,
        ),
        deployment_id=deployment.id,
    )
    static_source_table = StaticSourceTableCreate(
        _id="647b5ba9875a4313db21a1e0",
        name="static_source_table",
        feature_store_id=snowflake_feature_store.id,
        request_input=SourceTableStaticSourceInput(
            source=snowflake_event_table.tabular_source,
        ),
    )
    catalog = CatalogCreate(
        _id="646f6c1c0ed28a5271fb02db",
        name="grocery",
        default_feature_store_ids=[snowflake_feature_store.id],
    )
    credential = CredentialCreate(
        _id="646f6c1c0ed28a5271fb02dc",
        name="grocery",
        feature_store_id=snowflake_feature_store.id,
        database_credential=UsernamePasswordCredential(
            username="user",
            password="pass",
        ),
    )
    mysql_online_store = OnlineStoreCreate(
        _id="646f6c190ed28a5271fb02b9", **mysql_online_store_config
    )
    graph, node = snowflake_event_table.frame.extract_pruned_graph_and_node()
    feature_store_sample = FeatureStoreSample(
        graph=QueryGraph(**graph.model_dump(by_alias=True)),
        node_name=node.name,
        feature_store_id=snowflake_feature_store.id,
        from_timestamp="2012-11-24T11:00:00",
        to_timestamp="2019-11-24T11:00:00",
        timestamp_column="event_timestamp",
    )

    managed_view = ManagedViewCreate(
        _id="646f6c190ed28a5271fb02e9",
        sql="SELECT * FROM my_table",
        name="My Managed View",
        feature_store_id=snowflake_feature_store.id,
        columns_info=[ColumnInfo(name="col1", dtype=DBVarType.FLOAT)],
        description="This is a managed view",
    )
    observation_table_from_managed_view = ObservationTableCreate(
        _id="68ccea50820076b52bbcceb5",
        name="observation_table_from_managed_view",
        feature_store_id=snowflake_feature_store.id,
        request_input=ManagedViewObservationInput(
            managed_view_id=managed_view.id,
        ),
        primary_entity_ids=[cust_id_entity.id],
        purpose="other",
    )
    batch_feature_table_with_managed_view_request_input = BatchFeatureTableCreate(
        _id="646f6c1c0ed28a5271fb12de",
        name="batch_feature_table_with_managed_view_request_input",
        feature_store_id=snowflake_feature_store.id,
        request_input=ManagedViewBatchRequestInput(
            managed_view_id=managed_view.id,
        ),
        deployment_id=deployment.id,
    )
    development_dataset = DevelopmentDatasetCreate(
        _id="646f6c190ed28a5271fb02ea",
        name="My Development Dataset",
        sample_from_timestamp="2022-01-01",
        sample_to_timestamp="2024-12-31",
        development_tables=[
            DevelopmentTableCreate(
                table_id=ObjectId("6337f9651050ee7d5980660d"),
                location=TabularSource(
                    feature_store_id=snowflake_feature_store.id,
                    table_details=TableDetails(
                        database_name="sf_database",
                        schema_name="sf_schema",
                        table_name="sf_table_sample",
                    ),
                ),
            ),
        ],
        description="This is a development dataset",
    )

    treatment = TreatmentCreate(
        _id="6939304226d58567047d45bf",
        name="test_treatment",
        dtype=DBVarType.INT,
        treatment_type=TreatmentType.BINARY,
        source=AssignmentSource.RANDOMIZED,
        design=AssignmentDesign.SIMPLE_RANDOMIZATION,
        time=TreatmentTime.STATIC,
        time_structure=TreatmentTimeStructure.INSTANTANEOUS,
        interference=TreatmentInterference.NONE,
        treatment_labels=[0, 1],
        control_label=0,
        propensity=Propensity(
            granularity="global",
            knowledge="design-known",
            p_global=0.5,
        ),
    )

    context_with_treatment = ContextCreate(
        _id="693a208134d4b86a9a6945af",
        name="transaction_context_with_treatment",
        primary_entity_ids=[cust_id_entity.id],
        treatment_id=treatment.id,
    )

    generated_comment = [
        "THIS IS A GENERATED FILE. DO NOT EDIT THIS FILE DIRECTLY.",
        "Instead, update the test tests/unit/test_generate_payload_fixtures.py#test_save_payload_fixtures.",
        "Run `pytest --update-fixtures` to update it.",
    ]
    api_object_name_pairs = [
        (cust_id_entity, "entity"),
        (transaction_entity, "entity_transaction"),
        (snowflake_feature_store, "feature_store"),
        (snowflake_event_table, "event_table"),
        (snowflake_item_table, "item_table"),
        (snowflake_dimension_table, "dimension_table"),
        (snowflake_scd_table, "scd_table"),
        (snowflake_time_series_table, "time_series_table"),
        (snowflake_snapshots_table, "snapshots_table"),
        (feature_sum_30m, "feature_sum_30m"),
        (feature_sum_2h, "feature_sum_2h"),
        (feature_item_event, "feature_item_event"),
        (feature_iet, "feature_iet"),
        (feature_list, "feature_list_single"),
        (feature_list_repeated, "feature_list_single_repeated"),
        (feature_list_multiple, "feature_list_multi"),
        (float_target, "target"),
        (bool_target, "bool_target"),
    ]
    table_names = {"event_table", "item_table", "dimension_table", "scd_table"}
    for api_object, name in api_object_name_pairs:
        json_payload = api_object._get_create_payload()
        if name in table_names:
            # for tables, overwrite the columns_info to include entity related columns
            columns_info = api_object.cached_model.json_dict()["columns_info"]
            for col_info in columns_info:
                col_info.pop("semantic_id")
            json_payload["columns_info"] = columns_info

        if name == "scd_table":
            # do not include default_feature_job_setting for SCD table
            json_payload["default_feature_job_setting"] = None

        json_payload["_COMMENT"] = generated_comment
        update_or_check_payload_fixture(request_payload_dir, name, json_payload, update_fixtures)

    schema_payload_name_pairs = [
        (feature_job_setting_analysis, "feature_job_setting_analysis"),
        (context, "context"),
        (deployment, "deployment"),
        (relationship_info, "relationship_info"),
        (observation_table, "observation_table"),
        (observation_table_from_obs_table, "observation_table_from_obs_table"),
        (observation_table_from_managed_view, "observation_table_from_managed_view"),
        (historical_feature_table, "historical_feature_table"),
        (target_table, "target_table"),
        (batch_request_table, "batch_request_table"),
        (batch_feature_table, "batch_feature_table"),
        (batch_feature_table_with_request_input, "batch_feature_table_with_request_input"),
        (
            batch_feature_table_with_managed_view_request_input,
            "batch_feature_table_with_managed_view_request_input",
        ),
        (static_source_table, "static_source_table"),
        (catalog, "catalog"),
        (credential, "credential"),
        (target_namespace, "target_namespace"),
        (mysql_online_store, "mysql_online_store"),
        (feature_store_sample, "feature_store_sample"),
        (managed_view, "managed_view"),
        (development_dataset, "development_dataset"),
        (treatment, "treatment"),
        (context_with_treatment, "context_with_treatment"),
    ]
    for schema, name in schema_payload_name_pairs:
        json_to_write = schema.json_dict()
        json_to_write["_COMMENT"] = generated_comment
        update_or_check_payload_fixture(request_payload_dir, name, json_to_write, update_fixtures)


def test_generate_user_defined_function(
    update_fixtures, request_payload_dir, snowflake_feature_store_id
):
    """
    Write request payload for user defined function route

    Note: Run this after test_generate_payload_fixtures
    """
    user_defined_function = UserDefinedFunctionCreate(
        _id="64928868668f720c5bebbbd4",
        name="udf_test",
        description=None,
        sql_function_name="cos",
        function_parameters=[FunctionParameter(name="x", dtype=DBVarType.FLOAT)],
        output_dtype=DBVarType.FLOAT,
        feature_store_id=snowflake_feature_store_id,
    )
    json_payload = user_defined_function.json_dict()
    update_or_check_payload_fixture(
        request_payload_dir, "user_defined_function", json_payload, update_fixtures
    )
