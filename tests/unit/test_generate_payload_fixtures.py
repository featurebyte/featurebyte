"""
Test module for generating payload fixtures for testing api route
"""
import json

from featurebyte import AggFunc, FeatureJobSetting, FeatureList
from featurebyte.models.credential import UsernamePasswordCredential
from featurebyte.models.relationship import RelationshipType
from featurebyte.models.request_input import SourceTableRequestInput
from featurebyte.schema.batch_feature_table import BatchFeatureTableCreate
from featurebyte.schema.batch_request_table import BatchRequestTableCreate
from featurebyte.schema.catalog import CatalogCreate
from featurebyte.schema.context import ContextCreate
from featurebyte.schema.credential import CredentialCreate
from featurebyte.schema.deployment import DeploymentCreate
from featurebyte.schema.feature_job_setting_analysis import FeatureJobSettingAnalysisCreate
from featurebyte.schema.feature_list import FeatureListGetHistoricalFeatures
from featurebyte.schema.historical_feature_table import HistoricalFeatureTableCreate
from featurebyte.schema.observation_table import ObservationTableCreate
from featurebyte.schema.relationship_info import RelationshipInfoCreate
from tests.util.helper import iet_entropy


def test_save_payload_fixtures(  # pylint: disable=too-many-arguments
    update_fixtures,
    snowflake_feature_store,
    snowflake_event_table,
    snowflake_item_table,
    snowflake_dimension_table,
    snowflake_scd_table,
    snowflake_event_view_with_entity,
    feature_group,
    non_time_based_feature,
    cust_id_entity,
    transaction_entity,
):
    """
    Write request payload for testing api route
    """
    # pylint: disable=too-many-locals
    feature_sum_30m = feature_group["sum_30m"]
    feature_sum_2h = feature_group["sum_2h"]
    feature_iet = iet_entropy(
        view=snowflake_event_view_with_entity,
        group_by_col="cust_id",
        window="24h",
        name="iet_entropy_24h",
        feature_job_setting=FeatureJobSetting(
            frequency="6h",
            time_modulo_frequency="3h",
            blind_spot="3h",
        ),
    )
    snowflake_item_table.event_id_col.as_entity(transaction_entity.name)
    item_view = snowflake_item_table.get_view(event_suffix="_event_table")
    feature_item_event = item_view.groupby("event_id_col").aggregate(
        value_column="cust_id_event_table",
        method=AggFunc.SUM,
        feature_name="item_event_sum_cust_id_feature",
    )

    feature_list = FeatureList([feature_sum_30m], name="sf_feature_list")
    feature_list_multiple = FeatureList(
        [feature_sum_30m, feature_sum_2h], name="sf_feature_list_multiple"
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
    context = ContextCreate(name="transaction_context", entity_ids=[cust_id_entity.id])
    deployment = DeploymentCreate(name="my_deployment", feature_list_id=feature_list.id)
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
        name="observation_table",
        feature_store_id=snowflake_feature_store.id,
        request_input=SourceTableRequestInput(
            source=snowflake_event_table.tabular_source,
        ),
        context_id=context.id,
    )
    historical_feature_table = HistoricalFeatureTableCreate(
        name="historical_feature_table",
        feature_store_id=snowflake_feature_store.id,
        observation_table_id=observation_table.id,
        featurelist_get_historical_features=FeatureListGetHistoricalFeatures(
            feature_clusters=feature_list._get_feature_clusters(),
            feature_list_id=feature_list.id,
        ),
    )
    batch_request_table = BatchRequestTableCreate(
        name="batch_request_table",
        feature_store_id=snowflake_feature_store.id,
        request_input=SourceTableRequestInput(
            source=snowflake_dimension_table.tabular_source,
        ),
        context_id=context.id,
    )
    batch_feature_table = BatchFeatureTableCreate(
        name="batch_feature_table",
        feature_store_id=snowflake_feature_store.id,
        batch_request_table_id=batch_request_table.id,
        deployment_id=deployment.id,
    )
    catalog = CatalogCreate(
        name="grocery",
        default_feature_store_ids=[snowflake_feature_store.id],
    )
    credential = CredentialCreate(
        name="grocery",
        feature_store_id=snowflake_feature_store.id,
        database_credential=UsernamePasswordCredential(
            username="user",
            password="pass",
        ),
    )

    if update_fixtures:
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
            (feature_sum_30m, "feature_sum_30m"),
            (feature_sum_2h, "feature_sum_2h"),
            (non_time_based_feature, "feature_item_event"),
            (feature_item_event, "feature_item_event"),
            (feature_iet, "feature_iet"),
            (feature_list, "feature_list_single"),
            (feature_list_multiple, "feature_list_multi"),
        ]
        output_filenames = []
        base_path = "tests/fixtures/request_payloads"
        for api_object, name in api_object_name_pairs:
            filename = f"{base_path}/{name}.json"
            with open(filename, "w") as fhandle:
                json_payload = api_object._get_create_payload()
                json_payload["_COMMENT"] = generated_comment
                fhandle.write(json.dumps(json_payload, indent=4, sort_keys=True))
            output_filenames.append(filename)

        schema_payload_name_pairs = [
            (feature_job_setting_analysis, "feature_job_setting_analysis"),
            (context, "context"),
            (deployment, "deployment"),
            (relationship_info, "relationship_info"),
            (observation_table, "observation_table"),
            (historical_feature_table, "historical_feature_table"),
            (batch_request_table, "batch_request_table"),
            (batch_feature_table, "batch_feature_table"),
            (catalog, "catalog"),
            (credential, "credential"),
        ]
        for schema, name in schema_payload_name_pairs:
            filename = f"{base_path}/{name}.json"
            with open(filename, "w") as fhandle:
                json_to_write = schema.json_dict()
                json_to_write["_COMMENT"] = generated_comment
                fhandle.write(json.dumps(json_to_write, indent=4, sort_keys=True))
            output_filenames.append(filename)
