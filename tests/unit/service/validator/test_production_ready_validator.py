"""
Test production ready validator
"""
from typing import Any, Dict, List

import pytest

from featurebyte import DimensionView, EventView, FeatureJobSetting, MissingValueImputation
from featurebyte.query_graph.enum import GraphNodeType
from featurebyte.service.validator.production_ready_validator import ProductionReadyValidator


@pytest.fixture(name="production_ready_validator")
def production_ready_validator_fixture(feature_namespace_service, app_container):
    """
    Get production ready validator
    """
    return ProductionReadyValidator(
        feature_namespace_service=feature_namespace_service,
        data_service=app_container.tabular_data_service,
    )


@pytest.mark.asyncio
async def test_validate(
    production_ready_validator,
    snowflake_feature_store,
    snowflake_event_data_with_entity,
    feature_group_feature_job_setting,
):
    """
    Test the validate method returns an error when there are differences.
    """
    # Generate a feature
    snowflake_feature_store.save()
    snowflake_event_data_with_entity.save()
    snowflake_event_data_with_entity.update_default_feature_job_setting(
        feature_job_setting=FeatureJobSetting(**feature_group_feature_job_setting)
    )
    event_view = EventView.from_event_data(snowflake_event_data_with_entity)
    updated_feature_job_setting = feature_group_feature_job_setting
    assert feature_group_feature_job_setting["blind_spot"] == "10m"
    updated_feature_job_setting["blind_spot"] = "5m"  # set a new value
    feature = event_view.groupby("cust_id").aggregate_over(
        value_column="col_float",
        method="sum",
        windows=["30m", "2h", "1d"],
        feature_job_setting=updated_feature_job_setting,
        feature_names=["sum_30m", "sum_2h", "sum_1d"],
    )["sum_30m"]

    # Update cleaning operations after feature has been created
    snowflake_event_data_with_entity["col_int"].update_critical_data_info(
        cleaning_operations=[
            MissingValueImputation(imputed_value=0.0),
        ]
    )

    # Verify that validate does not throw an error if ignore_guardrails is True
    await production_ready_validator.validate(
        "sum_30m", feature.node, feature.graph, ignore_guardrails=True
    )

    # Verify that validates throws an error without ignore_guardrails
    with pytest.raises(ValueError) as exc:
        await production_ready_validator.validate("sum_30m", feature.node, feature.graph)
    exception_str = str(exc.value)
    assert "Discrepancies found between the current feature version" in exception_str
    assert "feature_job_setting" in exception_str
    assert "cleaning_operations" in exception_str


@pytest.mark.asyncio
async def test_assert_no_other_production_ready_feature__does_not_exist(production_ready_validator):
    """
    Test that there are no other production ready features.

    sum_30m is the name of the production_ready_feature used in the next test.
    """
    await production_ready_validator._assert_no_other_production_ready_feature("sum_30m")


@pytest.mark.asyncio
async def test_assert_no_other_production_ready_feature__exists(
    production_ready_validator, production_ready_feature
):
    """
    Test that assert throws an error if there are other production ready features with the same name.

    """
    with pytest.raises(ValueError) as exc:
        await production_ready_validator._assert_no_other_production_ready_feature(
            production_ready_feature.name
        )
    assert "Found another feature version that is already" in str(exc)


def test_get_feature_job_setting_from_graph(
    production_ready_feature, feature_group_feature_job_setting
):
    """
    Test retrieving feature job setting from a graph.
    """
    feature_job_setting = ProductionReadyValidator._get_feature_job_setting_from_graph(
        production_ready_feature.node, production_ready_feature.graph
    )
    expected_feature_job_setting = FeatureJobSetting(**feature_group_feature_job_setting)
    assert feature_job_setting.to_seconds() == expected_feature_job_setting.to_seconds()


@pytest.mark.asyncio
async def test_get_feature_job_setting_for_data_source(
    production_ready_validator,
    snowflake_event_data_with_entity,
    snowflake_feature_store,
    feature_group_feature_job_setting,
):
    """
    Test _get_feature_job_setting_for_data_source
    """
    snowflake_feature_store.save()
    snowflake_event_data_with_entity.save()
    snowflake_event_data_with_entity.update_default_feature_job_setting(
        feature_job_setting=FeatureJobSetting(**feature_group_feature_job_setting)
    )

    feature_job_setting = await production_ready_validator._get_feature_job_setting_for_data_source(
        snowflake_event_data_with_entity.id
    )
    expected_feature_job_setting = FeatureJobSetting(**feature_group_feature_job_setting)
    assert feature_job_setting == expected_feature_job_setting


@pytest.mark.asyncio
async def test_get_feature_job_setting_diffs__settings_differ(
    production_ready_validator,
    snowflake_feature_store,
    snowflake_event_data_with_entity,
    feature_group_feature_job_setting,
):
    """
    Test _check_feature_job_setting_match returns a dictionary when the settings differ
    """
    # update event data w/ a feature job setting
    snowflake_feature_store.save()
    snowflake_event_data_with_entity.save()
    snowflake_event_data_with_entity.update_default_feature_job_setting(
        FeatureJobSetting(**feature_group_feature_job_setting)
    )

    # create a feature with a different feature job setting from the event data
    event_view = EventView.from_event_data(snowflake_event_data_with_entity)
    updated_feature_job_setting = feature_group_feature_job_setting
    assert feature_group_feature_job_setting["blind_spot"] == "10m"
    updated_feature_job_setting["blind_spot"] = "5m"  # set a new value
    feature_group = event_view.groupby("cust_id").aggregate_over(
        value_column="col_float",
        method="sum",
        windows=["30m", "2h", "1d"],
        feature_job_setting=updated_feature_job_setting,
        feature_names=["sum_30m", "sum_2h", "sum_1d"],
    )
    feature = feature_group["sum_30m"]

    # check if the settings match
    differences = await production_ready_validator._get_feature_job_setting_diffs(
        feature.node, feature.graph
    )

    # assert that there are differences
    assert differences == {
        "default": FeatureJobSetting(frequency="30m", time_modulo_frequency="5m", blind_spot="10m"),
        "feature": FeatureJobSetting(
            frequency="1800s", time_modulo_frequency="300s", blind_spot="300s"
        ),
    }


@pytest.mark.asyncio
async def test_get_feature_job_setting_diffs__no_difference(
    production_ready_validator, production_ready_feature
):
    """
    Test _check_feature_job_setting_match
    """
    differences = await production_ready_validator._get_feature_job_setting_diffs(
        production_ready_feature.node, production_ready_feature.graph
    )
    assert not differences


@pytest.fixture(name="feature_from_dimension_data")
def feature_from_dimension_data_fixture(cust_id_entity, snowflake_dimension_data):
    """Feature from dimension data"""
    snowflake_dimension_data["col_int"].as_entity(cust_id_entity.name)
    dimension_view = DimensionView.from_dimension_data(snowflake_dimension_data)
    return dimension_view["col_float"].as_feature("FloatFeature")  # pylint: disable=no-member


@pytest.mark.asyncio
async def test_get_feature_job_setting_diffs__from_dimension_data_feature_has_no_result(
    production_ready_validator, feature_from_dimension_data
):
    """
    Test _check_feature_job_setting_match from dimension data has no result. This is because only features created
    from event or item data will have feature job settings. As such, we can skip this validation for features
    that are not created from item or event data.
    """
    differences = await production_ready_validator._get_feature_job_setting_diffs(
        feature_from_dimension_data.node, feature_from_dimension_data.graph
    )
    assert not differences


def test_get_input_event_data_id__event_data(production_ready_feature):
    """
    Test retrieving the input event data ID from event data backed feature.
    """
    input_event_data_id = ProductionReadyValidator._get_input_event_data_id(
        production_ready_feature.node, production_ready_feature.graph
    )
    assert input_event_data_id is not None


def test_get_input_event_data_id__dimension_data(feature_from_dimension_data):
    """
    Test retrieving input event data ID from dimension data backed feature - should return None.
    """
    input_event_data_id = ProductionReadyValidator._get_input_event_data_id(
        feature_from_dimension_data.node, feature_from_dimension_data.graph
    )
    assert input_event_data_id is None


def test_get_input_event_data_id__item_data(non_time_based_feature):
    """
    Test retrieving the input event data ID from item data
    """
    input_event_data_id = ProductionReadyValidator._get_input_event_data_id(
        non_time_based_feature.node, non_time_based_feature.graph
    )
    assert input_event_data_id is not None


def test_get_cleaning_node_from_feature_graph(feature_with_cleaning_operations):
    """
    Test _get_cleaning_node_from_feature_graph
    """
    cleaning_node = ProductionReadyValidator._get_cleaning_node_from_feature_graph(
        feature_with_cleaning_operations.node, feature_with_cleaning_operations.graph
    )
    assert cleaning_node.parameters.graph.dict()["nodes"] == [
        {
            "name": "proxy_input_1",
            "output_type": "frame",
            "parameters": {"input_order": 0},
            "type": "proxy_input",
        },
        {
            "name": "project_1",
            "output_type": "series",
            "parameters": {"columns": ["col_float"]},
            "type": "project",
        },
        {"name": "is_null_1", "output_type": "series", "parameters": {}, "type": "is_null"},
        {
            "name": "conditional_1",
            "output_type": "series",
            "parameters": {"value": 0},
            "type": "conditional",
        },
        {
            "name": "cast_1",
            "output_type": "series",
            "parameters": {"from_dtype": "FLOAT", "type": "float"},
            "type": "cast",
        },
        {
            "name": "assign_1",
            "output_type": "frame",
            "parameters": {"name": "col_float", "value": None},
            "type": "assign",
        },
    ]


@pytest.mark.asyncio
async def test_get_cleaning_node_from_input_data(
    feature_with_cleaning_operations, production_ready_validator
):
    """
    Test _get_cleaning_node_from_input_data
    """
    cleaning_node = await production_ready_validator._get_cleaning_node_from_input_data(
        feature_with_cleaning_operations.node, feature_with_cleaning_operations.graph
    )
    assert cleaning_node is not None
    assert cleaning_node.parameters.type == GraphNodeType.CLEANING


@pytest.fixture(name="cleaning_graph_node")
def cleaning_graph_node_fixture(feature_with_cleaning_operations):
    """
    Get cleaning graph node
    """
    graph_node = ProductionReadyValidator._get_cleaning_node_from_feature_graph(
        feature_with_cleaning_operations.node, feature_with_cleaning_operations.graph
    )
    assert graph_node is not None
    return graph_node


def test_diff_cleaning_nodes(cleaning_graph_node):
    """
    Test _diff_cleaning_nodes
    """

    response = ProductionReadyValidator._diff_cleaning_nodes(None, None)
    assert not response

    response = ProductionReadyValidator._diff_cleaning_nodes(None, cleaning_graph_node)
    expected_node_names = [
        "proxy_input_1",
        "project_1",
        "is_null_1",
        "conditional_1",
        "cast_1",
        "assign_1",
    ]
    assert len(response) == 1
    diff_b = response["feature"]
    node_names = [node["name"] for node in diff_b["nodes"]]
    assert node_names == expected_node_names

    response = ProductionReadyValidator._diff_cleaning_nodes(cleaning_graph_node, None)
    assert len(response) == 1
    diff_a = response["default"]
    node_names = [node["name"] for node in diff_a["nodes"]]
    assert node_names == expected_node_names

    response = ProductionReadyValidator._diff_cleaning_nodes(
        cleaning_graph_node, cleaning_graph_node
    )
    assert not response


def get_node_for_name(nodes: List[Dict[str, Any]], node_name: str) -> Dict[str, Any]:
    """Helper function to get node dictionary for a node name"""
    for node in nodes:
        if node["name"] == node_name:
            return node
    return {}


@pytest.mark.asyncio
async def test_get_cleaning_operations_diffs__with_two_different_nodes(
    snowflake_event_data_with_entity,
    snowflake_feature_store,
    production_ready_validator,
    feature_group_feature_job_setting,
):
    """
    Test diff_cleaning_nodes when the feature, and data, have different cleaning operations.
    """
    # Create some event data with a simple cleaning operation.
    snowflake_feature_store.save()
    snowflake_event_data_with_entity.save()
    snowflake_event_data_with_entity["col_int"].update_critical_data_info(
        cleaning_operations=[
            MissingValueImputation(imputed_value=0.0),
        ]
    )

    # Create a feature using that event data
    event_view = EventView.from_event_data(snowflake_event_data_with_entity)
    feature_group = event_view.groupby("cust_id").aggregate_over(
        value_column="col_float",
        method="sum",
        windows=["30m"],
        feature_job_setting=feature_group_feature_job_setting,
        feature_names=["sum_30m"],
    )
    feature = feature_group["sum_30m"]

    # Update the event data to have a different cleaning operation
    snowflake_event_data_with_entity["col_int"].update_critical_data_info(
        cleaning_operations=[
            MissingValueImputation(imputed_value=2.0),  # new value
        ]
    )

    # Check to see if the cleaning operations between the feature, and input data, match.
    diff = await production_ready_validator._get_cleaning_operations_diffs(
        feature.node, feature.graph
    )

    # Assert that they are different.
    default_cleaning_graph_nodes = diff["default"]
    conditional_node_a = get_node_for_name(default_cleaning_graph_nodes["nodes"], "conditional_1")
    assert conditional_node_a["parameters"]["value"] == 2
    feature_cleaning_graph_nodes = diff["feature"]
    conditional_node_b = get_node_for_name(feature_cleaning_graph_nodes["nodes"], "conditional_1")
    assert conditional_node_b["parameters"]["value"] == 0


def test_raise_error_if_diffs_present():
    """
    Test raise error if diffs present
    """
    # Nothing expected to happen
    ProductionReadyValidator._raise_error_if_diffs_present({}, {})

    # Feature job setting errors found, but no cleaning ops errors
    some_error_diff = {"random key": "random stuff"}
    with pytest.raises(ValueError) as exc:
        ProductionReadyValidator._raise_error_if_diffs_present(some_error_diff, {})
    str_exc = str(exc.value)
    assert "Discrepancies found" in str_exc
    assert "feature_job_setting" in str_exc
    assert "cleaning_operations" not in str_exc

    # Cleaning ops errors found, but no feature job setting errors
    with pytest.raises(ValueError) as exc:
        ProductionReadyValidator._raise_error_if_diffs_present({}, some_error_diff)
    str_exc = str(exc.value)
    assert "Discrepancies found" in str_exc
    assert "feature_job_setting" not in str_exc
    assert "cleaning_operations" in str_exc

    # Both feature job setting, and cleaning ops errors found
    with pytest.raises(ValueError) as exc:
        ProductionReadyValidator._raise_error_if_diffs_present(some_error_diff, some_error_diff)
    str_exc = str(exc.value)
    assert "Discrepancies found" in str_exc
    assert "feature_job_setting" in str_exc
    assert "cleaning_operations" in str_exc
