"""
Unit tests for EntityValidationService
"""

import pytest

from featurebyte.exception import RequiredEntityNotProvidedError, UnexpectedServingNamesMappingError


@pytest.fixture(name="graph_nodes")
def graph_nodes_fixture(production_ready_feature):
    """
    Fixture for a feature's graph and nodes
    """
    return {"graph_nodes": (production_ready_feature.graph, [production_ready_feature.node])}


@pytest.fixture(name="feature_list_model")
def feature_list_model_fixture(feature_list):
    """
    Fixture for a feature list model
    """
    return {"feature_list_model": feature_list}


@pytest.fixture(name="graph_nodes_or_feature_list", params=["graph_nodes", "feature_list_model"])
def graph_nodes_or_feature_list_fixture(request):
    """
    Fixture for graph_nodes or feature_list_model as input for
    validate_entities_or_prepare_for_parent_serving
    """
    return request.getfixturevalue(request.param)


@pytest.mark.asyncio
async def test_required_entity__missing(entity_validation_service, graph_nodes_or_feature_list, feature_store):
    """
    Test a required entity is missing
    """
    with pytest.raises(RequiredEntityNotProvidedError) as exc:
        await entity_validation_service.validate_entities_or_prepare_for_parent_serving(
            request_column_names=["a"], feature_store=feature_store, **graph_nodes_or_feature_list
        )
    expected = 'Required entities are not provided in the request: customer (serving name: "cust_id")'
    assert str(exc.value) == expected


@pytest.mark.asyncio
async def test_required_entity__no_error(entity_validation_service, graph_nodes_or_feature_list, feature_store):
    """
    Test required entity is provided
    """
    await entity_validation_service.validate_entities_or_prepare_for_parent_serving(
        request_column_names=["cust_id"], feature_store=feature_store, **graph_nodes_or_feature_list
    )


@pytest.mark.asyncio
async def test_required_entity__serving_names_mapping(
    entity_validation_service, graph_nodes_or_feature_list, feature_store
):
    """
    Test validating with serving names mapping
    """
    # ok if the provided name matches the overrides in serving_names_mapping
    await entity_validation_service.validate_entities_or_prepare_for_parent_serving(
        request_column_names=["new_cust_id"],
        serving_names_mapping={"cust_id": "new_cust_id"},
        feature_store=feature_store,
        **graph_nodes_or_feature_list,
    )


@pytest.mark.asyncio
async def test_required_entity__serving_names_mapping_invalid(
    entity_validation_service, graph_nodes_or_feature_list, feature_store
):
    """
    Test validating with serving names mapping that is invalid
    """
    with pytest.raises(UnexpectedServingNamesMappingError) as exc:
        await entity_validation_service.validate_entities_or_prepare_for_parent_serving(
            request_column_names=["new_cust_id"],
            serving_names_mapping={"cust_idz": "new_cust_id"},
            feature_store=feature_store,
            **graph_nodes_or_feature_list,
        )
    assert str(exc.value) == "Unexpected serving names provided in serving_names_mapping: cust_idz"
