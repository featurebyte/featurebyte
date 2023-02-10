"""
Unit tests for EntityValidationService
"""
import pytest

from featurebyte.exception import RequiredEntityNotProvidedError


@pytest.mark.asyncio
async def test_required_entity__missing(
    entity_validation_service, production_ready_feature, feature_store
):
    """
    Test a required entity is missing
    """
    with pytest.raises(RequiredEntityNotProvidedError) as exc:
        await entity_validation_service.validate_entities_or_prepare_for_parent_serving(
            graph=production_ready_feature.graph,
            nodes=[production_ready_feature.node],
            request_column_names=["a"],
            feature_store=feature_store,
        )
    expected = (
        'Required entities are not provided in the request: customer (serving name: "cust_id")'
    )
    assert str(exc.value) == expected


@pytest.mark.asyncio
async def test_required_entity__no_error(
    entity_validation_service, production_ready_feature, feature_store
):
    """
    Test required entity is provided
    """
    await entity_validation_service.validate_entities_or_prepare_for_parent_serving(
        graph=production_ready_feature.graph,
        nodes=[production_ready_feature.node],
        request_column_names=["cust_id"],
        feature_store=feature_store,
    )


@pytest.mark.asyncio
async def test_required_entity__serving_names_mapping(
    entity_validation_service, production_ready_feature, feature_store
):
    """
    Test validating with serving names mapping
    """
    # ok if the provided name matches the overrides in serving_names_mapping
    await entity_validation_service.validate_entities_or_prepare_for_parent_serving(
        graph=production_ready_feature.graph,
        nodes=[production_ready_feature.node],
        request_column_names=["new_cust_id"],
        serving_names_mapping={"cust_id": "new_cust_id"},
        feature_store=feature_store,
    )
