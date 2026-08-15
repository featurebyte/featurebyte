"""
Test models#exposure module.
"""

import pytest

from featurebyte.api.exposure import Exposure
from featurebyte.models.exposure import ExposureModel


@pytest.fixture(name="float_exposure")
def float_exposure_fixture(grouped_event_view):
    """
    Float exposure fixture - create a target then convert to exposure via as_exposure
    """
    target = grouped_event_view.forward_aggregate(
        method="sum",
        value_column="col_float",
        window="1d",
        target_name="float_exposure_target",
        fill_value=0.0,
    )
    return target.as_exposure("float_exposure")


@pytest.fixture(name="lookup_exposure")
def lookup_exposure_fixture(snowflake_event_view_with_entity):
    """
    Lookup exposure fixture - create a lookup target then convert to exposure
    """
    target = snowflake_event_view_with_entity["col_float"].as_target(
        "lookup_exposure_target", "7d", fill_value=None
    )
    return target.as_exposure("lookup_exposure")


def test_as_exposure_returns_exposure_type(grouped_event_view):
    """
    Test that Target.as_exposure() returns an Exposure instance
    """
    target = grouped_event_view.forward_aggregate(
        method="sum",
        value_column="col_float",
        window="1d",
        target_name="target_for_exposure",
        fill_value=0.0,
    )
    exposure = target.as_exposure("my_exposure")
    assert isinstance(exposure, Exposure)
    assert exposure.name == "my_exposure"
    assert exposure.dtype == target.dtype
    assert exposure.node_name == target.node_name


def test_as_exposure_shares_graph(grouped_event_view):
    """
    Test that the Exposure created by as_exposure shares the same graph as the Target
    """
    target = grouped_event_view.forward_aggregate(
        method="sum",
        value_column="col_float",
        window="1d",
        target_name="target_for_exposure_graph",
        fill_value=0.0,
    )
    exposure = target.as_exposure("my_exposure_graph")
    assert exposure.graph == target.graph
    assert exposure.node_name == target.node_name
    assert exposure.tabular_source == target.tabular_source


@pytest.mark.asyncio
async def test_exposure_save_and_retrieve(float_exposure, app_container):
    """
    Test saving an exposure and retrieving it via the service
    """
    float_exposure.save()
    exposure_doc = await app_container.exposure_service.get_document(document_id=float_exposure.id)
    assert exposure_doc.name == "float_exposure"
    assert exposure_doc.derive_window() == "1d"

    # Verify namespace was created
    ns_doc = await app_container.exposure_namespace_service.get_document(
        document_id=exposure_doc.exposure_namespace_id
    )
    assert ns_doc.name == "float_exposure"
    assert ns_doc.default_exposure_id == float_exposure.id


@pytest.mark.asyncio
async def test_lookup_exposure_save_and_retrieve(lookup_exposure, app_container):
    """
    Test saving a lookup exposure and retrieving it
    """
    lookup_exposure.save()
    exposure_doc = await app_container.exposure_service.get_document(document_id=lookup_exposure.id)
    assert exposure_doc.name == "lookup_exposure"
    assert exposure_doc.derive_window() == "7d"


@pytest.mark.asyncio
async def test_get_exposure_source_column__lookup_exposure(lookup_exposure, app_container):
    """
    Test that get_exposure_source_column extracts the correct column from a lookup exposure.
    """
    lookup_exposure.save()
    exposure_doc = await app_container.exposure_service.get_document(document_id=lookup_exposure.id)
    result = exposure_doc.get_exposure_source_column()
    assert result is not None
    assert result.column_name == "col_float"
    assert result.table_id in [tid.table_id for tid in exposure_doc.table_id_column_names]


@pytest.mark.asyncio
async def test_get_exposure_source_column__forward_aggregate_exposure(
    float_exposure, app_container
):
    """
    Test that get_exposure_source_column returns None for a forward_aggregate exposure
    (not created via as_target or forward_aggregate_asat).
    """
    float_exposure.save()
    exposure_doc = await app_container.exposure_service.get_document(document_id=float_exposure.id)
    result = exposure_doc.get_exposure_source_column()
    assert result is None


@pytest.mark.asyncio
async def test_get_exposure_source_column__no_graph(
    snowflake_event_view_with_entity, app_container
):
    """
    Test that get_exposure_source_column returns None when exposure has no graph.
    """
    target = snowflake_event_view_with_entity["col_float"].as_target(
        "exposure_no_graph_target", "7d", fill_value=None
    )
    exposure = target.as_exposure("exposure_no_graph")
    exposure.save()
    exposure_doc = await app_container.exposure_service.get_document(document_id=exposure.id)
    # Override internal_graph to simulate an exposure with no recipe
    object.__setattr__(exposure_doc, "internal_graph", None)
    result = exposure_doc.get_exposure_source_column()
    assert result is None


def test_exposure_model_collection_name():
    """
    Test that ExposureModel has the correct collection name
    """
    assert ExposureModel.Settings.collection_name == "exposure"
