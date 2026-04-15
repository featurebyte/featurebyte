"""
Exposure integration tests.

Tests the Exposure and ExposureNamespace lifecycle through the SDK API, including
creation via Target.as_exposure(), saving, namespace management, and association
with use cases.
"""

import pandas as pd
import pytest
from bson import ObjectId

from featurebyte import AggFunc, Context
from featurebyte.api.exposure import Exposure
from featurebyte.api.exposure_namespace import ExposureNamespace
from featurebyte.enum import DBVarType
from featurebyte.schema.use_case import UseCaseCreate
from tests.util.helper import (
    tz_localize_if_needed,
)


def test_target_as_exposure(event_table, source_type):
    """
    Test creating an Exposure from a Target via as_exposure().
    """
    event_view = event_table.get_view()

    target = event_view.groupby("ÜSER ID").forward_aggregate(
        method="sum",
        value_column="ÀMOUNT",
        window="24h",
        target_name="amount_target_for_exposure",
        fill_value=0.0,
    )

    exposure = target.as_exposure("amount_exposure_24h")
    assert isinstance(exposure, Exposure)
    assert exposure.name == "amount_exposure_24h"
    assert exposure.dtype == target.dtype
    assert exposure.node_name == target.node_name


def test_exposure_save_creates_namespace(event_table, user_entity, source_type):
    """
    Test that saving an Exposure created via as_exposure() automatically creates
    an ExposureNamespace.
    """
    event_view = event_table.get_view()

    target = event_view.groupby("ÜSER ID").forward_aggregate(
        method=AggFunc.COUNT,
        value_column=None,
        window="7d",
        target_name=f"count_target_for_exposure_{ObjectId()}",
        fill_value=None,
    )

    exposure = target.as_exposure(f"count_exposure_{ObjectId()}")
    exposure.save()

    exposure_ns = exposure.exposure_namespace
    assert exposure_ns is not None
    assert exposure_ns.default_exposure_id == exposure.id
    assert exposure_ns.dtype == exposure.dtype
    assert exposure.id in exposure_ns.exposure_ids


def test_exposure_preview(event_table, source_type):
    """
    Test previewing an Exposure created from a Target.
    """
    event_view = event_table.get_view()

    exposure_name = "exposure_for_preview"
    target = event_view.groupby("ÜSER ID").forward_aggregate(
        method="sum",
        value_column="ÀMOUNT",
        window="24h",
        target_name=exposure_name,
        fill_value=None,
    )

    exposure = target.as_exposure(exposure_name)

    preview_params = {"POINT_IN_TIME": "2001-11-15 10:00:00", "üser id": 1}
    df_preview = exposure.preview(pd.DataFrame([preview_params]))
    tz_localize_if_needed(df_preview, source_type)

    assert exposure_name in df_preview.columns
    assert "POINT_IN_TIME" in df_preview.columns
    assert len(df_preview) == 1
    assert df_preview[exposure_name].iloc[0] is not None


def test_exposure_and_target_share_computation(event_table, source_type):
    """
    Test that an Exposure created from a Target produces the same values when
    previewed with the same observation set.
    """
    event_view = event_table.get_view()

    col_name = "shared_computation_col"
    target = event_view.groupby("ÜSER ID").forward_aggregate(
        method="sum",
        value_column="ÀMOUNT",
        window="24h",
        target_name=col_name,
        fill_value=None,
    )
    exposure = target.as_exposure(col_name)

    preview_params = {"POINT_IN_TIME": "2001-11-15 10:00:00", "üser id": 1}
    df = pd.DataFrame([preview_params])

    target_preview = target.preview(df)
    exposure_preview = exposure.preview(df)

    tz_localize_if_needed(target_preview, source_type)
    tz_localize_if_needed(exposure_preview, source_type)

    assert target_preview[col_name].iloc[0] == exposure_preview[col_name].iloc[0]


@pytest.mark.asyncio
async def test_parent_entity_target_and_exposure_with_compute(
    event_table, item_table, user_entity, item_entity, source_type, app_container
):
    """
    Test that a use case can associate a target and exposure whose primary entity is a
    parent entity of the context's primary entity, and that compute_targets works
    with parent entity serving.
    """
    event_view = event_table.get_view()

    target_name = f"user_count_target_{ObjectId()}"
    target = event_view.groupby("ÜSER ID").forward_aggregate(
        method=AggFunc.COUNT,
        value_column=None,
        window="7d",
        target_name=target_name,
        fill_value=None,
    )
    target.save()
    target.update_target_type("regression")

    exposure = target.as_exposure(f"user_count_exposure_{ObjectId()}")
    exposure.save()

    # Exposure is now associated with the Context (not the UseCase)
    context = Context.create(
        name=f"item_context_{ObjectId()}",
        primary_entity=[item_entity.name],
        exposure_name=exposure.name,
    )

    use_case_service = app_container.use_case_service
    target_doc = await app_container.target_service.get_document(document_id=target.id)

    use_case = await use_case_service.create_use_case(
        UseCaseCreate(
            name=f"uc_parent_entity_{ObjectId()}",
            target_id=target.id,
            target_namespace_id=target_doc.target_namespace_id,
            context_id=context.id,
        )
    )
    assert use_case.target_id == target.id
    assert context.exposure_id == exposure.id

    # Verify compute_targets works with child entity (Item)
    df_obs_child = pd.DataFrame([
        {"POINT_IN_TIME": pd.Timestamp("2001-11-15 10:00:00"), "item_id": "item_42"},
    ])
    df_result_child = target.compute_targets(df_obs_child)
    assert target_name in df_result_child.columns
    assert len(df_result_child) == 1
    child_value = df_result_child[target_name].iloc[0]

    # Also verify compute_targets works directly with parent entity (User)
    df_obs_parent = pd.DataFrame([
        {"POINT_IN_TIME": pd.Timestamp("2001-11-15 10:00:00"), "üser id": 1},
    ])
    df_result_parent = target.compute_targets(df_obs_parent)
    assert target_name in df_result_parent.columns
    assert len(df_result_parent) == 1
    parent_value = df_result_parent[target_name].iloc[0]

    assert child_value is not None
    assert parent_value is not None


def test_exposure_namespace_create_and_delete(event_table, user_entity, source_type):
    """
    Test creating and deleting an ExposureNamespace directly.
    """
    exposure_ns = ExposureNamespace.create(
        name=f"test_exposure_ns_{ObjectId()}",
        primary_entity=[user_entity.name],
        dtype=DBVarType.FLOAT,
        window="7d",
    )
    assert exposure_ns.dtype == DBVarType.FLOAT
    assert exposure_ns.window == "7d"
    assert exposure_ns.default_version_mode == "AUTO"

    exposure_ns.delete()


def test_exposure_namespace_list(event_table, user_entity, source_type):
    """
    Test listing ExposureNamespaces.
    """
    unique = str(ObjectId())[:8]
    ns1 = ExposureNamespace.create(
        name=f"exposure_list_a_{unique}",
        primary_entity=[user_entity.name],
        dtype=DBVarType.FLOAT,
    )
    ns2 = ExposureNamespace.create(
        name=f"exposure_list_b_{unique}",
        primary_entity=[user_entity.name],
        dtype=DBVarType.INT,
    )

    listing = ExposureNamespace.list()
    assert len(listing) >= 2
    names = listing["name"].tolist()
    assert ns1.name in names
    assert ns2.name in names

    ns1.delete()
    ns2.delete()


@pytest.mark.asyncio
async def test_use_case_with_exposure(event_table, user_entity, source_type, app_container):
    """
    Test creating a UseCase with both a target and an exposure created via as_exposure().
    """
    event_view = event_table.get_view()

    target = event_view.groupby("ÜSER ID").forward_aggregate(
        method=AggFunc.COUNT,
        value_column=None,
        window="7d",
        target_name=f"target_for_uc_exposure_{ObjectId()}",
        fill_value=None,
    )
    target.save()
    target.update_target_type("regression")

    exposure = target.as_exposure(f"exposure_for_uc_{ObjectId()}")
    exposure.save()

    # Exposure is now associated with the Context
    context = Context.create(
        name=f"context_exposure_{ObjectId()}",
        primary_entity=[user_entity.name],
        exposure_name=exposure.name,
    )

    use_case_service = app_container.use_case_service
    target_doc = await app_container.target_service.get_document(document_id=target.id)

    use_case = await use_case_service.create_use_case(
        UseCaseCreate(
            name=f"uc_exposure_{ObjectId()}",
            target_id=target.id,
            target_namespace_id=target_doc.target_namespace_id,
            context_id=context.id,
        )
    )
    assert use_case.target_id == target.id
    assert context.exposure_id == exposure.id
    assert context.exposure_namespace_id is not None
