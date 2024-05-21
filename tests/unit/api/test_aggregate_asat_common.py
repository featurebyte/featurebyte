"""
Test common behaviour shared between aggregate_asat and forward_aggregate_asat
"""

import pytest


def aggregate_asat_helper(view, is_forward, groupby_args, aggregate_kwargs):
    """
    Helper function to call either aggregate_asat and forward_aggregate_asat
    """
    assert isinstance(groupby_args, (list, tuple))
    assert isinstance(aggregate_kwargs, dict)

    groupby_obj = view.groupby(*groupby_args)
    if is_forward:
        func = groupby_obj.forward_aggregate_asat
        if "name" in aggregate_kwargs:
            aggregate_kwargs["target_name"] = aggregate_kwargs.pop("name")
    else:
        func = groupby_obj.aggregate_asat
        if "name" in aggregate_kwargs:
            aggregate_kwargs["feature_name"] = aggregate_kwargs.pop("name")

    return func(**aggregate_kwargs)


@pytest.mark.parametrize("is_forward", [False, True])
def test_aggregate_asat__method_required(snowflake_scd_view_with_entity, is_forward):
    """
    Test method parameter is required
    """
    with pytest.raises(ValueError) as exc:
        aggregate_asat_helper(
            snowflake_scd_view_with_entity,
            is_forward,
            ["col_boolean"],
            dict(value_column="col_float", name="asat_output"),
        )
    assert str(exc.value) == "method is required"


@pytest.mark.parametrize("is_forward", [False, True])
def test_aggregate_asat__feature_name_required(snowflake_scd_view_with_entity, is_forward):
    """
    Test feature_name parameter is required
    """
    with pytest.raises(ValueError) as exc:
        aggregate_asat_helper(
            snowflake_scd_view_with_entity,
            is_forward,
            ["col_boolean"],
            dict(value_column="col_float", name="asat_output"),
        )
    assert str(exc.value) == "method is required"


@pytest.mark.parametrize("is_forward", [False, True])
def test_aggregate_asat__latest_not_supported(snowflake_scd_view_with_entity, is_forward):
    """
    Test using "latest" method is not supported
    """
    with pytest.raises(ValueError) as exc:
        aggregate_asat_helper(
            snowflake_scd_view_with_entity,
            is_forward,
            ["col_boolean"],
            dict(value_column="col_float", method="latest", name="asat_feature"),
        )
    assert str(exc.value) == "latest aggregation method is not supported for aggregated_asat"


@pytest.mark.parametrize("is_forward", [False, True])
def test_aggregate_asat__groupby_key_cannot_be_natural_key(
    snowflake_scd_view_with_entity, is_forward
):
    """
    Test using natural key as groupby key is not allowed
    """
    with pytest.raises(ValueError) as exc:
        aggregate_asat_helper(
            snowflake_scd_view_with_entity,
            is_forward,
            ["col_text"],
            dict(value_column="col_float", method="sum", name="asat_feature"),
        )
    assert str(exc.value) == "Natural key column cannot be used as a groupby key in aggregate_asat"


@pytest.mark.parametrize("is_forward", [False, True])
def test_aggregate_asat__invalid_offset_string(snowflake_scd_view_with_entity, is_forward):
    """
    Test offset string is validated
    """
    with pytest.raises(ValueError) as exc:
        aggregate_asat_helper(
            snowflake_scd_view_with_entity,
            is_forward,
            ["col_boolean"],
            dict(value_column="col_float", method="sum", name="asat_feature", offset="yesterday"),
        )
    assert "Failed to parse the offset parameter" in str(exc.value)
