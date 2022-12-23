"""
Unit tests for aggregate_over
"""
import pytest

from featurebyte.enum import DBVarType


@pytest.mark.parametrize(
    "value_column, expected_dtype",
    [
        ("col_int", DBVarType.INT),
        ("col_float", DBVarType.FLOAT),
        ("col_char", DBVarType.CHAR),
        ("col_text", DBVarType.VARCHAR),
        ("col_boolean", DBVarType.BOOL),
        ("created_at", DBVarType.TIMESTAMP),
    ],
)
def test_aggregate_over__latest_method_output_vartype(
    snowflake_event_view_with_entity, value_column, expected_dtype
):
    """
    Test latest aggregation output variable type
    """
    feature_group = snowflake_event_view_with_entity.groupby("cust_id").aggregate_over(
        value_column=value_column,
        method="latest",
        windows=["1h"],
        feature_names=["feat_1h"],
        feature_job_setting=dict(blind_spot="1m30s", frequency="6m", time_modulo_frequency="3m"),
    )
    assert feature_group["feat_1h"].dtype == expected_dtype
