"""
Unit tests for aggregate_over
"""
import pytest

from featurebyte.enum import DBVarType
from tests.util.helper import get_node


@pytest.mark.parametrize(
    "value_column, expected_dtype",
    [
        ("col_int", DBVarType.INT),
        ("col_float", DBVarType.FLOAT),
        ("col_char", DBVarType.CHAR),
        ("col_text", DBVarType.VARCHAR),
        ("col_boolean", DBVarType.BOOL),
        ("created_at", DBVarType.TIMESTAMP),
        ("col_binary", DBVarType.BINARY),
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


def test_unbounded_window__valid(snowflake_event_view_with_entity, cust_id_entity):
    """
    Test a valid case of specifying None as window size
    """
    feature_group = snowflake_event_view_with_entity.groupby("cust_id").aggregate_over(
        value_column="col_float",
        method="latest",
        windows=[None],
        feature_names=["feat_latest"],
        feature_job_setting=dict(blind_spot="1m30s", frequency="6m", time_modulo_frequency="3m"),
    )
    feature_dict = feature_group["feat_latest"].dict()
    node = get_node(feature_dict["graph"], "groupby_1")
    assert node["parameters"]["windows"] == [None]


def test_unbounded_window__non_latest(snowflake_event_view_with_entity):
    """
    Test window size of None is only valid for latest aggregation method
    """
    with pytest.raises(ValueError) as exc:
        snowflake_event_view_with_entity.groupby("cust_id").aggregate_over(
            value_column="col_float",
            method="sum",
            windows=[None],
            feature_names=["feat_latest"],
            feature_job_setting=dict(
                blind_spot="1m30s", frequency="6m", time_modulo_frequency="3m"
            ),
        )
    assert str(exc.value) == 'Unbounded window is only supported for the "latest" method'


def test_unbounded_window__category_not_supported(snowflake_event_view_with_entity):
    """
    Test aggregation per category is not supported with unbounded windows
    """
    with pytest.raises(ValueError) as exc:
        snowflake_event_view_with_entity.groupby("cust_id", category="col_int").aggregate_over(
            value_column="col_float",
            method="latest",
            windows=[None],
            feature_names=["feat_latest"],
            feature_job_setting=dict(
                blind_spot="1m30s", frequency="6m", time_modulo_frequency="3m"
            ),
        )
    assert str(exc.value) == "category is not supported for aggregation with unbounded window"


def test_unbounded_window__composite_keys(snowflake_event_view_with_entity):
    """
    Test composite keys not yet supported
    """
    with pytest.raises(NotImplementedError) as exc:
        snowflake_event_view_with_entity.groupby(["cust_id", "col_int"]).aggregate_over(
            value_column="col_float",
            method="latest",
            windows=[None],
            feature_names=["feat_latest"],
            feature_job_setting=dict(
                blind_spot="1m30s", frequency="6m", time_modulo_frequency="3m"
            ),
        )
    assert str(exc.value) == "Composite keys not yet supported"
