"""
Test ColumnInfo class
"""

import pytest

from featurebyte.enum import DBVarType
from featurebyte.query_graph.model.column_info import ColumnInfo
from featurebyte.query_graph.model.critical_data_info import CriticalDataInfo
from featurebyte.query_graph.node.cleaning_operation import (
    DisguisedValueImputation,
    MissingValueImputation,
    UnexpectedValueImputation,
    ValueBeyondEndpointImputation,
)


@pytest.mark.parametrize(
    "data_type,imputed_value,expected_imputed_value",
    [
        # successful cast
        (DBVarType.CHAR, "missing", "missing"),
        (DBVarType.VARCHAR, "missing", "missing"),
        (DBVarType.VARCHAR, 1234, "1234"),
        (DBVarType.VARCHAR, True, "True"),
        (DBVarType.INT, 12, 12),
        (DBVarType.INT, 12.9, 12),
        (DBVarType.INT, "12", 12),
        (DBVarType.FLOAT, 12, 12.0),
        (DBVarType.FLOAT, 12.9, 12.9),
        (DBVarType.FLOAT, "12.9", 12.9),
        (DBVarType.BOOL, 0, False),
        (DBVarType.BOOL, "", False),
        (DBVarType.BOOL, 12.9, True),
        (DBVarType.DATE, "2023-11-09T11:59:15", "2023-11-09"),
        (DBVarType.TIMESTAMP, "2023-11-09T11:59:15", "2023-11-09T11:59:15"),
        (DBVarType.TIMESTAMP, "2023-11-09", "2023-11-09T00:00:00"),
        (DBVarType.TIMESTAMP, "2021-01-01 00:00:00-0800", "2021-01-01T08:00:00"),
        (DBVarType.TIMESTAMP_TZ, "2023-11-09T11:59:15", "2023-11-09T11:59:15"),
        (DBVarType.TIMESTAMP_TZ, "2021-01-01 00:00:00-0800", "2021-01-01T00:00:00-0800"),
        # failed cast (expected imputed value is None)
        (DBVarType.VARCHAR, None, None),
        (DBVarType.INT, "abc", None),
        (DBVarType.FLOAT, "abc", None),
        (DBVarType.DATE, "abc", None),
        (DBVarType.TIMESTAMP, "abc", None),
        (DBVarType.TIMESTAMP_TZ, "abc", None),
    ],
)
def test_column_info__valid_construction_with_missing_value_imputation(
    data_type, imputed_value, expected_imputed_value
):
    """Test ColumnInfo construction"""
    if expected_imputed_value is None:
        with pytest.raises(ValueError):
            ColumnInfo(
                name="test_column",
                dtype=data_type,
                critical_data_info=CriticalDataInfo(
                    cleaning_operations=[MissingValueImputation(imputed_value=imputed_value)]
                ),
            )
    else:
        col_info = ColumnInfo(
            name="test_column",
            dtype=data_type,
            critical_data_info=CriticalDataInfo(
                cleaning_operations=[MissingValueImputation(imputed_value=imputed_value)]
            ),
        )
        cleaning_op = col_info.critical_data_info.cleaning_operations[0]
        assert cleaning_op.imputed_value == expected_imputed_value


@pytest.mark.parametrize(
    "data_type,imputed_value,list_param_values,expected_imputed_value,expected_list_param_values",
    [
        (DBVarType.VARCHAR, "missing", ["missing"], "missing", ["missing"]),
        (DBVarType.INT, None, ["-999", "-99"], None, [-999, -99]),
        (DBVarType.BOOL, False, [None, 0, ""], False, [None, False]),
    ],
)
def test_column_info__valid_construction_with_disguised_value_and_unexpected_value_imputation(
    data_type, imputed_value, list_param_values, expected_imputed_value, expected_list_param_values
):
    """Test ColumnInfo construction"""
    col_info = ColumnInfo(
        name="test_column",
        dtype=data_type,
        critical_data_info=CriticalDataInfo(
            cleaning_operations=[
                DisguisedValueImputation(imputed_value=imputed_value, disguised_values=list_param_values),
            ]
        ),
    )
    cleaning_op = col_info.critical_data_info.cleaning_operations[0]
    assert cleaning_op.imputed_value == expected_imputed_value
    assert cleaning_op.disguised_values == expected_list_param_values

    col_info = ColumnInfo(
        name="test_column",
        dtype=data_type,
        critical_data_info=CriticalDataInfo(
            cleaning_operations=[
                UnexpectedValueImputation(imputed_value=imputed_value, expected_values=list_param_values),
            ]
        ),
    )
    cleaning_op = col_info.critical_data_info.cleaning_operations[0]
    assert cleaning_op.imputed_value == expected_imputed_value
    assert cleaning_op.expected_values == expected_list_param_values


@pytest.mark.parametrize(
    "data_type,imputed_value,endpoint,expected_imputed_value,expected_endpoint",
    [
        (DBVarType.INT, 0, 0, 0, 0),
        (DBVarType.FLOAT, 0, 0, 0.0, 0.0),
        (DBVarType.DATE, None, "2020-01-01", None, "2020-01-01"),
        (DBVarType.TIME, None, "12:00:00", None, "12:00:00"),
        (DBVarType.TIMESTAMP, None, "2021-01-01 00:00:00-0800", None, "2021-01-01T08:00:00"),
    ],
)
def test_column_info__valid_construction_with_value_beyond_endpoint_imputation(
    data_type, imputed_value, endpoint, expected_imputed_value, expected_endpoint
):
    """Test ColumnInfo construction"""
    col_info = ColumnInfo(
        name="test_column",
        dtype=data_type,
        critical_data_info=CriticalDataInfo(
            cleaning_operations=[
                ValueBeyondEndpointImputation(type="less_than", imputed_value=imputed_value, end_point=endpoint)
            ]
        ),
    )
    cleaning_op = col_info.critical_data_info.cleaning_operations[0]
    assert cleaning_op.imputed_value == expected_imputed_value
    assert cleaning_op.end_point == expected_endpoint


NON_PRIMITIVE_TYPES = [var_type for var_type in DBVarType if var_type not in DBVarType.primitive_types()]


@pytest.mark.parametrize(
    "data_types,clean_op",
    [
        (
            NON_PRIMITIVE_TYPES,
            DisguisedValueImputation(imputed_value="some_value", disguised_values=["missing"]),
        ),
        (
            NON_PRIMITIVE_TYPES,
            UnexpectedValueImputation(imputed_value="some_value", expected_values=["missing"]),
        ),
        (
            NON_PRIMITIVE_TYPES + [DBVarType.BOOL, DBVarType.CHAR, DBVarType.VARCHAR],
            ValueBeyondEndpointImputation(imputed_value=0, end_point=0, type="less_than"),
        ),
    ],
)
def test_column_info__not_supported_casting(data_types, clean_op):
    """Test ColumnInfo construction"""
    for data_type in data_types:
        with pytest.raises(ValueError, match=f"Cleaning operation .* does not support dtype {data_type}"):
            ColumnInfo(
                name="test_column",
                dtype=data_type,
                critical_data_info=CriticalDataInfo(cleaning_operations=[clean_op]),
            )


def test_imputation_validation():
    """Test imputation validation"""
    with pytest.raises(ValueError, match="none is not an allowed value"):
        MissingValueImputation(imputed_value=None)

    with pytest.raises(ValueError, match="disguised_values cannot be empty"):
        DisguisedValueImputation(imputed_value=None, disguised_values=[])

    with pytest.raises(ValueError, match="expected_values cannot be empty"):
        UnexpectedValueImputation(imputed_value=None, expected_values=[])
