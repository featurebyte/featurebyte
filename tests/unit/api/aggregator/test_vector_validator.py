"""
Test vector validator module
"""

import pytest

from featurebyte.api.aggregator.vector_validator import (
    VECTOR_AGGREGATE_SUPPORTED_FUNCTIONS,
    validate_vector_aggregate_parameters,
)
from featurebyte.enum import AggFunc, DBVarType
from featurebyte.query_graph.model.column_info import ColumnInfo


def test_validate_vector_aggregate_parameters():
    """
    Test validate vector aggregate parameters
    """
    test_columns_info = [
        ColumnInfo(name="a", dtype=DBVarType.INT),
        ColumnInfo(name="b", dtype=DBVarType.FLOAT),
        ColumnInfo(name="c", dtype=DBVarType.ARRAY),
    ]
    for agg_func in AggFunc.all():
        # Verify that when value_column is None, we return None.
        assert not validate_vector_aggregate_parameters(test_columns_info, None, agg_func)

        for column_info in test_columns_info:
            # Throw an error if the column dtype is an array and the agg_func is not supported
            if (
                column_info.dtype == DBVarType.ARRAY
                and agg_func not in VECTOR_AGGREGATE_SUPPORTED_FUNCTIONS
                and agg_func != AggFunc.LATEST
            ):
                with pytest.raises(ValueError) as exc_info:
                    validate_vector_aggregate_parameters([column_info], column_info.name, agg_func)
                assert (
                    str(exc_info.value)
                    == f"Method {agg_func} is not supported for vector aggregate operations."
                )
            else:
                assert not validate_vector_aggregate_parameters(
                    [column_info], column_info.name, agg_func
                )
