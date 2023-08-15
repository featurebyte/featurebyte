"""
Test vector validator module
"""
import pytest

from featurebyte.api.aggregator.vector_validator import validate_vector_aggregate_parameters
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
    vector_agg_func_types = {AggFunc.MAX, AggFunc.AVG}
    for agg_func in AggFunc.all():
        for column_info in test_columns_info:
            # Throw an error if the column dtype is an array and the agg_func is not supported
            if column_info.dtype == DBVarType.ARRAY and agg_func in vector_agg_func_types:
                with pytest.raises(ValueError) as exc_info:
                    validate_vector_aggregate_parameters([column_info], column_info.name, agg_func)
                assert (
                    str(exc_info.value)
                    == f"Method {agg_func} is not supported for vector aggregate operations."
                )
            else:
                validate_vector_aggregate_parameters([column_info], column_info.name, agg_func)
