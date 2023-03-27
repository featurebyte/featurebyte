"""
Test join utils class
"""
from bson import ObjectId

from featurebyte.common.join_utils import (
    append_rsuffix_to_column_info,
    append_rsuffix_to_columns,
    combine_column_info_of_views,
    filter_columns,
    filter_columns_info,
    is_column_name_in_columns,
)
from featurebyte.enum import DBVarType
from featurebyte.models.base import PydanticObjectId
from featurebyte.query_graph.model.column_info import ColumnInfo


def test_filter_join_key_from_column():
    """
    Test filter_join_key_from_column
    """
    columns = ["colA", "colB", "colC"]
    filtered_columns = filter_columns(columns, ["randomCol"])
    assert filtered_columns == columns

    filtered_columns = filter_columns(columns, ["colA"])
    assert filtered_columns == ["colB", "colC"]

    filtered_columns = filter_columns(columns, [""])
    assert filtered_columns == columns


def test_filter_join_key_from_column_info():
    """
    Test filter_join_key_from_column_info
    """
    column_infos = [
        get_column_info("colA"),
        get_column_info("colB"),
        get_column_info("colC"),
    ]
    filtered_columns = filter_columns_info(column_infos, ["randomCol"])
    assert filtered_columns == column_infos

    filtered_columns = filter_columns_info(column_infos, ["colA"])
    assert filtered_columns == [get_column_info("colB"), get_column_info("colC")]

    filtered_columns = filter_columns_info(column_infos, [""])
    assert filtered_columns == column_infos


def test_append_rsuffix_to_column_info():
    """
    Test append_rsuffix_to_column_info
    """
    col_a_string = "colA"
    col_b_string = "colB"
    col_info_a, col_info_b = (
        ColumnInfo(name=col_a_string, dtype=DBVarType.INT),
        ColumnInfo(name=col_b_string, dtype=DBVarType.INT),
    )

    # Append w/ suffix
    suffix = "hello"
    output = append_rsuffix_to_column_info([col_info_a, col_info_b], suffix)
    assert len(output) == 2
    assert output[0].name == f"{col_a_string}{suffix}"
    assert output[1].name == f"{col_b_string}{suffix}"
    # Assert that original col_info's aren't changed
    col_info_a.name = col_a_string
    col_info_b.name = col_b_string

    # Append w/ suffix as empty string
    suffix = ""
    output = append_rsuffix_to_column_info([col_info_a, col_info_b], suffix)
    assert len(output) == 2
    assert output[0].name == col_a_string
    assert output[1].name == col_b_string

    # Append w/ suffix as None
    output = append_rsuffix_to_column_info([col_info_a, col_info_b], None)
    assert len(output) == 2
    assert output[0].name == col_a_string
    assert output[1].name == col_b_string


def test_append_rsuffix_to_columns():
    """
    Test _append_rsuffix_to_columns
    """
    columns = ["col_a", "col_b", "col_c"]
    results = append_rsuffix_to_columns(columns, "")
    assert columns == results

    results = append_rsuffix_to_columns(columns, "r")
    assert results == ["col_ar", "col_br", "col_cr"]


def get_pydantic_object_id(id_str: str) -> PydanticObjectId:
    """
    Helper method to get a pydantic object ID from a string.
    """
    return PydanticObjectId(ObjectId(id_str))


def get_column_info(name: str) -> ColumnInfo:
    """
    Helper function to get column info for a column name, and an arbitrary dtype.
    """
    return ColumnInfo(name=name, dtype=DBVarType.INT)


def test_combine_column_info_of_views():
    """
    Test combine_column_info_of_views
    """
    col1, col2, col3, col4, col5, col6 = (
        get_column_info("col1"),
        get_column_info("col2"),
        get_column_info("col3"),
        get_column_info("col4"),
        get_column_info("col5"),
        get_column_info("col6"),
    )
    columns_a = [col1, col2, col3]
    columns_b = [col4, col5, col6]
    # test that we combine them together
    result = combine_column_info_of_views(columns_a, columns_b)
    assert result == [col1, col2, col3, col4, col5, col6]

    # test that swapping the positions changes the ordering
    result = combine_column_info_of_views(columns_b, columns_a)
    assert result == [col4, col5, col6, col1, col2, col3]

    # test that filtering for a column works
    result = combine_column_info_of_views(columns_a, columns_b, filter_set={col5.name})
    assert result == [col1, col2, col3, col5]

    # test that filtering for a column that isn't in the second column set removes all of it
    result = combine_column_info_of_views(columns_a, columns_b, filter_set={col1.name})
    assert result == [col1, col2, col3]


def test_is_column_name_in_columns():
    """
    Test is_column_name_in_columns
    """
    col1, col2, col3 = (
        get_column_info("col1"),
        get_column_info("col2"),
        get_column_info("col3"),
    )
    assert is_column_name_in_columns("col2", [col1, col2, col3])
    assert not is_column_name_in_columns("col4", [col1, col2, col3])
    assert not is_column_name_in_columns("", [col1, col2, col3])
