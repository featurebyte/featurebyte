"""
Test join utils class
"""
from bson import ObjectId

from featurebyte.api.join_utils import (
    append_rsuffix_to_column_info,
    append_rsuffix_to_columns,
    combine_column_info_of_views,
    join_column_lineage_map,
    join_tabular_data_ids,
    update_column_lineage_map_with_suffix,
)
from featurebyte.enum import DBVarType
from featurebyte.models.base import PydanticObjectId
from featurebyte.models.feature_store import ColumnInfo


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


def test_join_tabular_data_ids__empty():
    """
    Test joining two empty lists
    """
    output = join_tabular_data_ids([], [])
    assert output == []


def get_pydantic_object_id(id_str: str) -> PydanticObjectId:
    """
    Helper method to get a pydantic object ID from a string.
    """
    return PydanticObjectId(ObjectId(id_str))


def test_join_tabular_data_ids__join():
    """
    Test joining of items
    """
    base_object_id = "6337f9651050ee7d1231111"
    object_2 = get_pydantic_object_id(f"{base_object_id}2")
    output = join_tabular_data_ids([object_2], [])
    assert output == [object_2]

    output = join_tabular_data_ids([], [object_2])
    assert output == [object_2]

    object_0 = get_pydantic_object_id(f"{base_object_id}0")
    object_3 = get_pydantic_object_id(f"{base_object_id}3")
    object_5 = get_pydantic_object_id(f"{base_object_id}5")
    object_9 = get_pydantic_object_id(f"{base_object_id}9")
    object_a = get_pydantic_object_id(f"{base_object_id}a")
    data_ids_a = [object_2, object_0, object_5]
    data_ids_b = [object_3, object_a, object_9]
    output = join_tabular_data_ids(data_ids_a, data_ids_b)
    assert output == [object_0, object_2, object_3, object_5, object_9, object_a]


def test_join_column_lineage_map():
    """
    Test join_column_lineage_map
    """
    lineage_a = {"colA": ("node1", "node2", "node3")}
    lineage_b = {"colB": ("nodeX", "nodeY", "nodeZ")}
    column_filter = ["colB"]
    new_node_name = "nodeNew"

    # assert that filtering and appending works properly
    response = join_column_lineage_map(lineage_a, lineage_b, column_filter, new_node_name)
    assert response == {
        "colA": ("node1", "node2", "node3", "nodeNew"),
        "colB": ("nodeX", "nodeY", "nodeZ", "nodeNew"),
    }

    # assert that no filter present will not add on any columns
    response = join_column_lineage_map(lineage_a, lineage_b, [], new_node_name)
    assert response == {
        "colA": ("node1", "node2", "node3", "nodeNew"),
    }

    # assert that random filter present will not add on any columns
    response = join_column_lineage_map(lineage_a, lineage_b, ["colRandom"], new_node_name)
    assert response == {
        "colA": ("node1", "node2", "node3", "nodeNew"),
    }


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

    # test that passing an empty filter set doesn't perform any filtering
    result = combine_column_info_of_views(columns_a, columns_b, filter_set=set())
    assert result == [col1, col2, col3, col4, col5, col6]


def test_update_column_lineage_map_with_suffix():
    """
    Test update_column_lineage_map_with_suffix
    """
    node_a = ("node1", "node2", "node3")
    node_b = ("nodeX", "nodeY", "nodeZ")
    input_lineage = {"colA": node_a, "colB": node_b}

    # no update if no suffix passed in
    output_lineage = update_column_lineage_map_with_suffix(input_lineage, None)
    assert output_lineage == input_lineage

    # update if no suffix passed in
    output_lineage = update_column_lineage_map_with_suffix(input_lineage, "right")
    assert output_lineage == {"colAright": node_a, "colBright": node_b}
