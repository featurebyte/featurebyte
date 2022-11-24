"""
Test join utils class
"""
from bson import ObjectId

from featurebyte.api.join_utils import append_rsuffix_to_columns, join_tabular_data_ids
from featurebyte.models.base import PydanticObjectId


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
