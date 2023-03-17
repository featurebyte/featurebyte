"""
Unit tests for DictTransform class
"""
import pytest

from featurebyte.schema.common.operation import DictProject, DictTransform


@pytest.mark.parametrize(
    "rule,input_value,expected",
    [
        ([], {}, {}),
        ((), {"a": 1}, {"a": 1}),
        ("a", {"a": 1}, 1),
        (("a",), {"a": 1}, 1),
        (["a", "b"], {"a": 1, "b": 2, "c": 3}, {"a": 1, "b": 2}),
        ((["a", "b"],), {"a": 1, "b": 2, "c": 3}, {"a": 1, "b": 2}),
        (("a", ["b", "c"], "b"), {"a": {"b": 1, "c": 2}}, 1),
        (("a", ["b", "c"], ["b"]), {"a": {"b": 1, "c": 2}}, {"b": 1}),
    ],
)
def test_dict_project(rule, input_value, expected):
    """Test DictProject"""
    dict_project = DictProject(rule=rule)
    output = dict_project.project(input_value)
    assert output == expected


@pytest.mark.parametrize(
    "rule,input_value,expected",
    [
        (("a",), {}, None),
        (("a", "b"), {}, None),
        (["a"], {}, {"a": None}),
        (["a", "b"], {}, {"a": None, "b": None}),
    ],
)
def test_dict_project__item_not_exists(rule, input_value, expected):
    """Test DictProject"""
    dict_project = DictProject(rule=rule)
    output = dict_project.project(input_value)
    assert output == expected


@pytest.fixture(name="input_dict")
def input_dict_fixture():
    """input_dict fixture"""
    return {
        "name": "event_data",
        "event_timestamp_column": "event_timestamp",
        "record_creation_date_column": "created_at",
        "columns_info": [
            {"name": "event_timestamp", "dtype": "timestamp"},
            {"name": "created_at", "dtype": "timestamp"},
            {
                "name": "cust_id",
                "dtype": "int",
                "entity": {"name": "customer", "serving_names": ["cust_id"]},
            },
        ],
    }


@pytest.mark.parametrize(
    "rule,expected",
    [
        (
            {"__root__": DictProject(rule=["name", "event_timestamp_column"])},
            {"name": "event_data", "event_timestamp_column": "event_timestamp"},
        ),
        (
            {"columns": DictProject(rule=("columns_info", ["name", "dtype", "entity"]))},
            {
                "columns": [
                    {"name": "event_timestamp", "dtype": "timestamp", "entity": None},
                    {"name": "created_at", "dtype": "timestamp", "entity": None},
                    {
                        "name": "cust_id",
                        "dtype": "int",
                        "entity": {"name": "customer", "serving_names": ["cust_id"]},
                    },
                ]
            },
        ),
    ],
)
def test_dict_transform(input_dict, rule, expected):
    """Test DictTransform"""
    dict_transform = DictTransform(rule=rule)
    output = dict_transform.transform(input_dict)
    assert output == expected


@pytest.mark.parametrize(
    "rule,expected",
    [
        (
            {
                "__root__": DictProject(rule=["name"]),
                "event_timestamp_column": DictProject(
                    rule=("event_timestamp_column",), verbose_only=True
                ),
            },
            {"name": "event_data"},
        ),
    ],
)
def test_dict_transform__not_verbose(input_dict, rule, expected):
    """Test DictTransform"""
    dict_transform = DictTransform(rule=rule)
    output = dict_transform.transform(input_dict, verbose=False)
    assert output == expected
