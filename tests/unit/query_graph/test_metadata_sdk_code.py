"""
Unit tests for featurebyte.query_graph.node.metadata.sdk_code
"""

import importlib
import textwrap

import pytest
from bson import ObjectId

from featurebyte.query_graph.enum import NodeOutputType
from featurebyte.query_graph.node.metadata.operation import NodeOutputCategory
from featurebyte.query_graph.node.metadata.sdk_code import (
    ClassEnum,
    CodeGenerator,
    ExpressionStr,
    StatementStr,
    ValueStr,
    VariableNameGenerator,
    VariableNameStr,
)


@pytest.mark.parametrize(
    "value, expected, expected_as_input",
    [
        (123, "123", "123"),
        ("abc", '"abc"', '"abc"'),
        ("'abc'", "\"'abc'\"", "\"'abc'\""),
        ([123, "abc"], "[123, 'abc']", "[123, 'abc']"),
    ],
)
def test_value_string(value, expected, expected_as_input):
    """Test ValueStr class"""
    obj = ValueStr.create(value)
    assert str(obj) == expected
    assert obj.as_input() == expected_as_input
    assert eval(obj) == value


@pytest.mark.parametrize(
    "obj, expected",
    [
        (VariableNameStr("event_table"), "event_table"),
        (ExpressionStr("1 + 1"), "(1 + 1)"),
    ],
)
def test_variable_name_expression(obj, expected):
    """Test VariableNameStr and ExpressionStr"""
    assert obj.as_input() == expected


def test_class_enum__module_import():
    """Test enum in ClassEnum can be imported properly"""
    for tag in ClassEnum:
        package_path, name = tag.value
        package = importlib.import_module(package_path)
        assert name in dir(package)


def test_class_enum_and_object_class():
    """Test ClassEnum & ObjectClass interaction"""
    object_id = ObjectId("63eaeafcbe3a62da29705ad1")
    event_table = ClassEnum.EVENT_TABLE(ClassEnum.OBJECT_ID(object_id), name="event_table")
    assert (
        str(event_table)
        == repr(event_table)
        == 'EventTable(ObjectId("63eaeafcbe3a62da29705ad1"), name="event_table")'
    )
    assert event_table.extract_import() == {ClassEnum.EVENT_TABLE.value, ClassEnum.OBJECT_ID.value}

    # check ObjectClass object inside containers (list & dict)
    event_table = ClassEnum.EVENT_TABLE(
        id=ClassEnum.OBJECT_ID(object_id),
        list_value=[ClassEnum.COLUMN_INFO(name="column")],
        dict_value={
            "key": ClassEnum.TABULAR_SOURCE("some_value"),
            "list_value": [ClassEnum.COLUMN_INFO(name="other_column")],
        },
    )
    expected_str = (
        'EventTable(id=ObjectId("63eaeafcbe3a62da29705ad1"), list_value=[ColumnInfo(name="column")], '
        "dict_value={'key': TabularSource(\"some_value\"), 'list_value': [ColumnInfo(name=\"other_column\")]})"
    )
    assert str(event_table) == repr(event_table) == expected_str
    assert event_table.extract_import() == {
        ClassEnum.EVENT_TABLE.value,
        ClassEnum.OBJECT_ID.value,
        ClassEnum.COLUMN_INFO.value,
        ClassEnum.TABULAR_SOURCE.value,
    }

    # check _method_name
    event_table = ClassEnum.EVENT_TABLE(ClassEnum.OBJECT_ID("1234"), _method_name="get_by_id")
    assert str(event_table) == repr(event_table) == 'EventTable.get_by_id(ObjectId("1234"))'
    assert event_table.extract_import() == {ClassEnum.EVENT_TABLE.value, ClassEnum.OBJECT_ID.value}


def test_variable_name_generator():
    """Test VariableNameGenerator"""
    var_gen = VariableNameGenerator()
    input_params_expected_pairs = [
        ((NodeOutputType.SERIES, NodeOutputCategory.VIEW), "col"),
        ((NodeOutputType.FRAME, NodeOutputCategory.VIEW), "view"),
        ((NodeOutputType.SERIES, NodeOutputCategory.FEATURE), "feat"),
        ((NodeOutputType.FRAME, NodeOutputCategory.FEATURE), "grouped"),
        ((NodeOutputType.SERIES, NodeOutputCategory.VIEW), "col_1"),
        ((NodeOutputType.FRAME, NodeOutputCategory.VIEW), "view_1"),
        ((NodeOutputType.SERIES, NodeOutputCategory.FEATURE), "feat_1"),
        ((NodeOutputType.FRAME, NodeOutputCategory.FEATURE), "grouped_1"),
    ]
    for (output_type, output_cat), expected in input_params_expected_pairs:
        var_name = var_gen.generate_variable_name(
            node_output_type=output_type,
            node_output_category=output_cat,
            node_name=None,
        )
        assert var_name == expected

    assert var_gen.convert_to_variable_name("event_table", node_name=None) == "event_table"
    assert var_gen.convert_to_variable_name("event_view", node_name=None) == "event_view"
    assert var_gen.convert_to_variable_name("event_view", node_name=None) == "event_view_1"
    assert var_gen.convert_to_variable_name("feat", node_name=None) == "feat_2"


@pytest.mark.parametrize(
    "one_based,inputs,expected_var_name,expected_count",
    [
        (False, [], None, None),
        (True, [], None, None),
        (False, ["col"], "col", 1),
        (True, ["col"], "col_1", 1),
        (False, ["col", "col", "col", "foo", "bar"], "col_2", 3),
        (True, ["col", "col", "col", "foo", "bar"], "col_3", 3),
    ],
)
def test_variable_name_generator__get_latest_variable_name(
    one_based, inputs, expected_var_name, expected_count
):
    """Test VariableNameGenerator.get_latest_variable_name()"""
    var_gen = VariableNameGenerator(one_based=one_based)
    for var_name_prefix in inputs:
        _ = var_gen.convert_to_variable_name(
            variable_name_prefix=var_name_prefix,
            node_name=None,
        )

    if expected_var_name:
        latest_var_name = var_gen.get_latest_variable_name(variable_name_prefix="col")
        assert latest_var_name == expected_var_name
        assert var_gen.var_name_counter["col"] == expected_count
    else:
        with pytest.raises(ValueError, match="Variable name prefix col does not exist"):
            var_gen.get_latest_variable_name(variable_name_prefix="col")


def test_code_generator():
    """Test CodeGenerator"""
    code_gen = CodeGenerator()
    code_gen.add_statements(
        statements=[(VariableNameStr("event_table"), ClassEnum.EVENT_TABLE(name="event_table"))],
    )
    assert code_gen.generate(remove_unused_variables=False).strip() == (
        'from featurebyte import EventTable\n\nevent_table = EventTable(name="event_table")'
    )

    code_gen.add_statements(
        statements=[
            (VariableNameStr("event_table_1"), ClassEnum.EVENT_TABLE(name="another_event_table"))
        ],
    )
    assert code_gen.generate(remove_unused_variables=False).strip() == (
        "from featurebyte import EventTable\n\n"
        'event_table = EventTable(name="event_table")\n'
        'event_table_1 = EventTable(name="another_event_table")'
    )


def test_code_generator__on_demand_view():
    """Test CodeGenerator for on-demand view template"""
    code_gen = CodeGenerator(template="on_demand_view.tpl")
    code_gen.add_statements(
        statements=[
            (VariableNameStr("feat1"), ExpressionStr("feat + 1")),
            (VariableNameStr('output_df["feat"]'), ExpressionStr("feat1")),
        ],
    )
    codes = code_gen.generate(
        function_name="on_demand_feature_view_func",
        input_df_name="input_df",
        output_df_name="output_df",
    )
    assert (
        codes.strip()
        == textwrap.dedent(
            """
        import datetime
        import json
        import numpy as np
        import pandas as pd
        import scipy as sp


        def on_demand_feature_view_func(input_df: pd.DataFrame) -> pd.DataFrame:
            output_df = pd.DataFrame()
            feat1 = feat + 1
            output_df["feat"] = feat1
            return output_df
        """
        ).strip()
    )


def test_code_generator__on_demand_function():
    """Test CodeGenerator for on-demand function template"""
    code_gen = CodeGenerator(template="on_demand_function.tpl")
    code_gen.add_statements(
        statements=[
            (VariableNameStr("feat"), ExpressionStr("input1 + 1")),
            (StatementStr("return feat")),
        ],
    )
    py_codes = code_gen.generate(
        py_function_name="on_demand_feature_func",
        py_function_params="input1: float",
        py_return_type="float",
        py_comment="# This is a comment",
        output_var_name="output",
    ).strip()
    assert (
        py_codes.strip()
        == textwrap.dedent(
            """
        import datetime
        import json
        import numpy as np
        import pandas as pd
        import scipy as sp


        def on_demand_feature_func(input1: float) -> float:
            # This is a comment
            feat = input1 + 1
            return feat
        """
        ).strip()
    )

    code_gen = CodeGenerator(template="on_demand_function_sql.tpl")
    sql_codes = code_gen.generate(
        sql_function_name="ml.feature_engineering.on_demand_func",
        sql_function_params="x1 FLOAT",
        sql_return_type="FLOAT",
        sql_comment="On demand function used for feature engineering",
        py_function_name="on_demand_feature_func",
        input_arguments="x1",
        py_function_body=py_codes,
    )
    assert (
        sql_codes.strip()
        == textwrap.dedent(
            """
            CREATE FUNCTION ml.feature_engineering.on_demand_func(x1 FLOAT)
            RETURNS FLOAT
            LANGUAGE PYTHON
            COMMENT 'On demand function used for feature engineering'
            AS $$
            import datetime
            import json
            import numpy as np
            import pandas as pd
            import scipy as sp


            def on_demand_feature_func(input1: float) -> float:
                # This is a comment
                feat = input1 + 1
                return feat

            output = on_demand_feature_func(x1)
            return None if pd.isnull(output) else output
            $$
            """
        ).strip()
    )
