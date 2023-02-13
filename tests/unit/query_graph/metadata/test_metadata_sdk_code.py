"""
Unit tests for featurebyte.query_graph.node.metadata.sdk_code
"""
import importlib

import pytest

from featurebyte.query_graph.enum import NodeOutputType
from featurebyte.query_graph.node.metadata.operation import NodeOutputCategory
from featurebyte.query_graph.node.metadata.sdk_code import (
    ClassEnum,
    CodeGenerator,
    ExpressionStr,
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
        (VariableNameStr("event_data"), "event_data"),
        (ExpressionStr("1 + 1"), "(1 + 1)"),
    ],
)
def test_variable_name_expression(obj, expected):
    """Test VariableNameStr and ExpressionStr"""
    assert obj.as_input() == expected


def test_import_tag():
    """Test enum in ImportTag can be imported properly"""
    for tag in ClassEnum:
        package_path, name = tag.value
        package = importlib.import_module(package_path)
        assert name in dir(package)


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
            node_output_type=output_type, node_output_category=output_cat
        )
        assert var_name == expected

    assert var_gen.convert_to_variable_name("event_data") == "event_data"
    assert var_gen.convert_to_variable_name("event_view") == "event_view"
    assert var_gen.convert_to_variable_name("event_view") == "event_view_1"
    assert var_gen.convert_to_variable_name("feat") == "feat_2"


def test_code_generator():
    """Test CodeGenerator"""
    code_gen = CodeGenerator()
    assert code_gen.generate() == "# Generated by SDK version: 0.1.0\n\n\n"

    code_gen.add_statements(
        statements=[(VariableNameStr("event_data"), ExpressionStr("EventData(...)"))],
        imports=[ClassEnum.EVENT_DATA],
    )
    assert code_gen.generate() == (
        "# Generated by SDK version: 0.1.0\n"
        "from featurebyte import EventData\n\n"
        "event_data = EventData(...)"
    )

    code_gen.add_statements(
        statements=[(VariableNameStr("event_data_1"), ExpressionStr("EventData(...)"))],
        imports=[ClassEnum.EVENT_DATA],
    )
    assert code_gen.generate() == (
        "# Generated by SDK version: 0.1.0\n"
        "from featurebyte import EventData\n\n"
        "event_data = EventData(...)\n"
        "event_data_1 = EventData(...)"
    )
