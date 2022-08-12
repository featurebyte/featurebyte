"""
Unit tests for core/accessor/string.py
"""
import textwrap

import pytest

from featurebyte.enum import DBVarType
from featurebyte.query_graph.enum import NodeOutputType, NodeType


@pytest.fixture(name="expression_sql_template")
def expression_sql_template_fixture():
    """SQL template used to construct the expect sql code"""
    template_sql = """
    SELECT
      {expression}
    FROM (
        SELECT
          "CUST_ID" AS "CUST_ID",
          "PRODUCT_ACTION" AS "PRODUCT_ACTION",
          "VALUE" AS "VALUE",
          "MASK" AS "MASK"
        FROM "db"."public"."transaction"
    )
    LIMIT 10
    """
    return textwrap.dedent(template_sql).strip()


def test_length_expression(varchar_series, expression_sql_template):
    """
    Test string accessor function (LengthNode)
    """
    str_len = varchar_series.str.len()
    assert str_len.var_type == DBVarType.INT
    assert str_len.node.type == NodeType.LENGTH
    assert str_len.node.output_type == NodeOutputType.SERIES
    expected_sql = expression_sql_template.format(expression='LENGTH("PRODUCT_ACTION")')
    assert str_len.preview_sql() == expected_sql


@pytest.mark.parametrize(
    "accessor_func, exp_expression",
    [
        (lambda s: s.str.lower(), 'LOWER("PRODUCT_ACTION")'),
        (lambda s: s.str.upper(), 'UPPER("PRODUCT_ACTION")'),
    ],
)
def test_str_case_expression(
    varchar_series,
    expression_sql_template,
    accessor_func,
    exp_expression,
):
    """
    Test string accessor function (StringCaseNode)
    """
    series = accessor_func(varchar_series)
    assert series.var_type == DBVarType.VARCHAR
    assert series.node.type == NodeType.STRCASE
    assert series.node.output_type == NodeOutputType.SERIES
    expected_sql = expression_sql_template.format(expression=exp_expression)
    assert series.preview_sql() == expected_sql


@pytest.mark.parametrize(
    "accessor_func, exp_expression",
    [
        (lambda s: s.str.strip(), 'TRIM("PRODUCT_ACTION")'),
        (lambda s: s.str.strip("x"), "TRIM(\"PRODUCT_ACTION\", 'x')"),
        (lambda s: s.str.lstrip(), 'LTRIM("PRODUCT_ACTION")'),
        (lambda s: s.str.lstrip("y"), "LTRIM(\"PRODUCT_ACTION\", 'y')"),
        (lambda s: s.str.rstrip(), 'RTRIM("PRODUCT_ACTION")'),
        (lambda s: s.str.rstrip("z"), "RTRIM(\"PRODUCT_ACTION\", 'z')"),
    ],
)
def test_strip_expression(
    varchar_series,
    expression_sql_template,
    accessor_func,
    exp_expression,
):
    """
    Test string accessor function (TrimNode)
    """
    series = accessor_func(varchar_series)
    assert series.var_type == DBVarType.VARCHAR
    assert series.node.type == NodeType.TRIM
    assert series.node.output_type == NodeOutputType.SERIES
    expected_sql = expression_sql_template.format(expression=exp_expression)
    assert series.preview_sql() == expected_sql


def test_replace_expression(
    varchar_series,
    expression_sql_template,
):
    """
    Test string accessor function (ReplaceNode)
    """
    str_replace = varchar_series.str.replace("hello", "kitty")
    assert str_replace.var_type == DBVarType.VARCHAR
    assert str_replace.node.type == NodeType.REPLACE
    assert str_replace.node.output_type == NodeOutputType.SERIES
    expected_sql = expression_sql_template.format(
        expression="REPLACE(\"PRODUCT_ACTION\", 'hello', 'kitty')"
    )
    assert str_replace.preview_sql() == expected_sql


@pytest.mark.parametrize(
    "accessor_func, exp_expression",
    [
        (
            lambda s: s.str.pad(width=10, side="left", fillchar="-"),
            (
                "CASE\n"
                '    WHEN LENGTH("PRODUCT_ACTION") >= 10 THEN "PRODUCT_ACTION"\n'
                "    ELSE LPAD(\"PRODUCT_ACTION\", 10, '-')\n"
                "  END"
            ),
        ),
        (
            lambda s: s.str.pad(width=9, side="right", fillchar="-"),
            (
                "CASE\n"
                '    WHEN LENGTH("PRODUCT_ACTION") >= 9 THEN "PRODUCT_ACTION"\n'
                "    ELSE RPAD(\"PRODUCT_ACTION\", 9, '-')\n"
                "  END"
            ),
        ),
        (
            lambda s: s.str.pad(width=8, side="both", fillchar="-"),
            (
                "CASE\n"
                '    WHEN LENGTH("PRODUCT_ACTION") >= 8 THEN "PRODUCT_ACTION"\n'
                "    ELSE RPAD(LPAD(\"PRODUCT_ACTION\", 8 - CEIL((8 - LENGTH(\"PRODUCT_ACTION\")) / 2), '-'), 8, '-')\n"
                "  END"
            ),
        ),
    ],
)
def test_pad_expression(
    varchar_series,
    expression_sql_template,
    accessor_func,
    exp_expression,
):
    """
    Test string accessor function (PadNode)
    """
    series = accessor_func(varchar_series)
    assert series.var_type == DBVarType.VARCHAR
    assert series.node.type == NodeType.PAD
    assert series.node.output_type == NodeOutputType.SERIES
    expected_sql = expression_sql_template.format(expression=exp_expression)
    assert series.preview_sql() == expected_sql


@pytest.mark.parametrize(
    "accessor_func, exp_expression",
    [
        (lambda s: s.str.contains("abc"), "CONTAINS(\"PRODUCT_ACTION\", 'abc')"),
        (lambda s: s.str.contains("abc", case=True), "CONTAINS(\"PRODUCT_ACTION\", 'abc')"),
        (
            lambda s: s.str.contains("abc", case=False),
            "CONTAINS(LOWER(\"PRODUCT_ACTION\"), LOWER('abc'))",
        ),
    ],
)
def test_contains_expression(
    varchar_series,
    expression_sql_template,
    accessor_func,
    exp_expression,
):
    """
    Test string accessor function (StringContainNode)
    """
    series = accessor_func(varchar_series)
    assert series.var_type == DBVarType.BOOL
    assert series.node.type == NodeType.STRCONTAINS
    assert series.node.output_type == NodeOutputType.SERIES
    expected_sql = expression_sql_template.format(expression=exp_expression)
    assert series.preview_sql() == expected_sql


@pytest.mark.parametrize(
    "accessor_func, exp_expression",
    [
        (lambda s: s.str[:], 'SUBSTRING("PRODUCT_ACTION", 0)'),
        (lambda s: s.str[:5], 'SUBSTRING("PRODUCT_ACTION", 0, 5)'),
        (lambda s: s.str[5:], 'SUBSTRING("PRODUCT_ACTION", 5)'),
        (lambda s: s.str[:5:1], 'SUBSTRING("PRODUCT_ACTION", 0, 5)'),
        (lambda s: s.str[5::1], 'SUBSTRING("PRODUCT_ACTION", 5)'),
        (lambda s: s.str.slice(None, None, None), 'SUBSTRING("PRODUCT_ACTION", 0)'),
    ],
)
def test_slice_expression(
    varchar_series,
    expression_sql_template,
    accessor_func,
    exp_expression,
):
    """
    Test string accessor function (SubStringNode)
    """
    series = accessor_func(varchar_series)
    assert series.var_type == DBVarType.VARCHAR
    assert series.node.type == NodeType.SUBSTRING
    assert series.node.output_type == NodeOutputType.SERIES
    expected_sql = expression_sql_template.format(expression=exp_expression)
    assert series.preview_sql() == expected_sql


def test_slice_expression__step_size_not_supported(varchar_series):
    """
    Test string accessor function (slice, step size not supported)
    """
    with pytest.raises(ValueError) as exc:
        varchar_series.str[::2]
    assert "Can only use step size equals to 1." in str(exc.value)


def test_accessor_with_unsupported_var_type(int_series):
    """
    Test string accessor with non-supported var_type
    """
    with pytest.raises(AttributeError) as exc:
        int_series.str.len()
    assert "Can only use .str accessor with VARCHAR values!" in str(exc.value)
