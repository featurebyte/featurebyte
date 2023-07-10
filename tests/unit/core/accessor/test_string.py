"""
Unit tests for core/accessor/string.py
"""
import pytest

from featurebyte.enum import DBVarType
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from tests.util.helper import get_preview_sql_for_series


def test_length_expression(varchar_series, expression_sql_template):
    """
    Test string accessor function (LengthNode)
    """
    str_len = varchar_series.str.len()
    assert str_len.dtype == DBVarType.INT
    assert str_len.node.type == NodeType.LENGTH
    assert str_len.node.output_type == NodeOutputType.SERIES
    expected_sql = expression_sql_template.format(expression='LENGTH("PRODUCT_ACTION")')
    assert get_preview_sql_for_series(str_len) == expected_sql


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
    assert series.dtype == DBVarType.VARCHAR
    assert series.node.type == NodeType.STR_CASE
    assert series.node.output_type == NodeOutputType.SERIES
    expected_sql = expression_sql_template.format(expression=exp_expression)
    assert get_preview_sql_for_series(series) == expected_sql


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
    assert series.dtype == DBVarType.VARCHAR
    assert series.node.type == NodeType.TRIM
    assert series.node.output_type == NodeOutputType.SERIES
    expected_sql = expression_sql_template.format(expression=exp_expression)
    assert get_preview_sql_for_series(series) == expected_sql


def test_replace_expression(
    varchar_series,
    expression_sql_template,
):
    """
    Test string accessor function (ReplaceNode)
    """
    str_replace = varchar_series.str.replace("hello", "kitty")
    assert str_replace.dtype == DBVarType.VARCHAR
    assert str_replace.node.type == NodeType.REPLACE
    assert str_replace.node.output_type == NodeOutputType.SERIES
    expected_sql = expression_sql_template.format(
        expression="REPLACE(\"PRODUCT_ACTION\", 'hello', 'kitty')"
    )
    assert get_preview_sql_for_series(str_replace) == expected_sql


@pytest.mark.parametrize(
    "accessor_func, exp_expression",
    [
        (
            lambda s: s.str.pad(width=10, side="left", fillchar="-"),
            """
            IFF(LENGTH("PRODUCT_ACTION") >= 10, "PRODUCT_ACTION", LPAD("PRODUCT_ACTION", 10, '-'))
            """,
        ),
        (
            lambda s: s.str.pad(width=9, side="right", fillchar="-"),
            """
            IFF(LENGTH("PRODUCT_ACTION") >= 9, "PRODUCT_ACTION", RPAD(\"PRODUCT_ACTION\", 9, '-'))
            """,
        ),
        (
            lambda s: s.str.pad(width=8, side="both", fillchar="-"),
            """
            IFF(
              LENGTH("PRODUCT_ACTION") >= 8,
              "PRODUCT_ACTION",
              RPAD(
                LPAD("PRODUCT_ACTION", 8 - CEIL((
                  8 - LENGTH("PRODUCT_ACTION")
                ) / 2), '-'),
                8,
                '-'
              )
            )
            """,
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
    assert series.dtype == DBVarType.VARCHAR
    assert series.node.type == NodeType.PAD
    assert series.node.output_type == NodeOutputType.SERIES
    expected_sql = expression_sql_template.format(expression=exp_expression)
    assert get_preview_sql_for_series(series) == expected_sql


@pytest.mark.parametrize(
    "accessor_func, exp_expression",
    [
        (lambda s: s.str.contains("abc"), "CONTAINS(\"PRODUCT_ACTION\", 'abc')"),
        (lambda s: s.str.contains("abc", case=True), "CONTAINS(\"PRODUCT_ACTION\", 'abc')"),
        (
            lambda s: s.str.contains("abc", case=False),
            "CONTAINS(LOWER(\"PRODUCT_ACTION\"), 'abc')",
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
    assert series.dtype == DBVarType.BOOL
    assert series.node.type == NodeType.STR_CONTAINS
    assert series.node.output_type == NodeOutputType.SERIES
    expected_sql = expression_sql_template.format(expression=exp_expression)
    assert get_preview_sql_for_series(series) == expected_sql


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
    assert series.dtype == DBVarType.VARCHAR
    assert series.node.type == NodeType.SUBSTRING
    assert series.node.output_type == NodeOutputType.SERIES
    expected_sql = expression_sql_template.format(expression=exp_expression)
    assert get_preview_sql_for_series(series) == expected_sql


def test_slice_expression__step_size_not_supported_or_exception(varchar_series):
    """
    Test string accessor function (slice, step size not supported)
    """
    with pytest.raises(ValueError) as exc:
        varchar_series.str[::2]
    assert "Can only use step size equals to 1." in str(exc.value)

    with pytest.raises(TypeError) as exc:
        varchar_series.str["hello"]
    assert 'type of argument "item" must be slice; got str instead' in str(exc.value)

    with pytest.raises(TypeError) as exc:
        varchar_series.str.pad(10, side="hello")
    expected_msg = (
        "the value of argument \"side\" must be one of ('left', 'right', 'both'); got hello instead"
    )
    assert expected_msg in str(exc.value)


def test_accessor_with_unsupported_var_type(int_series):
    """
    Test string accessor with non-supported var_type
    """
    with pytest.raises(AttributeError) as exc:
        int_series.str.len()
    assert "Can only use .str accessor with VARCHAR values!" in str(exc.value)
