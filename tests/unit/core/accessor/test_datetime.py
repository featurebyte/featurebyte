"""
Unit tests for core/accessor/datetime.py
"""
import pytest

from featurebyte.core.accessor.datetime import DatetimeAccessor
from featurebyte.enum import DBVarType
from featurebyte.query_graph.enum import NodeOutputType, NodeType


@pytest.mark.parametrize(
    "accessor_func, exp_expression",
    [
        (lambda s: s.dt.year, 'EXTRACT(year FROM "TIMESTAMP")'),
        (lambda s: s.dt.quarter, 'EXTRACT(quarter FROM "TIMESTAMP")'),
        (lambda s: s.dt.month, 'EXTRACT(month FROM "TIMESTAMP")'),
        (lambda s: s.dt.week, 'EXTRACT(week FROM "TIMESTAMP")'),
        (lambda s: s.dt.day, 'EXTRACT(day FROM "TIMESTAMP")'),
        (lambda s: s.dt.day_of_week, '(EXTRACT(dayofweek FROM "TIMESTAMP") + 6) % 7'),
        (lambda s: s.dt.hour, 'EXTRACT(hour FROM "TIMESTAMP")'),
        (lambda s: s.dt.minute, 'EXTRACT(minute FROM "TIMESTAMP")'),
        (lambda s: s.dt.second, 'EXTRACT(second FROM "TIMESTAMP")'),
    ],
)
def test_datetime_property_extraction__timestamp(
    timestamp_series,
    expression_sql_template,
    accessor_func,
    exp_expression,
):
    """
    Test datetime accessor function (DatetimeExtractNode)
    """
    series = accessor_func(timestamp_series)
    assert series.var_type == DBVarType.INT
    assert series.node.type == NodeType.DT_EXTRACT
    assert series.node.output_type == NodeOutputType.SERIES
    expected_sql = expression_sql_template.format(expression=exp_expression)
    assert series.preview_sql() == expected_sql


def test_accessor_getattr__timestamp(timestamp_series):
    """
    Test __getattr__ works properly
    """
    # check that AttributeError exception is raised
    with pytest.raises(AttributeError):
        _ = timestamp_series.dt.random_attribute

    # check that able to access builtin attribute
    assert timestamp_series.dt.__class__ == DatetimeAccessor

    # check __dir__ magic method
    assert set(dir(timestamp_series.dt)) == {
        "year",
        "quarter",
        "month",
        "week",
        "day",
        "day_of_week",
        "hour",
        "minute",
        "second",
    }


@pytest.mark.parametrize(
    "accessor_func, exp_expression",
    [
        (lambda s: s.dt.hour, 'DATEDIFF(hour, "PROMOTION_START_DATE", "TIMESTAMP")'),
        (lambda s: s.dt.minute, 'DATEDIFF(minute, "PROMOTION_START_DATE", "TIMESTAMP")'),
        (lambda s: s.dt.second, 'DATEDIFF(second, "PROMOTION_START_DATE", "TIMESTAMP")'),
        (lambda s: s.dt.millisecond, 'DATEDIFF(millisecond, "PROMOTION_START_DATE", "TIMESTAMP")'),
        (lambda s: s.dt.microsecond, 'DATEDIFF(microsecond, "PROMOTION_START_DATE", "TIMESTAMP")'),
    ],
)
def test_datetime_property_extraction__timedelta(
    timedelta_series,
    expression_sql_template,
    accessor_func,
    exp_expression,
):
    """
    Test datetime accessor function (DatetimeExtractNode)
    """
    series = accessor_func(timedelta_series)
    assert series.var_type == DBVarType.INT
    assert series.node.type == NodeType.DATE_DIFF_UNIT
    assert series.node.output_type == NodeOutputType.SERIES
    expected_sql = expression_sql_template.format(expression=exp_expression)
    assert series.preview_sql() == expected_sql


def test_accessor_getattr__timedelta(timedelta_series):
    """
    Test __getattr__ works properly
    """
    # check that AttributeError exception is raised
    with pytest.raises(AttributeError):
        _ = timedelta_series.dt.random_attribute

    # check that able to access builtin attribute
    assert timedelta_series.dt.__class__ == DatetimeAccessor

    # check __dir__ magic method
    assert set(dir(timedelta_series.dt)) == {
        "hour",
        "minute",
        "second",
        "millisecond",
        "microsecond",
    }
