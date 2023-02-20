"""Unit tests for SDK code generation"""
from featurebyte import EventView
from featurebyte.core.timedelta import to_timedelta
from tests.util.helper import check_sdk_code_generation


def test_sdk_code_generation__complex_arithmetic_expression(saved_event_data, update_fixtures):
    """Check SDK code generation for complex arithmetic expression"""
    to_use_saved_data, to_format = True, True
    event_view = EventView.from_event_data(event_data=saved_event_data)
    col_a, col_b = event_view["col_int"], event_view["col_float"]
    output = (
        (1 - col_a) * (col_b - 1) / (col_a + col_b)
        + 1 / (col_a - col_b)
        + col_a % 10
        - col_b.pow(2)
        + col_a
    )
    check_sdk_code_generation(
        output,
        to_use_saved_data=to_use_saved_data,
        to_format=to_format,
        fixture_path="tests/fixtures/sdk_code/complex_arithmetic_expression.py",
        update_fixtures=update_fixtures,
        to_compare_generated_code=True,
        data_id=saved_event_data.id,
    )


def test_sdk_code_generation__complex_relational_expression(saved_event_data, update_fixtures):
    """SDK code generation for complex relational expression"""
    to_use_saved_data, to_format = True, True
    event_view = EventView.from_event_data(event_data=saved_event_data)
    col_a = event_view["col_int"]
    output = (
        (col_a > 1) & (col_a < 10) | (col_a == 1) | (col_a != 10) | (col_a >= 1) | (col_a <= 10)
    )
    check_sdk_code_generation(
        output,
        to_use_saved_data=to_use_saved_data,
        to_format=to_format,
        fixture_path="tests/fixtures/sdk_code/complex_relational_expression.py",
        update_fixtures=update_fixtures,
        to_compare_generated_code=True,
        data_id=saved_event_data.id,
    )


def test_sdk_code_generation__complex_math_expression(saved_event_data, update_fixtures):
    """SDK code generation for complex math expression"""
    to_use_saved_data, to_format = True, True
    event_view = EventView.from_event_data(event_data=saved_event_data)
    col_a, col_b = event_view["col_int"], event_view["col_float"]
    output = (
        (col_a > 10).astype(int)
        - (~(col_a > 10)).astype(int)
        - col_b.abs().sqrt().ceil()
        + col_a.floor() * col_b.log() / col_a.exp()
        + col_a.isnull().astype(float)
    )
    check_sdk_code_generation(
        output,
        to_use_saved_data=to_use_saved_data,
        to_format=to_format,
        fixture_path="tests/fixtures/sdk_code/complex_math_expression.py",
        update_fixtures=update_fixtures,
        to_compare_generated_code=True,
        data_id=saved_event_data.id,
    )


def test_sdk_code_generation__complex_date_related_operations(saved_event_data, update_fixtures):
    """SDK code generation for complex date related operations"""
    to_use_saved_data, to_format = True, True
    event_view = EventView.from_event_data(event_data=saved_event_data)
    col_a = event_view["event_timestamp"]
    col_b = to_timedelta(event_view["col_int"], unit="hour")
    # create complex date property related operations
    output_date_prop = (
        col_a.dt.year
        + col_a.dt.quarter
        - col_a.dt.month * col_a.dt.week / col_a.dt.day % col_a.dt.day_of_week
        + col_a.dt.hour
        - col_a.dt.minute * col_a.dt.second
    )
    # create complex timedelta related operations
    output_timedelta = (
        col_b.dt.day * col_b.dt.hour
        - col_b.dt.minute / col_b.dt.second * col_b.dt.millisecond
        + col_b.dt.microsecond
    )
    # create complex date related operations
    output_date = (col_a + col_b).dt.second + (col_a - col_a).dt.minute
    output = output_date_prop + output_timedelta + output_date
    check_sdk_code_generation(
        output,
        to_use_saved_data=to_use_saved_data,
        to_format=to_format,
        fixture_path="tests/fixtures/sdk_code/complex_date_related_operations.py",
        update_fixtures=update_fixtures,
        to_compare_generated_code=False,  # skip comparison as local unit test result is different from CI
        data_id=saved_event_data.id,
    )


def test_sdk_code_generation__complex_string_related_operations(saved_event_data, update_fixtures):
    """SDK code generation for complex string related operations"""
    to_use_saved_data, to_format = True, True
    event_view = EventView.from_event_data(event_data=saved_event_data)
    col_a = event_view["col_text"]
    output = (
        col_a.str.len().astype(str)
        + col_a.str.strip(to_strip=" ")
        + col_a.str.replace(pat=" ", repl="_")
        + col_a.str.pad(width=10, side="left", fillchar="_")
        + col_a.str.upper()
        + col_a.str.lower()
        + col_a.str.contains(pat=" ", case=True).astype(str)
        + col_a.str.slice(start=0, stop=10)
    )
    check_sdk_code_generation(
        output,
        to_use_saved_data=to_use_saved_data,
        to_format=to_format,
        fixture_path="tests/fixtures/sdk_code/complex_string_related_operations.py",
        update_fixtures=update_fixtures,
        to_compare_generated_code=True,
        data_id=saved_event_data.id,
    )
