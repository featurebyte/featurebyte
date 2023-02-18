"""Unit tests for SDK code generation"""
from featurebyte import EventView
from tests.util.helper import check_sdk_code_generation, compare_generated_api_object_sdk_code


def test_sdk_code_generation__complex_arithmetic_expression(saved_event_data, update_fixtures):
    """Check SDK code generation for complex arithmetic expression"""
    to_use_saved_data = True
    event_view = EventView.from_event_data(event_data=saved_event_data)
    col_a, col_b = event_view["col_int"], event_view["col_float"]
    output = (
        (1 - col_a) * (col_b - 1) / (col_a + col_b)
        + 1 / (col_a - col_b)
        + col_a % 10
        - col_b.pow(2)
        + col_a
    )
    check_sdk_code_generation(output, to_use_saved_data=to_use_saved_data)
    compare_generated_api_object_sdk_code(
        api_object=output,
        data_id=saved_event_data.id,
        fixture_path="tests/fixtures/sdk_code/complex_arithmetic_expression.py",
        update_fixtures=update_fixtures,
        to_use_saved_data=to_use_saved_data,
    )


def test_sdk_code_generation__complex_relational_expression(saved_event_data, update_fixtures):
    """SDK code generation for complex relational expression"""
    to_use_saved_data = True
    event_view = EventView.from_event_data(event_data=saved_event_data)
    col_a = event_view["col_int"]
    output = (
        (col_a > 1) & (col_a < 10) | (col_a == 1) | (col_a != 10) | (col_a >= 1) | (col_a <= 10)
    )
    check_sdk_code_generation(output, to_use_saved_data=to_use_saved_data)
    compare_generated_api_object_sdk_code(
        api_object=output,
        data_id=saved_event_data.id,
        fixture_path="tests/fixtures/sdk_code/complex_relational_expression.py",
        update_fixtures=update_fixtures,
        to_use_saved_data=to_use_saved_data,
    )


def test_sdk_code_generation__complex_math_expression(saved_event_data, update_fixtures):
    """SDK code generation for complex math expression"""
    to_use_saved_data = True
    event_view = EventView.from_event_data(event_data=saved_event_data)
    col_a, col_b = event_view["col_int"], event_view["col_float"]
    output = (
        (col_a > 10).astype(int)
        - (~(col_a > 10)).astype(int)
        - col_b.abs().sqrt().ceil()
        + col_a.floor() * col_b.log() / col_a.exp()
        + col_a.isnull().astype(float)
    )
    check_sdk_code_generation(output, to_use_saved_data=to_use_saved_data)
    compare_generated_api_object_sdk_code(
        api_object=output,
        data_id=saved_event_data.id,
        fixture_path="tests/fixtures/sdk_code/complex_math_expression.py",
        update_fixtures=update_fixtures,
        to_use_saved_data=to_use_saved_data,
    )
