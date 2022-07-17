"""
Tests for OrderedEnum class
"""
import pytest

from featurebyte.enum import OrderedEnum


@pytest.fixture(name="order_number")
def order_number_fixture():
    """
    OrderNumber test fixture
    """

    class OrderNumber(OrderedEnum):
        """
        OrderNumber class
        """

        ONE = "one"
        TWO = "two"
        THREE = "three"

    yield OrderNumber


def test_ordered_enum__ordering(order_number):
    """
    Test OrderedEnum ordering comparison
    """
    assert order_number.ONE == order_number("one")
    assert order_number.TWO == order_number("two")
    assert order_number.THREE == order_number("three")
    assert order_number.ONE < order_number.TWO
    assert order_number.TWO < order_number.THREE
    assert order_number.TWO > order_number.ONE
    assert order_number.THREE > order_number.TWO
    assert order_number.min() == order_number.ONE
    assert order_number.max() == order_number.THREE


def test_ordered_enum__invalid_comparison(order_number):
    """
    Test OrderedEnum (invalid comparison)
    """
    with pytest.raises(TypeError):
        assert order_number.TWO > "one"

    with pytest.raises(TypeError):
        assert "two" > order_number.ONE
