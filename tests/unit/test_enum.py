"""
Tests for OrderedEnum class
"""
from __future__ import annotations

import pytest

from featurebyte.enum import OrderedEnum, OrderedStrEnum


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


@pytest.fixture(name="order_str_number")
def order_str_number_fixture():
    """
    OrderStrNumber text fixture
    """

    class OrderStrNumber(OrderedStrEnum):
        """
        OrderStrNumber class
        """

        ONE = "one"
        TWO = "two"
        THREE = "three"

    yield OrderStrNumber


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


def test_ordered_str_enum__ordering(order_str_number):
    """
    Test OrderedStrEnum ordering comparison
    """
    assert order_str_number.ONE == order_str_number("one") == "one"
    assert order_str_number.TWO > "one"
    assert order_str_number("three") > "one"
    assert "three" > order_str_number.ONE
    assert order_str_number.ONE < "three"
    assert order_str_number("two") < "three"
    assert "one" < order_str_number("three")
    assert "two" < order_str_number.THREE
    assert order_str_number.ONE != "two"
    assert order_str_number.ONE != order_str_number("two")
    assert order_str_number.ONE != order_str_number.TWO
    assert order_str_number.max() == "three"
    assert order_str_number.max() == order_str_number("three")
    assert order_str_number.max() == order_str_number.THREE
    assert order_str_number.min() == "one"
    assert order_str_number.min() == order_str_number("one")
    assert order_str_number.min() == order_str_number.ONE


def test_hashable(order_str_number):
    """
    Test the OrderedStrEnum is hashable
    """
    order_map = {order_str_number.ONE: 1, order_str_number.TWO: 2}
    assert order_map[order_str_number.ONE] == 1
    assert order_map["one"] == 1
