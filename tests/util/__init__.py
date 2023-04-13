"""
Module for utility functions used in tests.

Helper modules are registered using register_assert_rewrite() so that pytest assertion rewrite is
enabled, showing more readable assertion errors.
"""
import pytest

pytest.register_assert_rewrite("tests.util.helper")
