"""
Tests for classes defined in featurebyte/query_graph/node/metadata/operation.py
"""
import pytest

from featurebyte.query_graph.enum import NodeType
from featurebyte.query_graph.node.metadata.operation import (
    DerivedDataColumn,
    NodeTransform,
    SourceDataColumn,
)


@pytest.fixture(name="source_col1")
def source_col1_fixture():
    """Source column fixture"""
    return SourceDataColumn(
        name="source_col1", tabular_data_id=None, tabular_data_type="event_data"
    )


@pytest.fixture(name="source_col2")
def source_col2_fixture():
    """Source column fixture"""
    return SourceDataColumn(
        name="source_col2", tabular_data_id=None, tabular_data_type="event_data"
    )


@pytest.fixture(name="transform_add")
def transform_add_fixture():
    """Transform (add) fixture"""
    return NodeTransform(node_type=NodeType.ADD, parameters={})


@pytest.fixture(name="transform_mul")
def transform_multiply_fixture():
    """Transform (multiply) fixture"""
    return NodeTransform(node_type=NodeType.MUL, parameters={})


@pytest.fixture(name="derived_col1")
def derived_col1_fixture(source_col1, source_col2, transform_add):
    """ "Derived column fixture"""
    return DerivedDataColumn(
        name="derived_col1", columns=[source_col1, source_col2], transforms=[transform_add]
    )


def test_flatten_columns(source_col1, source_col2, derived_col1, transform_add):
    """Test flatten_columns method"""
    # edge case
    columns, transforms = DerivedDataColumn._flatten_columns([])
    assert columns == transforms == []

    # simple case: source column
    columns, transforms = DerivedDataColumn._flatten_columns([source_col1])
    assert columns == [source_col1]
    assert transforms == []

    # simple case: derived column
    columns, transforms = DerivedDataColumn._flatten_columns([derived_col1])
    assert columns == [source_col1, source_col2]
    assert transforms == [transform_add]

    # general case
    columns, transforms = DerivedDataColumn._flatten_columns(
        [source_col1, source_col2, derived_col1]
    )
    assert columns == [source_col1, source_col2]
    assert transforms == [transform_add]


def test_derived_data_column_create(
    source_col1, source_col2, derived_col1, transform_add, transform_mul
):
    """Test create method"""
    derived_col = DerivedDataColumn.create(
        name="new_derived_col",
        columns=[source_col1, derived_col1],
        transform=transform_mul,
    )
    assert derived_col == DerivedDataColumn(
        name="new_derived_col",
        columns=[source_col1, source_col2],
        transforms=[transform_add, transform_mul],
    )
