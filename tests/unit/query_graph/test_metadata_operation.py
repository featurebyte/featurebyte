"""
Tests for classes defined in featurebyte/query_graph/node/metadata/operation.py
"""
import pytest

from featurebyte.enum import DBVarType
from featurebyte.query_graph.enum import NodeType
from featurebyte.query_graph.node.metadata.operation import DerivedDataColumn, SourceDataColumn


@pytest.fixture(name="source_col1")
def source_col1_fixture():
    """Source column fixture"""
    return SourceDataColumn(
        name="source_col1",
        tabular_data_id=None,
        tabular_data_type="event_data",
        node_names={"input_1"},
        dtype=DBVarType.FLOAT,
    )


@pytest.fixture(name="source_col2")
def source_col2_fixture():
    """Source column fixture"""
    return SourceDataColumn(
        name="source_col2",
        tabular_data_id=None,
        tabular_data_type="event_data",
        node_names={"input_1"},
        dtype=DBVarType.INT,
    )


@pytest.fixture(name="transform_add")
def transform_add_fixture():
    """Transform (add) fixture"""
    return "add"


@pytest.fixture(name="transform_mul")
def transform_multiply_fixture():
    """Transform (multiply) fixture"""
    return "mul"


@pytest.fixture(name="derived_col1")
def derived_col1_fixture(source_col1, source_col2, transform_add):
    """ "Derived column fixture"""
    return DerivedDataColumn.create(
        name="derived_col1",
        columns=[source_col1, source_col2],
        transform=transform_add,
        node_name="add_1",
        dtype=DBVarType.FLOAT,
    )


@pytest.fixture(name="derived_col2")
def derived_col2_fixture(derived_col1):
    """ "Derived column fixture"""
    return DerivedDataColumn.create(
        name="derived_col2",
        columns=[derived_col1],
        transform="alias",
        node_name="alias_1",
        dtype=DBVarType.FLOAT,
    )


def test_flatten_columns(source_col1, source_col2, derived_col1, derived_col2, transform_add):
    """Test flatten_columns method"""
    # edge case
    columns, transforms, node_names = DerivedDataColumn._flatten_columns([])
    assert columns == transforms == []
    assert node_names == set()

    # simple case: source column
    columns, transforms, node_names = DerivedDataColumn._flatten_columns([source_col1])
    assert columns == [source_col1]
    assert transforms == []
    assert node_names == {"input_1"}

    # simple case: derived column
    columns, transforms, node_names = DerivedDataColumn._flatten_columns([derived_col1])
    assert columns == [source_col1, source_col2]
    assert transforms == [transform_add]
    assert node_names == {"input_1", "add_1"}

    columns, transforms, node_names = DerivedDataColumn._flatten_columns([derived_col2])
    assert columns == [source_col1, source_col2]
    assert transforms == [transform_add, "alias"]
    assert node_names == {"input_1", "add_1", "alias_1"}

    # general case
    columns, transforms, node_names = DerivedDataColumn._flatten_columns(
        [source_col1, source_col2, derived_col1]
    )
    assert columns == [source_col1, source_col2]
    assert transforms == [transform_add]
    assert node_names == {"input_1", "add_1"}


def test_derived_data_column_create(
    source_col1, source_col2, derived_col1, transform_add, transform_mul
):
    """Test create method"""
    derived_col = DerivedDataColumn.create(
        name="new_derived_col",
        columns=[source_col1, derived_col1],
        transform=transform_mul,
        node_name="mul_1",
        dtype=DBVarType.FLOAT,
    )
    assert derived_col == DerivedDataColumn(
        name="new_derived_col",
        columns=[source_col1, source_col2],
        transforms=[transform_add, transform_mul],
        node_names={"input_1", "add_1", "mul_1"},
        dtype=DBVarType.FLOAT,
    )


def test_insert_column():
    """Test insert column"""
    col1 = SourceDataColumn(
        name="col1",
        tabular_data_id=None,
        tabular_data_type="event_data",
        node_names={"input_1", "project_1"},
        dtype=DBVarType.FLOAT,
    )
    another_col1 = col1.clone(node_names={"input_1", "filter_1"}, filter=True)
    col_map = DerivedDataColumn.insert_column(
        DerivedDataColumn.insert_column({}, col1), another_col1
    )
    assert col_map == {
        "col1": {
            "name": "col1",
            "node_names": {"input_1", "project_1", "filter_1"},
            "tabular_data_id": None,
            "tabular_data_type": "event_data",
            "type": "source",
            "filter": True,
            "dtype": "FLOAT",
        }
    }


def test_data_column_clone_with_replacement(source_col1):
    """Test data column clone_with_replacement"""
    # case 1: when the node name found in the replace_node_name_map
    output = source_col1.clone_without_internal_nodes(
        proxy_node_name_map={"input_1": {"input_1", "project_1"}},
        graph_node_name="graph_1",
        graph_node_transform="graph",
    )
    assert output == SourceDataColumn(
        name=source_col1.name,
        tabular_data_id=source_col1.tabular_data_id,
        tabular_data_type=source_col1.tabular_data_type,
        node_names={"input_1", "project_1"},
        dtype=DBVarType.FLOAT,
    )

    # case 2: when the node name not found in the replace_node_name_map
    output = source_col1.clone_without_internal_nodes(
        proxy_node_name_map={"project_2": {"project_2"}},
        graph_node_name="graph_1",
        graph_node_transform="graph",
    )
    assert output == SourceDataColumn(
        name=source_col1.name,
        tabular_data_id=source_col1.tabular_data_id,
        tabular_data_type=source_col1.tabular_data_type,
        node_names={"graph_1"},
        dtype=DBVarType.FLOAT,
    )


def test_derived_data_column_clone_with_replacement(derived_col1):
    """Test derived data column clone_with_replacement"""
    # case 1: when all the node names found in the replace_node_name_map
    assert derived_col1.node_names == {"input_1", "add_1"}
    output = derived_col1.clone_without_internal_nodes(
        proxy_node_name_map={"input_1": {"input_2"}, "add_1": {"add_2"}},
        graph_node_name="graph_1",
        graph_node_transform="graph",
    )
    assert output == {
        "columns": [
            {
                "filter": False,
                "name": "source_col1",
                "node_names": {"input_2"},
                "tabular_data_id": None,
                "tabular_data_type": "event_data",
                "type": "source",
                "dtype": "FLOAT",
            },
            {
                "filter": False,
                "name": "source_col2",
                "node_names": {"input_2"},
                "tabular_data_id": None,
                "tabular_data_type": "event_data",
                "type": "source",
                "dtype": "INT",
            },
        ],
        "filter": False,
        "name": "derived_col1",
        "node_names": {"add_2", "input_2"},  # note that graph_1 is not included here
        "transforms": ["add"],
        "type": "derived",
        "dtype": "FLOAT",
    }

    # case 2: when some node name found in the replace_node_name_map and some not found
    assert derived_col1.node_names == {"input_1", "add_1"}
    output = derived_col1.clone_without_internal_nodes(
        proxy_node_name_map={"input_1": {"input_2"}},
        graph_node_name="graph_1",
        graph_node_transform="graph",
    )
    assert output == {
        "columns": [
            {
                "filter": False,
                "name": "source_col1",
                "node_names": {"input_2"},
                "tabular_data_id": None,
                "tabular_data_type": "event_data",
                "type": "source",
                "dtype": "FLOAT",
            },
            {
                "filter": False,
                "name": "source_col2",
                "node_names": {"input_2"},
                "tabular_data_id": None,
                "tabular_data_type": "event_data",
                "type": "source",
                "dtype": "INT",
            },
        ],
        "filter": False,
        "name": "derived_col1",
        "node_names": {"input_2", "graph_1"},  # note that add_1 is removed
        "transforms": ["graph"],
        "type": "derived",
        "dtype": "FLOAT",
    }

    # case 3: when all nodes not found replace_node_name_map
    assert derived_col1.node_names == {"input_1", "add_1"}
    output = derived_col1.clone_without_internal_nodes(
        proxy_node_name_map={},
        graph_node_name="graph_1",
        graph_node_transform="graph",
    )
    assert output == {
        "columns": [
            {
                "filter": False,
                "name": "source_col1",
                "node_names": {"graph_1"},  # note that input_1 is replaced with graph_1
                "tabular_data_id": None,
                "tabular_data_type": "event_data",
                "type": "source",
                "dtype": "FLOAT",
            },
            {
                "filter": False,
                "name": "source_col2",
                "node_names": {"graph_1"},  # note that input_1 is replaced with graph_1
                "tabular_data_id": None,
                "tabular_data_type": "event_data",
                "type": "source",
                "dtype": "INT",
            },
        ],
        "filter": False,
        "name": "derived_col1",
        "node_names": {"graph_1"},
        "transforms": ["graph"],
        "type": "derived",
        "dtype": "FLOAT",
    }
