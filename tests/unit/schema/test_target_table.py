"""
Test target table schema
"""

import pytest
from bson import ObjectId

from featurebyte.query_graph.model.graph import QueryGraphModel
from featurebyte.schema.target_table import TargetTableCreate


@pytest.mark.parametrize(
    "target_id, graph, node_names, expected_error",
    [
        (ObjectId(), None, None, None),
        (ObjectId(), None, ["node1"], ValueError),
        (ObjectId(), QueryGraphModel(), None, ValueError),
        (ObjectId(), QueryGraphModel(), ["node1"], ValueError),
        (None, None, None, ValueError),
        (None, None, ["node1"], ValueError),
        (None, QueryGraphModel(), None, ValueError),
        (None, QueryGraphModel(), ["node1"], None),
    ],
)
def test_target_table_create(target_id, graph, node_names, expected_error):
    """
    Test target table create schema
    """
    common_params = {
        "name": "target_name",
        "feature_store_id": ObjectId(),
        "serving_names_mapping": {},
        "target_id": target_id,
        "context_id": None,
    }
    if expected_error:
        with pytest.raises(expected_error):
            TargetTableCreate(graph=graph, node_names=node_names, **common_params)
    else:
        target_table_create = TargetTableCreate(graph=graph, node_names=node_names, **common_params)
        assert target_table_create.name == "target_name"
        assert target_table_create.target_id == target_id
