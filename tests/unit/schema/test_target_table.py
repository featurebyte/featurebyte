"""
Test target table schema
"""
import pytest
from bson import ObjectId

from featurebyte.models.observation_table import UploadedFileInput
from featurebyte.models.request_input import RequestInputType
from featurebyte.query_graph.model.graph import QueryGraphModel
from featurebyte.schema.target_table import TargetTableCreate


@pytest.mark.parametrize(
    "graph, node_names, expected_error",
    [
        (None, None, None),
        (None, ["node_name"], ValueError),
        (QueryGraphModel(), None, ValueError),
        (QueryGraphModel(), ["node_name"], None),
    ],
)
def test_target_table_create(graph, node_names, expected_error):
    """
    Test target table create schema
    """
    target_id = ObjectId()
    common_params = {
        "name": "target_name",
        "feature_store_id": ObjectId(),
        "serving_names_mapping": {},
        "target_id": target_id,
        "context_id": None,
        "request_input": UploadedFileInput(
            type=RequestInputType.UPLOADED_FILE,
            file_name="random_file_name",
        ),
    }
    if expected_error:
        with pytest.raises(expected_error):
            TargetTableCreate(graph=graph, node_names=node_names, **common_params)
        return
    else:
        target_table_create = TargetTableCreate(graph=graph, node_names=node_names, **common_params)
        assert target_table_create.name == "target_name"
        assert target_table_create.target_id == target_id
