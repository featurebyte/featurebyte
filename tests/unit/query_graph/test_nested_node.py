"""Test node in nested.py"""

import pytest

from featurebyte.enum import DBVarType
from featurebyte.query_graph.model.dtype import DBVarTypeInfo
from featurebyte.query_graph.node.nested import OfflineStoreMetadata


@pytest.mark.parametrize(
    "dtype_data",
    [
        # old graph
        {"output_dtype": DBVarType.VARCHAR},
        # new graph that breaks old client
        {"output_dtype_info": DBVarTypeInfo(dtype=DBVarType.VARCHAR)},
        {"output_dtype_info": {"dtype": DBVarType.VARCHAR, "metadata": None}},
        {
            # new graph that does not break old client
            "output_dtype_info": DBVarTypeInfo(dtype=DBVarType.VARCHAR, metadata=None),
            "output_dtype": DBVarType.VARCHAR,
        },
    ],
)
def test_offline_store_metadata_backward_compatibility(dtype_data):
    """
    Test backward compatibility of OfflineStoreMetadata
    """
    metadata_dict = {
        "aggregation_nodes_info": [],
        "feature_job_setting": None,
        "has_ttl": False,
        "offline_store_table_name": "test_table",
        "primary_entity_dtypes": [],
    }
    metadata_dict.update(dtype_data)
    metadata = OfflineStoreMetadata(**metadata_dict)

    # check that old way of specifying dtype works
    assert metadata.output_dtype == DBVarType.VARCHAR

    # check that dtype_info is set correctly
    assert metadata.output_dtype_info == DBVarTypeInfo(dtype=DBVarType.VARCHAR)
