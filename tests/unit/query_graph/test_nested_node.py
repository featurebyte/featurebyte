"""Test node in nested.py"""

from featurebyte.enum import DBVarType
from featurebyte.query_graph.model.dtype import DBVarTypeInfo
from featurebyte.query_graph.node.nested import OfflineStoreMetadata


def test_offline_store_metadata_backward_compatibility():
    """
    Test backward compatibility of OfflineStoreMetadata
    """
    metadata = OfflineStoreMetadata(
        aggregation_nodes_info=[],
        feature_job_setting=None,
        has_ttl=False,
        offline_store_table_name="test_table",
        output_dtype=DBVarType.VARCHAR,  # old way of specifying dtype
        primary_entity_dtypes=[],
    )

    # check that old way of specifying dtype works
    assert metadata.output_dtype == DBVarType.VARCHAR

    # check that dtype_info is set correctly
    assert metadata.output_dtype_info == DBVarTypeInfo(dtype=DBVarType.VARCHAR)
