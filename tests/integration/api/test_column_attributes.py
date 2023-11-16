"""Integration test for column attributes"""
from featurebyte.enum import DBVarType
from featurebyte.query_graph.model.column_info import ColumnInfo


def test_detect_and_update_column_dtypes(event_table, source_type):
    """Test embedding and flat dict dtypes"""
    specialized_dtype_columns = [
        col for col in event_table.columns_info if col.dtype in {DBVarType.ARRAY, DBVarType.OBJECT}
    ]
    assert len(specialized_dtype_columns) == 2
    assert specialized_dtype_columns == [
        ColumnInfo(
            name="EMBEDDING_ARRAY",
            dtype="EMBEDDING",
            entity_id=None,
            critical_data_info=None,
            semantic_id=None,
            description=None,
        ),
        ColumnInfo(
            name="FLAT_DICT",
            dtype="FLAT_DICT",
            entity_id=None,
            critical_data_info=None,
            semantic_id=None,
            description=None,
        ),
    ]
