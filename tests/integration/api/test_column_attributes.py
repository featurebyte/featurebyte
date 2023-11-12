"""Integration test for column attributes"""
from featurebyte.query_graph.model.column_info import ColumnInfo


def test_column_attributes_added(event_table, source_type):
    """Test embedding and flat dict attributes"""
    columns_with_attribues = [col for col in event_table.columns_info if col.attributes]
    assert len(columns_with_attribues) == 2
    assert columns_with_attribues == [
        ColumnInfo(
            name="EMBEDDING_ARRAY",
            dtype="ARRAY",
            entity_id=None,
            critical_data_info=None,
            semantic_id=None,
            description=None,
            attributes=["embedding"],
        ),
        ColumnInfo(
            name="FLAT_DICT",
            dtype="OBJECT" if source_type == "snowflake" else "STRUCT",
            entity_id=None,
            critical_data_info=None,
            semantic_id=None,
            description=None,
            attributes=["flat_dict"],
        ),
    ]
