"""
Tests for the Table classes in the schema module.
"""

import pytest
from bson import ObjectId

from featurebyte.enum import DBVarType
from featurebyte.query_graph.model.column_info import ColumnSpecWithDescription
from featurebyte.query_graph.model.common_table import TabularSource
from featurebyte.query_graph.model.dtype import DBVarTypeMetadata, PartitionMetadata
from featurebyte.query_graph.node.schema import TableDetails
from featurebyte.schema.table import TableCreate


@pytest.mark.parametrize("has_datetime_partition", [True, False])
def test_table_creation_schema(has_datetime_partition):
    # Create an instance of TableCreate with minimal required fields
    table_create = TableCreate(
        name="test_table",
        tabular_source=TabularSource(
            feature_store_id=ObjectId(),
            table_details=TableDetails(
                database_name="test_db",
                schema_name="test_schema",
                table_name="test_table",
            ),
        ),
        columns_info=[
            ColumnSpecWithDescription(
                name="snapshot_dt",
                dtype=DBVarType.DATE,
                dtype_metadata=None,
                partition_metadata=PartitionMetadata(is_partition_key=True),
            ),
            ColumnSpecWithDescription(
                name="test_column",
                dtype=DBVarType.INT,
                dtype_metadata=None,
                partition_metadata=PartitionMetadata(is_partition_key=True),
            ),
        ],
        datetime_partition_column="snapshot_dt" if has_datetime_partition else None,
    )

    # Ensure partition metadata is cleared other than for datetime partition column
    assert table_create.name == "test_table"
    assert table_create.columns_info == [
        ColumnSpecWithDescription(
            name="snapshot_dt",
            dtype=DBVarType.DATE,
            dtype_metadata=DBVarTypeMetadata() if has_datetime_partition else None,
            partition_metadata=PartitionMetadata(is_partition_key=True)
            if has_datetime_partition
            else None,
        ),
        ColumnSpecWithDescription(
            name="test_column",
            dtype=DBVarType.INT,
            dtype_metadata=None,
        ),
    ]
