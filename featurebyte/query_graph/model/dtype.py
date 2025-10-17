"""
Models related to dtype
"""

from typing import Optional

from pydantic import Field

from featurebyte.enum import DBVarType
from featurebyte.models.base import FeatureByteBaseModel
from featurebyte.query_graph.model.timestamp_schema import (
    TimestampSchema,
    TimestampTupleSchema,
    TimeZoneColumn,
)


class ObjectDtypeMetadata(FeatureByteBaseModel):
    """
    Metadata for object dtype
    """

    key_dtype: DBVarType
    value_dtype: DBVarType


class DBVarTypeMetadata(FeatureByteBaseModel):
    """
    Metadata for DBVarType
    """

    timestamp_schema: Optional[TimestampSchema] = None
    timestamp_tuple_schema: Optional[TimestampTupleSchema] = None
    # this field is only used internally and should not be serialized
    # TODO: consider to remove exclude=True when we add support for tagging object dtype in the API
    object_dtype: Optional[ObjectDtypeMetadata] = Field(default=None, exclude=True)


class DBVarTypeInfo(FeatureByteBaseModel):
    """
    DBVarTypeInfo
    """

    dtype: DBVarType
    metadata: Optional[DBVarTypeMetadata] = None

    def __hash__(self) -> int:
        return hash((self.dtype, self.metadata))

    @property
    def timestamp_schema(self) -> Optional[TimestampSchema]:
        """
        Timestamp schema from the DBVarTypeInfo

        Returns
        -------
        Optional[TimestampSchema]
        """
        if self.metadata is None:
            return None
        return self.metadata.timestamp_schema

    @property
    def timestamp_format_string(self) -> Optional[str]:
        """
        Timestamp format string from the DBVarTypeInfo

        Returns
        -------
        Optional[str]
        """
        timestamp_schema = self.timestamp_schema
        if timestamp_schema is None:
            return None
        return timestamp_schema.format_string

    def remap_column_name(self, column_name_map: dict[str, str]) -> "DBVarTypeInfo":
        """
        Remap column name in the DBVarTypeInfo

        Parameters
        ----------
        column_name_map: dict[str, str]
            Column name map from the old name to the new name

        Returns
        -------
        DBVarTypeInfo
        """
        if self.metadata is None or self.metadata.timestamp_schema is None:
            return self

        timestamp_schema = self.metadata.timestamp_schema
        timezone = timestamp_schema.timezone if timestamp_schema.timezone else None
        if timezone and isinstance(timezone, TimeZoneColumn):
            offset_column_name = timezone.column_name
            if offset_column_name in column_name_map:
                return DBVarTypeInfo(
                    dtype=self.dtype,
                    metadata=DBVarTypeMetadata(
                        timestamp_schema=TimestampSchema(
                            is_utc_time=timestamp_schema.is_utc_time,
                            format_string=timestamp_schema.format_string,
                            timezone=TimeZoneColumn(
                                column_name=column_name_map[offset_column_name], type=timezone.type
                            ),
                        )
                    ),
                )

        return self

    def add_object_dtype_metadata(
        self, object_dtype_metadata: ObjectDtypeMetadata
    ) -> "DBVarTypeInfo":
        """
        Add object dtype metadata to the DBVarTypeInfo

        Parameters
        ----------
        object_dtype_metadata: ObjectDtypeMetadata
            Object dtype metadata to add

        Returns
        -------
        DBVarTypeInfo
        """
        return DBVarTypeInfo(
            dtype=self.dtype,
            metadata=DBVarTypeMetadata(
                timestamp_schema=self.metadata.timestamp_schema if self.metadata else None,
                timestamp_tuple_schema=self.metadata.timestamp_tuple_schema
                if self.metadata
                else None,
                object_dtype=object_dtype_metadata,
            ),
        )


class PartitionMetadata(FeatureByteBaseModel):
    """
    Metadata for PartitionMetadata
    """

    is_partition_key: bool = Field(default=False)
    is_partition_key_candidate: bool = Field(default=False)
