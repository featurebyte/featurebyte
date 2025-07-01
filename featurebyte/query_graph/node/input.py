"""
This module contains SQL operation related to input node
"""

# DO NOT include "from __future__ import annotations" as it will trigger issue for pydantic model nested definition
from abc import abstractmethod
from typing import Any, ClassVar, Dict, List, Optional, Sequence, Tuple, Union

from bson import ObjectId
from pydantic import BaseModel, Field, model_validator
from typing_extensions import Annotated, Literal

from featurebyte.enum import DBVarType, SourceType, TableDataType
from featurebyte.models.base import FeatureByteBaseModel, PydanticObjectId
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.model.time_series_table import TimeInterval
from featurebyte.query_graph.model.timestamp_schema import TimestampSchema
from featurebyte.query_graph.node.base import BaseNode
from featurebyte.query_graph.node.metadata.column import InColumnStr
from featurebyte.query_graph.node.metadata.config import SDKCodeGenConfig
from featurebyte.query_graph.node.metadata.operation import (
    NodeOutputCategory,
    OperationStructure,
    OperationStructureInfo,
    SourceDataColumn,
)
from featurebyte.query_graph.node.metadata.sdk_code import (
    ClassEnum,
    CodeGenerationContext,
    CommentStr,
    NodeCodeGenOutput,
    ObjectClass,
    StatementT,
    VariableNameGenerator,
    VarNameExpressionInfo,
    derive_sdk_code_from_timestamp_schema,
)
from featurebyte.query_graph.node.schema import (
    ColumnSpec,
    DatabaseDetails,
    InputNodeFeatureStoreDetails,
    TableDetails,
)
from featurebyte.query_graph.sql.source_info import SourceInfo


class BaseInputNodeParameters(FeatureByteBaseModel):
    """BaseInputNodeParameters"""

    columns: List[ColumnSpec]
    table_details: TableDetails
    feature_store_details: InputNodeFeatureStoreDetails

    # class variable
    _source_type_to_import: ClassVar[Dict[SourceType, ClassEnum]] = {
        SourceType.SNOWFLAKE: ClassEnum.SNOWFLAKE_DETAILS,
        SourceType.DATABRICKS: ClassEnum.DATABRICK_DETAILS,
        SourceType.DATABRICKS_UNITY: ClassEnum.DATABRICK_DETAILS,
        SourceType.SPARK: ClassEnum.SPARK_DETAILS,
        SourceType.SQLITE: ClassEnum.SQLITE_DETAILS,
        SourceType.TEST: ClassEnum.TESTDB_DETAILS,
    }

    @model_validator(mode="before")
    @classmethod
    def _convert_columns_format(cls, values: Any) -> Any:
        # DEV-556: convert list of string to list of dictionary
        if isinstance(values, BaseModel):
            values = values.model_dump(by_alias=True)

        columns = values.get("columns")
        if columns and isinstance(columns[0], str):
            values["columns"] = [{"name": col, "dtype": DBVarType.UNKNOWN} for col in columns]
        return values

    @model_validator(mode="after")
    def _validate_parameters(self) -> "BaseInputNodeParameters":
        if not self.columns:
            raise ValueError("columns should not be empty")
        return self

    def extract_feature_store_object(
        self,
        feature_store_name: str,
        database_details: DatabaseDetails,
    ) -> ObjectClass:
        """
        Construct feature store object for SDK code generation

        Parameters
        ----------
        feature_store_name: str
            Feature store name
        database_details: DatabaseDetails
            Database details

        Returns
        -------
        ObjectClass
        """
        source_type = self.feature_store_details.type
        source_details = self._source_type_to_import[source_type]
        return ClassEnum.FEATURE_STORE(
            name=feature_store_name,
            type=self.feature_store_details.type,
            details=source_details(**database_details.model_dump()),
        )

    def extract_tabular_source_object(self, feature_store_id: ObjectId) -> ObjectClass:
        """
        Construct tabular source object for SDK code generation

        Parameters
        ----------
        feature_store_id: ObjectId
            Feature store ID

        Returns
        -------
        ObjectClass
        """
        return ClassEnum.TABULAR_SOURCE(
            feature_store_id=ClassEnum.OBJECT_ID(feature_store_id),
            table_details=ClassEnum.TABLE_DETAILS(
                database_name=self.table_details.database_name,
                schema_name=self.table_details.schema_name,
                table_name=self.table_details.table_name,
            ),
        )

    def extract_columns_info_objects(self) -> List[ObjectClass]:
        """
        Construct list of column info objects for SDK code generation

        Returns
        -------
        List[ObjectClass]
        """
        return [ClassEnum.COLUMN_INFO(name=col.name, dtype=col.dtype) for col in self.columns]

    @property
    @abstractmethod
    def variable_name_prefix(self) -> str:
        """
        Pre-variable name used by the specific table

        Returns
        -------
        str
        """

    @abstractmethod
    def extract_other_constructor_parameters(self, table_info: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract other constructor parameters used in SDK code generation

        Parameters
        ----------
        table_info: Dict[str, Any]
            Table info that does not store in the query graph input node

        Returns
        -------
        Dict[str, Any]
        """

    @abstractmethod
    def construct_comment(
        self, table_id_to_info: Dict[PydanticObjectId, Dict[str, Any]]
    ) -> Optional[CommentStr]:
        """
        Construct comment for the input node

        Parameters
        ----------
        table_id_to_info: Dict[PydanticObjectId, Dict[str, Any]]
            Table ID to table info mapping

        Returns
        -------
        Optional[CommentStr]
        """

    def get_source_info(self) -> SourceInfo:
        """
        Get source info

        Returns
        -------
        SourceInfo
        """
        feature_store_details = self.feature_store_details
        if feature_store_details.details is not None:
            return feature_store_details.details.get_source_info()
        return SourceInfo(
            database_name="",
            schema_name="",
            source_type=self.feature_store_details.type,
        )


class SourceTableInputNodeParameters(BaseInputNodeParameters):
    """SourceTableInputNodeParameters"""

    type: Literal[TableDataType.SOURCE_TABLE] = TableDataType.SOURCE_TABLE
    id: Optional[PydanticObjectId] = Field(default=None)

    @property
    def variable_name_prefix(self) -> str:
        return "table"

    def extract_other_constructor_parameters(self, table_info: Dict[str, Any]) -> Dict[str, Any]:
        return {}

    def construct_comment(
        self, table_id_to_info: Dict[PydanticObjectId, Dict[str, Any]]
    ) -> Optional[CommentStr]:
        return None


class EventTableInputNodeParameters(BaseInputNodeParameters):
    """EventTableParameters"""

    type: Literal[TableDataType.EVENT_TABLE] = TableDataType.EVENT_TABLE
    id: Optional[PydanticObjectId] = Field(default=None)
    timestamp_column: Optional[InColumnStr] = Field(
        default=None
    )  # DEV-556: this should be compulsory
    id_column: Optional[InColumnStr] = Field(default=None)  # DEV-556: this should be compulsory
    event_timestamp_timezone_offset: Optional[str] = Field(default=None)
    event_timestamp_timezone_offset_column: Optional[InColumnStr] = Field(default=None)
    event_timestamp_schema: Optional[TimestampSchema] = Field(default=None)

    @model_validator(mode="before")
    @classmethod
    def _convert_node_parameters_format(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        # DEV-556: converted older record (parameters) into a newer format
        if "dbtable" in values:
            values["table_details"] = values["dbtable"]
        if "feature_store" in values:
            values["feature_store_details"] = values["feature_store"]
        if "timestamp" in values:
            values["timestamp_column"] = values["timestamp"]
        return values

    @property
    def variable_name_prefix(self) -> str:
        return "event_table"

    def extract_other_constructor_parameters(self, table_info: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "record_creation_timestamp_column": table_info.get("record_creation_timestamp_column"),
            "event_id_column": self.id_column,
            "event_timestamp_column": self.timestamp_column,
            "_id": ClassEnum.OBJECT_ID(self.id),
        }

    def construct_comment(
        self, table_id_to_info: Dict[PydanticObjectId, Dict[str, Any]]
    ) -> Optional[CommentStr]:
        output = None
        if self.id:
            table_name = table_id_to_info.get(self.id, {}).get("name")
            if table_name:
                output = CommentStr(f'event_table name: "{table_name}"')
        return output


class ItemTableInputNodeParameters(BaseInputNodeParameters):
    """ItemTableParameters"""

    type: Literal[TableDataType.ITEM_TABLE] = TableDataType.ITEM_TABLE
    id: Optional[PydanticObjectId] = Field(default=None)
    id_column: Optional[InColumnStr] = Field(default=None)  # DEV-556: this should be compulsory
    event_table_id: Optional[PydanticObjectId] = Field(default=None)
    event_id_column: Optional[InColumnStr] = Field(default=None)

    @property
    def variable_name_prefix(self) -> str:
        return "item_table"

    def extract_other_constructor_parameters(self, table_info: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "record_creation_timestamp_column": table_info.get("record_creation_timestamp_column"),
            "item_id_column": self.id_column,
            "event_id_column": self.event_id_column,
            "event_table_id": ClassEnum.OBJECT_ID(self.event_table_id),
            "_id": ClassEnum.OBJECT_ID(self.id),
        }

    def construct_comment(
        self, table_id_to_info: Dict[PydanticObjectId, Dict[str, Any]]
    ) -> Optional[CommentStr]:
        output = None
        if self.id and self.event_table_id:
            table_name = table_id_to_info.get(self.id, {}).get("name")
            event_table_name = table_id_to_info.get(self.event_table_id, {}).get("name")
            if table_name and event_table_name:
                output = CommentStr(
                    f'item_table name: "{table_name}", event_table name: "{event_table_name}"'
                )
        return output


class DimensionTableInputNodeParameters(BaseInputNodeParameters):
    """DimensionTableParameters"""

    type: Literal[TableDataType.DIMENSION_TABLE] = TableDataType.DIMENSION_TABLE
    id: Optional[PydanticObjectId] = Field(default=None)
    id_column: Optional[InColumnStr] = Field(default=None)  # DEV-556: this should be compulsory

    @property
    def variable_name_prefix(self) -> str:
        return "dimension_table"

    def extract_other_constructor_parameters(self, table_info: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "record_creation_timestamp_column": table_info.get("record_creation_timestamp_column"),
            "dimension_id_column": self.id_column,
            "_id": ClassEnum.OBJECT_ID(self.id),
        }

    def construct_comment(
        self, table_id_to_info: Dict[PydanticObjectId, Dict[str, Any]]
    ) -> Optional[CommentStr]:
        output = None
        if self.id:
            table_name = table_id_to_info.get(self.id, {}).get("name")
            if table_name:
                output = CommentStr(f'dimension_table name: "{table_name}"')
        return output


class SCDTableInputNodeParameters(BaseInputNodeParameters):
    """SCDTableParameters"""

    type: Literal[TableDataType.SCD_TABLE] = TableDataType.SCD_TABLE
    id: Optional[PydanticObjectId] = Field(default=None)
    natural_key_column: Optional[InColumnStr] = Field(
        default=None
    )  # DEV-556: this should be compulsory
    effective_timestamp_column: Optional[InColumnStr] = Field(
        default=None
    )  # DEV-556: this should be compulsory
    surrogate_key_column: Optional[InColumnStr] = Field(default=None)
    end_timestamp_column: Optional[InColumnStr] = Field(default=None)
    current_flag_column: Optional[InColumnStr] = Field(default=None)

    @property
    def variable_name_prefix(self) -> str:
        return "scd_table"

    def extract_other_constructor_parameters(self, table_info: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "record_creation_timestamp_column": table_info.get("record_creation_timestamp_column"),
            "natural_key_column": self.natural_key_column,
            "effective_timestamp_column": self.effective_timestamp_column,
            "end_timestamp_column": self.end_timestamp_column,
            "surrogate_key_column": self.surrogate_key_column,
            "current_flag_column": self.current_flag_column,
            "_id": ClassEnum.OBJECT_ID(self.id),
        }

    def construct_comment(
        self, table_id_to_info: Dict[PydanticObjectId, Dict[str, Any]]
    ) -> Optional[CommentStr]:
        output = None
        if self.id:
            table_name = table_id_to_info.get(self.id, {}).get("name")
            if table_name:
                output = CommentStr(f'scd_table name: "{table_name}"')
        return output


class TimeSeriesTableInputNodeParameters(BaseInputNodeParameters):
    """TimeSeriesTableParameters"""

    type: Literal[TableDataType.TIME_SERIES_TABLE] = TableDataType.TIME_SERIES_TABLE
    id: Optional[PydanticObjectId] = Field(default=None)
    id_column: Optional[InColumnStr] = Field(default=None)
    reference_datetime_column: InColumnStr
    reference_datetime_schema: TimestampSchema
    time_interval: TimeInterval

    @property
    def variable_name_prefix(self) -> str:
        return "time_series_table"

    def extract_other_constructor_parameters(self, table_info: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "record_creation_timestamp_column": table_info.get("record_creation_timestamp_column"),
            "series_id_column": self.id_column,
            "reference_datetime_column": self.reference_datetime_column,
            "reference_datetime_schema": derive_sdk_code_from_timestamp_schema(
                timestamp_schema=self.reference_datetime_schema
            ),
            "time_interval": ClassEnum.TIME_INTERVAL(
                value=self.time_interval.value,
                unit=self.time_interval.unit,
            ),
            "_id": ClassEnum.OBJECT_ID(self.id),
        }

    def construct_comment(
        self, table_id_to_info: Dict[PydanticObjectId, Dict[str, Any]]
    ) -> Optional[CommentStr]:
        output = None
        if self.id:
            table_name = table_id_to_info.get(self.id, {}).get("name")
            if table_name:
                output = CommentStr(f'time_series_table name: "{table_name}"')
        return output


InputNodeParameters = Annotated[
    Union[
        EventTableInputNodeParameters,
        ItemTableInputNodeParameters,
        SourceTableInputNodeParameters,
        DimensionTableInputNodeParameters,
        SCDTableInputNodeParameters,
        TimeSeriesTableInputNodeParameters,
    ],
    Field(discriminator="type"),
]


class InputNode(BaseNode):
    """InputNode class"""

    type: Literal[NodeType.INPUT] = NodeType.INPUT
    output_type: NodeOutputType = NodeOutputType.FRAME
    parameters: InputNodeParameters

    # class variable
    _table_type_to_table_class_enum: ClassVar[Dict[TableDataType, ClassEnum]] = {
        TableDataType.SOURCE_TABLE: ClassEnum.SOURCE_TABLE,
        TableDataType.EVENT_TABLE: ClassEnum.EVENT_TABLE,
        TableDataType.ITEM_TABLE: ClassEnum.ITEM_TABLE,
        TableDataType.DIMENSION_TABLE: ClassEnum.DIMENSION_TABLE,
        TableDataType.SCD_TABLE: ClassEnum.SCD_TABLE,
        TableDataType.TIME_SERIES_TABLE: ClassEnum.TIME_SERIES_TABLE,
    }

    @property
    def max_input_count(self) -> int:
        return 0

    def _get_required_input_columns(
        self, input_index: int, available_column_names: List[str]
    ) -> Sequence[str]:
        return self._extract_column_str_values(self.parameters.model_dump(), InColumnStr)

    def _derive_node_operation_info(
        self,
        inputs: List[OperationStructure],
        global_state: OperationStructureInfo,
    ) -> OperationStructure:
        _ = global_state
        return OperationStructure(
            columns=[
                SourceDataColumn(
                    name=column.name,
                    table_id=self.parameters.id,
                    table_type=TableDataType(self.parameters.type),
                    node_names={self.name},
                    node_name=self.name,
                    dtype_info=column.dtype_info,
                    filter=False,
                )
                for column in self.parameters.columns
            ],
            output_type=NodeOutputType.FRAME,
            output_category=NodeOutputCategory.VIEW,
            row_index_lineage=(self.name,),
        )

    def _derive_sdk_code(
        self,
        node_inputs: List[NodeCodeGenOutput],
        var_name_generator: VariableNameGenerator,
        operation_structure: OperationStructure,
        config: SDKCodeGenConfig,
        context: CodeGenerationContext,
    ) -> Tuple[List[StatementT], VarNameExpressionInfo]:
        statements: List[StatementT] = []
        table_type = self.parameters.type
        table_class_enum = self._table_type_to_table_class_enum[table_type]

        # construct table sdk statement
        table_var_name = var_name_generator.convert_to_variable_name(
            variable_name_prefix=self.parameters.variable_name_prefix, node_name=self.name
        )
        table_id = self.parameters.id
        table_info = config.table_id_to_info.get(table_id, {}) if table_id else {}
        table_name = table_info.get("name")
        if config.to_use_saved_data and self.parameters.id:
            # to generate `*Data.get_by_id(ObjectId("<table_id>"))` statement
            comment = self.parameters.construct_comment(table_id_to_info=config.table_id_to_info)
            if comment:
                statements.append(comment)
            object_id = ClassEnum.OBJECT_ID(self.parameters.id)
            right_op = table_class_enum(object_id, _method_name="get_by_id")
        else:
            # to generate `*Table(
            #     name="<table_name>",
            #     feature_store=FeatureStore(...),
            #     tabular_source=TabularSource(...),
            #     columns_info=[ColumnInfo(...), ...],
            #     ...
            # )` statement
            assert config.database_details is not None, "database_details should not be None"
            columns_info = table_info.get(
                "columns_info", self.parameters.extract_columns_info_objects()
            )
            right_op = table_class_enum(
                name=table_name or str(table_var_name),
                feature_store=self.parameters.extract_feature_store_object(
                    feature_store_name=config.feature_store_name,
                    database_details=config.database_details,
                ),
                tabular_source=self.parameters.extract_tabular_source_object(
                    feature_store_id=config.feature_store_id
                ),
                columns_info=columns_info,
                **self.parameters.extract_other_constructor_parameters(table_info),
            )

        statements.append((table_var_name, right_op))
        return statements, table_var_name
