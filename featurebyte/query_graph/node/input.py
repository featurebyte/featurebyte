"""
This module contains SQL operation related to input node
"""
# DO NOT include "from __future__ import annotations" as it will trigger issue for pydantic model nested definition
from typing import Any, ClassVar, Dict, List, Literal, Optional, Sequence, Tuple, Union
from typing_extensions import Annotated

from abc import abstractmethod  # pylint: disable=wrong-import-order

from bson import ObjectId
from pydantic import BaseModel, Field, root_validator

from featurebyte.enum import DBVarType, SourceType, TableDataType
from featurebyte.models.base import PydanticObjectId
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.node.base import BaseNode
from featurebyte.query_graph.node.metadata.column import InColumnStr
from featurebyte.query_graph.node.metadata.operation import (
    NodeOutputCategory,
    OperationStructure,
    OperationStructureBranchState,
    OperationStructureInfo,
    SourceDataColumn,
)
from featurebyte.query_graph.node.metadata.sdk_code import (
    ClassEnum,
    CodeGenerationConfig,
    CommentStr,
    ObjectClass,
    StatementT,
    VariableNameGenerator,
    VarNameExpressionStr,
)
from featurebyte.query_graph.node.schema import ColumnSpec, FeatureStoreDetails, TableDetails


class BaseInputNodeParameters(BaseModel):
    """BaseInputNodeParameters"""

    columns: List[ColumnSpec]
    table_details: TableDetails
    feature_store_details: FeatureStoreDetails

    # class variable
    _source_type_to_import: ClassVar[Dict[SourceType, ClassEnum]] = {
        SourceType.SNOWFLAKE: ClassEnum.SNOWFLAKE_DETAILS,
        SourceType.DATABRICKS: ClassEnum.DATABRICK_DETAILS,
        SourceType.SPARK: ClassEnum.SPARK_DETAILS,
        SourceType.SQLITE: ClassEnum.SQLITE_DETAILS,
        SourceType.TEST: ClassEnum.TESTDB_DETAILS,
    }

    @root_validator(pre=True)
    @classmethod
    def _convert_columns_format(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        # DEV-556: convert list of string to list of dictionary
        columns = values.get("columns")
        if columns and isinstance(columns[0], str):
            values["columns"] = [{"name": col, "dtype": DBVarType.UNKNOWN} for col in columns]
        return values

    def extract_feature_store_object(self, feature_store_name: str) -> ObjectClass:
        """
        Construct feature store object for SDK code generation

        Parameters
        ----------
        feature_store_name: str
            Feature store name

        Returns
        -------
        ObjectClass
        """
        source_type = self.feature_store_details.type
        source_details = self._source_type_to_import[source_type]
        return ClassEnum.FEATURE_STORE(
            name=feature_store_name,
            type=self.feature_store_details.type,
            details=source_details(**self.feature_store_details.details.dict()),
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
        Pre-variable name used by the specific table data

        Returns
        -------
        str
        """

    @abstractmethod
    def extract_other_constructor_parameters(self, data_info: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract other constructor parameters used in SDK code generation

        Parameters
        ----------
        data_info: Dict[str, Any]
            Data info that does not store in the query graph input node

        Returns
        -------
        Dict[str, Any]
        """

    @abstractmethod
    def construct_comment(
        self, data_id_to_info: Dict[PydanticObjectId, Dict[str, Any]]
    ) -> Optional[CommentStr]:
        """
        Construct comment for the input node

        Parameters
        ----------
        data_id_to_info: Dict[PydanticObjectId, Dict[str, Any]]
            Data ID to data info mapping

        Returns
        -------
        Optional[CommentStr]
        """


class GenericInputNodeParameters(BaseInputNodeParameters):
    """GenericParameters"""

    type: Literal[TableDataType.GENERIC] = Field(TableDataType.GENERIC)
    id: Optional[PydanticObjectId] = Field(default=None)

    @property
    def variable_name_prefix(self) -> str:
        return "data"

    def extract_other_constructor_parameters(self, data_info: Dict[str, Any]) -> Dict[str, Any]:
        return {}

    def construct_comment(
        self, data_id_to_info: Dict[PydanticObjectId, Dict[str, Any]]
    ) -> Optional[CommentStr]:
        return None


class EventDataInputNodeParameters(BaseInputNodeParameters):
    """EventDataParameters"""

    type: Literal[TableDataType.EVENT_DATA] = Field(TableDataType.EVENT_DATA, const=True)
    id: Optional[PydanticObjectId] = Field(default=None)
    timestamp_column: Optional[InColumnStr] = Field(
        default=None
    )  # DEV-556: this should be compulsory
    id_column: Optional[InColumnStr] = Field(default=None)  # DEV-556: this should be compulsory

    @root_validator(pre=True)
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
        return "event_data"

    def extract_other_constructor_parameters(self, data_info: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "record_creation_date_column": data_info.get("record_creation_date_column"),
            "event_id_column": self.id_column,
            "event_timestamp_column": self.timestamp_column,
            "_id": ClassEnum.OBJECT_ID(self.id),
        }

    def construct_comment(
        self, data_id_to_info: Dict[PydanticObjectId, Dict[str, Any]]
    ) -> Optional[CommentStr]:
        output = None
        if self.id:
            data_name = data_id_to_info.get(self.id, {}).get("name")
            if data_name:
                output = CommentStr(f'event_data name: "{data_name}"')
        return output


class ItemDataInputNodeParameters(BaseInputNodeParameters):
    """ItemDataParameters"""

    type: Literal[TableDataType.ITEM_DATA] = Field(TableDataType.ITEM_DATA, const=True)
    id: Optional[PydanticObjectId] = Field(default=None)
    id_column: Optional[InColumnStr] = Field(default=None)  # DEV-556: this should be compulsory
    event_data_id: Optional[PydanticObjectId] = Field(default=None)
    event_id_column: Optional[InColumnStr] = Field(default=None)

    @property
    def variable_name_prefix(self) -> str:
        return "item_data"

    def extract_other_constructor_parameters(self, data_info: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "record_creation_date_column": data_info.get("record_creation_date_column"),
            "item_id_column": self.id_column,
            "event_id_column": self.event_id_column,
            "event_data_id": ClassEnum.OBJECT_ID(self.event_data_id),
            "_id": ClassEnum.OBJECT_ID(self.id),
        }

    def construct_comment(
        self, data_id_to_info: Dict[PydanticObjectId, Dict[str, Any]]
    ) -> Optional[CommentStr]:
        output = None
        if self.id and self.event_data_id:
            data_name = data_id_to_info.get(self.id, {}).get("name")
            event_data_name = data_id_to_info.get(self.event_data_id, {}).get("name")
            if data_name and event_data_name:
                output = CommentStr(
                    f'item_data name: "{data_name}", event_data name: "{event_data_name}"'
                )
        return output


class DimensionDataInputNodeParameters(BaseInputNodeParameters):
    """DimensionDataParameters"""

    type: Literal[TableDataType.DIMENSION_DATA] = Field(TableDataType.DIMENSION_DATA, const=True)
    id: Optional[PydanticObjectId] = Field(default=None)
    id_column: Optional[InColumnStr] = Field(default=None)  # DEV-556: this should be compulsory

    @property
    def variable_name_prefix(self) -> str:
        return "dimension_data"

    def extract_other_constructor_parameters(self, data_info: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "record_creation_date_column": data_info.get("record_creation_date_column"),
            "dimension_id_column": self.id_column,
            "_id": ClassEnum.OBJECT_ID(self.id),
        }

    def construct_comment(
        self, data_id_to_info: Dict[PydanticObjectId, Dict[str, Any]]
    ) -> Optional[CommentStr]:
        output = None
        if self.id:
            data_name = data_id_to_info.get(self.id, {}).get("name")
            if data_name:
                output = CommentStr(f'dimension_data name: "{data_name}"')
        return output


class SCDDataInputNodeParameters(BaseInputNodeParameters):
    """SCDDataParameters"""

    type: Literal[TableDataType.SCD_DATA] = Field(TableDataType.SCD_DATA, const=True)
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
        return "slowly_changing_data"

    def extract_other_constructor_parameters(self, data_info: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "record_creation_date_column": data_info.get("record_creation_date_column"),
            "natural_key_column": self.natural_key_column,
            "effective_timestamp_column": self.effective_timestamp_column,
            "end_timestamp_column": self.end_timestamp_column,
            "surrogate_key_column": self.surrogate_key_column,
            "current_flag_column": self.current_flag_column,
            "_id": ClassEnum.OBJECT_ID(self.id),
        }

    def construct_comment(
        self, data_id_to_info: Dict[PydanticObjectId, Dict[str, Any]]
    ) -> Optional[CommentStr]:
        output = None
        if self.id:
            data_name = data_id_to_info.get(self.id, {}).get("name")
            if data_name:
                output = CommentStr(f'scd_data name: "{data_name}"')
        return output


class InputNode(BaseNode):
    """InputNode class"""

    type: Literal[NodeType.INPUT] = Field(NodeType.INPUT, const=True)
    output_type: NodeOutputType = Field(NodeOutputType.FRAME, const=True)
    parameters: Annotated[
        Union[
            EventDataInputNodeParameters,
            ItemDataInputNodeParameters,
            GenericInputNodeParameters,
            DimensionDataInputNodeParameters,
            SCDDataInputNodeParameters,
        ],
        Field(discriminator="type"),
    ]

    # class variable
    _data_to_data_class_enum: ClassVar[Dict[TableDataType, ClassEnum]] = {
        TableDataType.GENERIC: ClassEnum.DATABASE_TABLE,
        TableDataType.EVENT_DATA: ClassEnum.EVENT_DATA,
        TableDataType.ITEM_DATA: ClassEnum.ITEM_DATA,
        TableDataType.DIMENSION_DATA: ClassEnum.DIMENSION_DATA,
        TableDataType.SCD_DATA: ClassEnum.SCD_DATA,
    }

    @root_validator(pre=True)
    @classmethod
    def _set_default_table_data_type(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        # DEV-556: set default table data type when it is not present
        if values.get("type") == NodeType.INPUT:
            # only attempt to fix this if it is an INPUT node
            # otherwise, it may cause issue when deserializing the graph in fastapi response
            if "parameters" in values and "type" not in values["parameters"]:
                values["parameters"]["type"] = TableDataType.EVENT_DATA
        return values

    @property
    def max_input_count(self) -> int:
        return 0

    def _get_required_input_columns(
        self, input_index: int, available_column_names: List[str]
    ) -> Sequence[str]:
        return self._extract_column_str_values(self.parameters.dict(), InColumnStr)

    def _derive_node_operation_info(
        self,
        inputs: List[OperationStructure],
        branch_state: OperationStructureBranchState,
        global_state: OperationStructureInfo,
    ) -> OperationStructure:
        _ = branch_state, global_state
        return OperationStructure(
            columns=[
                SourceDataColumn(
                    name=column.name,
                    tabular_data_id=self.parameters.id,
                    tabular_data_type=self.parameters.type,
                    node_names={self.name},
                    node_name=self.name,
                    dtype=column.dtype,
                )
                for column in self.parameters.columns
            ],
            output_type=NodeOutputType.FRAME,
            output_category=NodeOutputCategory.VIEW,
            row_index_lineage=(self.name,),
        )

    def _derive_sdk_code(
        self,
        input_var_name_expressions: List[VarNameExpressionStr],
        input_node_types: List[NodeType],
        var_name_generator: VariableNameGenerator,
        operation_structure: OperationStructure,
        config: CodeGenerationConfig,
    ) -> Tuple[List[StatementT], VarNameExpressionStr]:
        statements: List[StatementT] = []
        table_type = self.parameters.type
        data_class_enum = self._data_to_data_class_enum[table_type]

        # construct data sdk statement
        data_var_name = var_name_generator.convert_to_variable_name(
            variable_name_prefix=self.parameters.variable_name_prefix
        )
        data_id = self.parameters.id
        data_info = config.data_id_to_info.get(data_id, {}) if data_id else {}
        data_name = data_info.get("name")
        if config.to_use_saved_data and self.parameters.id:
            # to generate `*Data.get_by_id(ObjectId("<data_id>"))` statement
            comment = self.parameters.construct_comment(data_id_to_info=config.data_id_to_info)
            if comment:
                statements.append(comment)
            object_id = ClassEnum.OBJECT_ID(self.parameters.id)
            right_op = data_class_enum(object_id, _method_name="get_by_id")
        else:
            # to generate `*Data(
            #     name="<data_name>",
            #     feature_store=FeatureStore(...),
            #     tabular_source=TabularSource(...),
            #     columns_info=[ColumnInfo(...), ...],
            #     ...
            # )` statement
            columns_info = data_info.get(
                "columns_info", self.parameters.extract_columns_info_objects()
            )
            right_op = data_class_enum(
                name=data_name or str(data_var_name),
                feature_store=self.parameters.extract_feature_store_object(
                    feature_store_name=config.feature_store_name
                ),
                tabular_source=self.parameters.extract_tabular_source_object(
                    feature_store_id=config.feature_store_id
                ),
                columns_info=columns_info,
                **self.parameters.extract_other_constructor_parameters(data_info),
            )

        statements.append((data_var_name, right_op))
        return statements, data_var_name
