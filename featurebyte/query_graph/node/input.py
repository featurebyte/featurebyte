"""
This module contains SQL operation related to input node
"""
# DO NOT include "from __future__ import annotations" as it will trigger issue for pydantic model nested definition
from typing import Any, ClassVar, Dict, List, Literal, Optional, Tuple, Union
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
    ObjectClass,
    StatementT,
    ValueStr,
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

    @property
    @abstractmethod
    def pre_variable_prefix(self) -> str:
        """
        Pre-variable name prefix used by the specific table data

        Returns
        -------
        str
        """

    @property
    @abstractmethod
    def from_data_method(self) -> str:
        """
        Method name used to construct view from data

        Returns
        -------
        str
        """

    def get_feature_store_object(self, feature_store_name: str) -> ObjectClass:
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

    def get_tabular_source_object(self, feature_store_id: ObjectId) -> ObjectClass:
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
            feature_store_id=feature_store_id,
            table_details=ClassEnum.TABLE_DETAILS(
                database_name=self.table_details.database_name,
                schema_name=self.table_details.schema_name,
                table_name=self.table_details.table_name,
            ),
        )

    def get_columns_info_object(self) -> List[ObjectClass]:
        """
        Construct list of column info objects for SDK code generation

        Returns
        -------
        List[ObjectClass]
        """
        return [ClassEnum.COLUMN_INFO(name=col.name, dtype=col.dtype) for col in self.columns]


class GenericInputNodeParameters(BaseInputNodeParameters):
    """GenericParameters"""

    type: Literal[TableDataType.GENERIC] = Field(TableDataType.GENERIC)
    id: Optional[PydanticObjectId] = Field(default=None)

    @property
    def pre_variable_prefix(self) -> str:
        return ""

    @property
    def from_data_method(self) -> str:
        return ""


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
    def pre_variable_prefix(self):
        return "event_"

    @property
    def from_data_method(self) -> str:
        return "from_event_data"


class ItemDataInputNodeParameters(BaseInputNodeParameters):
    """ItemDataParameters"""

    type: Literal[TableDataType.ITEM_DATA] = Field(TableDataType.ITEM_DATA, const=True)
    id: Optional[PydanticObjectId] = Field(default=None)
    id_column: Optional[InColumnStr] = Field(default=None)  # DEV-556: this should be compulsory
    event_data_id: Optional[PydanticObjectId] = Field(default=None)
    event_id_column: Optional[InColumnStr] = Field(default=None)

    @property
    def pre_variable_prefix(self):
        return "item_"

    @property
    def from_data_method(self) -> str:
        return "from_item_data"


class DimensionDataInputNodeParameters(BaseInputNodeParameters):
    """DimensionDataParameters"""

    type: Literal[TableDataType.DIMENSION_DATA] = Field(TableDataType.DIMENSION_DATA, const=True)
    id: Optional[PydanticObjectId] = Field(default=None)
    id_column: Optional[InColumnStr] = Field(default=None)  # DEV-556: this should be compulsory

    @property
    def pre_variable_prefix(self):
        return "dimension_"

    @property
    def from_data_method(self) -> str:
        return "from_dimension_data"


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
    def pre_variable_prefix(self):
        return "scd_"

    @property
    def from_data_method(self) -> str:
        return "from_slowly_changing_data"


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
    _data_to_view_class_enum: ClassVar[Dict[TableDataType, Optional[ClassEnum]]] = {
        TableDataType.GENERIC: None,
        TableDataType.EVENT_DATA: ClassEnum.EVENT_VIEW,
        TableDataType.ITEM_DATA: ClassEnum.ITEM_VIEW,
        TableDataType.DIMENSION_DATA: ClassEnum.DIMENSION_VIEW,
        TableDataType.SCD_DATA: ClassEnum.SCD_VIEW,
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

    def _derive_sdk_codes(
        self,
        input_var_name_expressions: List[VarNameExpressionStr],
        input_node_types: List[NodeType],
        var_name_generator: VariableNameGenerator,
        operation_structure: OperationStructure,
        config: CodeGenerationConfig,
    ) -> Tuple[List[StatementT], VarNameExpressionStr]:
        statements = []
        table_type = self.parameters.type
        data_class_enum = self._data_to_data_class_enum[table_type]

        # construct data sdk statement
        if config.to_use_saved_data and self.parameters.id:
            object_id = ClassEnum.OBJECT_ID(ValueStr.create(self.parameters.id))
            right_op = data_class_enum(object_id, method="get_by_id")
        else:
            object_id = ClassEnum.OBJECT_ID(ValueStr.create(config.feature_store_id))
            right_op = data_class_enum(
                feature_store=self.parameters.get_feature_store_object(
                    feature_store_name=config.feature_store_name
                ),
                tabular_source=self.parameters.get_tabular_source_object(
                    feature_store_id=object_id
                ),
                columns_info=self.parameters.get_columns_info_object(),
            )

        data_pre_var_name = f"{self.parameters.pre_variable_prefix}data"
        data_var_name = var_name_generator.convert_to_variable_name(
            pre_variable_name=data_pre_var_name
        )
        statements.append((data_var_name, right_op))

        if table_type != TableDataType.GENERIC:
            # construct view sdk statement
            view_class_enum = self._data_to_view_class_enum[table_type]
            right_op = view_class_enum(data_var_name, _method_name=self.parameters.from_data_method)

            view_pre_var_name = f"{self.parameters.pre_variable_prefix}view"
            view_var_name = var_name_generator.convert_to_variable_name(
                pre_variable_name=view_pre_var_name
            )
            statements.append((view_var_name, right_op))
            return statements, view_var_name
        return statements, data_var_name
