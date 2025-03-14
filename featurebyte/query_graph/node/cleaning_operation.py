"""
This module contains cleaning operation related classes.
"""

# DO NOT include "from __future__ import annotations" as it will trigger issue for pydantic model nested definition
from abc import abstractmethod
from typing import TYPE_CHECKING, Any, ClassVar, List, Optional, Sequence, Set, Union

import pandas as pd
from pydantic import Field, field_validator
from typing_extensions import Annotated, Literal

from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.common.model_util import construct_serialize_function
from featurebyte.enum import DBVarType, StrEnum
from featurebyte.models.base import FeatureByteBaseModel, PydanticObjectId
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.model.timestamp_schema import TimestampSchema
from featurebyte.query_graph.node.metadata.sdk_code import (
    ClassEnum,
    ObjectClass,
    derive_sdk_code_from_timestamp_schema,
)
from featurebyte.query_graph.node.validator import construct_unique_name_validator
from featurebyte.typing import OptionalScalar, Scalar

if TYPE_CHECKING:
    from featurebyte.query_graph.graph_node.base import GraphNode
    from featurebyte.query_graph.node import Node


class CleaningOperationType(StrEnum):
    """Field values used in critical data info operation"""

    # conditional cleaning operations
    MISSING = "missing"
    DISGUISED = "disguised"
    NOT_IN = "not_in"
    LESS_THAN = "less_than"
    LESS_THAN_OR_EQUAL = "less_than_or_equal"
    GREATER_THAN = "greater_than"
    GREATER_THAN_OR_EQUAL = "greater_than_or_equal"
    IS_STRING = "is_string"

    # add metadata to the schema
    ADD_TIMESTAMP_SCHEMA = "add_timestamp_schema"

    @classmethod
    def imputation_cleaning_operations(cls) -> Set["CleaningOperationType"]:
        """
        Imputation cleaning operations

        Returns
        -------
        Set[CleaningOperationType]
        """
        return {
            cls.MISSING,
            cls.DISGUISED,
            cls.NOT_IN,
            cls.LESS_THAN,
            cls.LESS_THAN_OR_EQUAL,
            cls.GREATER_THAN,
            cls.GREATER_THAN_OR_EQUAL,
            cls.IS_STRING,
        }


class BaseCleaningOperation(FeatureByteBaseModel):
    """BaseCleaningOperation class"""

    type: CleaningOperationType

    # support all data types by default (if None)
    supported_dtypes: ClassVar[Optional[Set[DBVarType]]] = None

    def __str__(self) -> str:
        class_name = self.__class__.__name__
        params = super().__str__()
        return f"{class_name}({params})"

    @abstractmethod
    def add_cleaning_operation(
        self, graph_node: "GraphNode", input_node: "Node", dtype: DBVarType
    ) -> "Node":
        """
        Add cleaning operation to the graph node

        Parameters
        ----------
        graph_node: GraphNode
            Nested graph node
        input_node: Node
            Input node to the query graph
        dtype: DBVarType
            Data type that output column will be casted to

        Returns
        -------
        Node
        """

    @abstractmethod
    def derive_sdk_code(self) -> ObjectClass:
        """
        Derive SDK code for the current cleaning operation

        Returns
        -------
        ObjectClass
        """


class AddTimestampSchema(BaseCleaningOperation):
    """
    The AddTimestampSchema class adds missing timestamp schema information to a table column.
    This includes details such as the format string, whether the timestamp is in UTC, and the timezone.

    **Important:** Do not use this column cleaning operation if the table registration already contains a
    timestamp schema input (for example, the `reference_datetime_schema` provided in the table registration via
    the create_time_series_table method). In such cases, the timestamp schema has been specified and this
    operation is unnecessary.

    Note that when used, this operation should be the last step in the cleaning operation list. When this operation
    is applied, the semantic type of the column will be reset to None.

    Parameters
    ----------
    timestamp_schema: TimestampSchema
        Timestamp schema to be added to the column.

    Examples
    --------
    Create a cleaning operation to add timestamp schema to the column with format string "YYYY-MM-DD HH24:MI:SS"
    where the timestamp is in UTC time and the timezone is "Asia/Singapore". Note that the format is specific to
    the underlying database and is used to parse the timestamp values.
    >>> fb.AddTimestampSchema(
    ...     timestamp_schema=fb.TimestampSchema(
    ...         format_string="YYYY-MM-DD HH24:MI:SS", is_utc_time=True, timezone="Asia/Singapore"
    ...     )
    ... )  # doctest: +SKIP

    See Also
    --------
    - [TimestampSchema](/reference/featurebyte.query_graph.model.timestamp_schema.TimestampSchema/):
        Schema for a timestamp column that can include timezone information.
    """

    # class variables
    __fbautodoc__: ClassVar[FBAutoDoc] = FBAutoDoc(proxy_class="featurebyte.AddTimestampSchema")

    # instance variables
    type: Literal[CleaningOperationType.ADD_TIMESTAMP_SCHEMA] = Field(
        CleaningOperationType.ADD_TIMESTAMP_SCHEMA, frozen=True, repr=False
    )
    timestamp_schema: TimestampSchema = Field(
        description="Timestamp schema to be added to the column."
    )

    def add_cleaning_operation(
        self, graph_node: "GraphNode", input_node: "Node", dtype: DBVarType
    ) -> "Node":
        ts_schema_node = graph_node.add_operation(
            node_type=NodeType.ADD_TIMESTAMP_SCHEMA,
            node_params={"timestamp_schema": self.timestamp_schema},
            node_output_type=NodeOutputType.SERIES,
            input_nodes=[input_node],
        )
        return ts_schema_node

    def derive_sdk_code(self) -> ObjectClass:
        return ClassEnum.ADD_TIMESTAMP_SCHEMA(
            timestamp_schema=derive_sdk_code_from_timestamp_schema(
                timestamp_schema=self.timestamp_schema
            )
        )


class BaseImputationCleaningOperation(BaseCleaningOperation):
    """BaseImputationCleaningOperation class"""

    imputed_value: OptionalScalar = Field(
        description="Value to replace existing value", default=None
    )

    def add_cleaning_operation(
        self, graph_node: "GraphNode", input_node: "Node", dtype: DBVarType
    ) -> "Node":
        """
        Add cleaning operation to the graph node

        Parameters
        ----------
        graph_node: GraphNode
            Nested graph node
        input_node: Node
            Input node to the query graph
        dtype: DBVarType
            Data type that output column will be casted to

        Returns
        -------
        Node
        """
        condition_node = self.add_condition_operation(graph_node=graph_node, input_node=input_node)
        cond_assign_node = graph_node.add_operation(
            node_type=NodeType.CONDITIONAL,
            node_params={"value": self.imputed_value},
            node_output_type=NodeOutputType.SERIES,
            input_nodes=[input_node, condition_node],
        )
        # explicitly cast the output back to original type if `dtype.to_type_str()` is not None
        type_str = DBVarType(dtype).to_type_str()
        if type_str:
            graph_node.add_operation(
                node_type=NodeType.CAST,
                node_params={"type": type_str, "from_dtype": dtype},
                node_output_type=NodeOutputType.SERIES,
                input_nodes=[cond_assign_node],
            )
        return graph_node.output_node

    @staticmethod
    def _cast_scalar_parameter_to_dtype(value: Any, dtype: DBVarType) -> Any:
        if value is not None:
            if dtype in {DBVarType.CHAR, DBVarType.VARCHAR}:
                return str(value)
            if dtype == DBVarType.INT:
                return int(value)
            if dtype == DBVarType.FLOAT:
                return float(value)
            if dtype == DBVarType.BOOL:
                return bool(value)
            if dtype == DBVarType.TIMESTAMP:
                val = pd.Timestamp(str(value))
                if val.tz:
                    # convert to UTC if timestamp has timezone
                    return val.tz_convert("UTC").strftime("%Y-%m-%dT%H:%M:%S")
                return val.strftime("%Y-%m-%dT%H:%M:%S")
            if dtype == DBVarType.TIMESTAMP_TZ:
                return pd.Timestamp(str(value)).strftime("%Y-%m-%dT%H:%M:%S%z")
            if dtype == DBVarType.DATE:
                return pd.Timestamp(str(value)).strftime("%Y-%m-%d")
            if dtype == DBVarType.TIME:
                return pd.Timestamp(str(value)).strftime("%H:%M:%S")
        return value

    @classmethod
    def _cast_list_parameter_to_dtype(
        cls, values: Sequence[Any], dtype: DBVarType
    ) -> Sequence[Any]:
        output = []
        found_values = set()
        for item in values:
            cast_item = cls._cast_scalar_parameter_to_dtype(item, dtype)
            # deduplicate on casted value
            if str(cast_item) not in found_values:
                output.append(cast_item)
                found_values.add(str(cast_item))
        return output

    def cast(self, dtype: DBVarType) -> None:
        """
        Cast the operation parameter value to the given dtype

        Parameters
        ----------
        dtype: DBVarType
            Data type that imputed value will be casted to
        """
        self.imputed_value = self._cast_scalar_parameter_to_dtype(self.imputed_value, dtype)

    @abstractmethod
    def check_condition(self, value: OptionalScalar) -> bool:
        """
        Check whether the value fulfill this condition

        Parameters
        ----------
        value: OptionalScalar
            Value to be checked

        Returns
        -------
        bool
        """

    @abstractmethod
    def add_condition_operation(self, graph_node: "GraphNode", input_node: "Node") -> "Node":
        """
        Construct a node that generate conditional filtering column

        Parameters
        ----------
        graph_node: GraphNode
            Graph node
        input_node: Node
            Input (series) node

        Returns
        -------
        Node
        """


class MissingValueImputation(BaseImputationCleaningOperation):
    """
    MissingValueImputation class is used to declare the operation to impute the missing value of a table column.

    Examples
    --------
    Create an imputation rule to replace missing value with 0

    >>> fb.MissingValueImputation(imputed_value=0)  # doctest: +SKIP
    """

    # class variables
    __fbautodoc__: ClassVar[FBAutoDoc] = FBAutoDoc(proxy_class="featurebyte.MissingValueImputation")

    # instance variables
    type: Literal[CleaningOperationType.MISSING] = Field(
        CleaningOperationType.MISSING, frozen=True, repr=False
    )
    imputed_value: Scalar

    def derive_sdk_code(self) -> ObjectClass:
        return ClassEnum.MISSING_VALUE_IMPUTATION(imputed_value=self.imputed_value)

    def check_condition(self, value: OptionalScalar) -> bool:
        return bool(pd.isnull(value))

    def add_condition_operation(self, graph_node: "GraphNode", input_node: "Node") -> "Node":
        return graph_node.add_operation(
            node_type=NodeType.IS_NULL,
            node_params={},
            node_output_type=NodeOutputType.SERIES,
            input_nodes=[input_node],
        )


class DisguisedValueImputation(BaseImputationCleaningOperation):
    """
    DisguisedValueImputation class is used to declare the operation to impute the disguised value of a table column.

    If the imputed_value parameter is None, the values to impute are replaced by missing values and the corresponding
    rows are ignored during aggregation operations.

    Examples
    --------
    Create an imputation rule to replace -999 with 0

    >>> fb.DisguisedValueImputation(disguised_values=[-999], imputed_value=0)  # doctest: +SKIP

    Create an imputation rule to treat -1 and -96 as missing

    >>> fb.DisguisedValueImputation(
    ...     disguised_values=[-1, -96], imputed_value=None
    ... )  # doctest: +SKIP
    """

    # class variables
    __fbautodoc__: ClassVar[FBAutoDoc] = FBAutoDoc(
        proxy_class="featurebyte.DisguisedValueImputation"
    )

    # instance variables
    type: Literal[CleaningOperationType.DISGUISED] = Field(
        CleaningOperationType.DISGUISED, frozen=True, repr=False
    )
    disguised_values: Sequence[OptionalScalar] = Field(
        description="List of values that need to be replaced."
    )

    supported_dtypes: ClassVar[Optional[Set[DBVarType]]] = DBVarType.primitive_types()

    @field_validator("disguised_values")
    @classmethod
    def _validate_disguised_values(cls, values: Sequence[Any]) -> Sequence[Any]:
        if len(values) == 0:
            raise ValueError("disguised_values cannot be empty")
        return values

    def cast(self, dtype: DBVarType) -> None:
        super().cast(dtype)
        self.disguised_values = self._cast_list_parameter_to_dtype(self.disguised_values, dtype)

    def derive_sdk_code(self) -> ObjectClass:
        return ClassEnum.DISGUISED_VALUE_IMPUTATION(
            imputed_value=self.imputed_value, disguised_values=self.disguised_values
        )

    def check_condition(self, value: OptionalScalar) -> bool:
        return value in self.disguised_values

    def add_condition_operation(self, graph_node: "GraphNode", input_node: "Node") -> "Node":
        return graph_node.add_operation(
            node_type=NodeType.IS_IN,
            node_params={"value": self.disguised_values},
            node_output_type=NodeOutputType.SERIES,
            input_nodes=[input_node],
        )


class UnexpectedValueImputation(BaseImputationCleaningOperation):
    """
    UnexpectedValueImputation class is used to decalre the operation to impute the unexpected value of a table column.
    Note that this imputation operation will not impute missing value.

    If the imputed_value parameter is None, the values to impute are replaced by missing values and the corresponding
    rows are ignored during aggregation operations.

    Examples
    --------
    Create an imputation rule to replace value other than "buy" or "sell" to "missing"

    >>> fb.UnexpectedValueImputation(
    ...     expected_values=["buy", "sell"], imputed_value="missing"
    ... )  # doctest: +SKIP
    """

    # class variables
    __fbautodoc__: ClassVar[FBAutoDoc] = FBAutoDoc(
        proxy_class="featurebyte.UnexpectedValueImputation"
    )

    # instance variables
    type: Literal[CleaningOperationType.NOT_IN] = Field(
        CleaningOperationType.NOT_IN, frozen=True, repr=False
    )
    expected_values: Sequence[OptionalScalar] = Field(
        description="List of values that are expected to be present."
    )

    supported_dtypes: ClassVar[Optional[Set[DBVarType]]] = DBVarType.primitive_types()

    @field_validator("expected_values")
    @classmethod
    def _validate_expected_values(cls, values: Sequence[Any]) -> Sequence[Any]:
        if len(values) == 0:
            raise ValueError("expected_values cannot be empty")
        return values

    def cast(self, dtype: DBVarType) -> None:
        super().cast(dtype)
        self.expected_values = self._cast_list_parameter_to_dtype(self.expected_values, dtype)

    def derive_sdk_code(self) -> ObjectClass:
        return ClassEnum.UNEXPECTED_VALUE_IMPUTATION(
            imputed_value=self.imputed_value, expected_values=self.expected_values
        )

    def check_condition(self, value: OptionalScalar) -> bool:
        return value not in self.expected_values

    def add_condition_operation(self, graph_node: "GraphNode", input_node: "Node") -> "Node":
        isin_node = graph_node.add_operation(
            node_type=NodeType.IS_IN,
            node_params={"value": self.expected_values},
            node_output_type=NodeOutputType.SERIES,
            input_nodes=[input_node],
        )
        return graph_node.add_operation(
            node_type=NodeType.NOT,
            node_params={},
            node_output_type=NodeOutputType.SERIES,
            input_nodes=[isin_node],
        )


class ValueBeyondEndpointImputation(BaseImputationCleaningOperation):
    """
    ValueBeyondEndpointImputation class is used to declare the operation to impute the value when the value in the
    column exceeds a specified endpoint type.

    If the imputed_value parameter is None, the values to impute are replaced by missing values and the corresponding
    rows are ignored during aggregation operations.

    Examples
    --------
    Create an imputation rule to replace value less than 0 to 0.

    >>> fb.ValueBeyondEndpointImputation(
    ...     type="less_than", end_point=0, imputed_value=0
    ... )  # doctest: +SKIP


    Create an imputation rule to ignore value higher than 1M.

    >>> fb.ValueBeyondEndpointImputation(type="less_than", end_point=1e6, imputed_value=None)
    ValueBeyondEndpointImputation(type=less_than, imputed_value=None, end_point=1000000.0)
    """

    # class variables
    __fbautodoc__: ClassVar[FBAutoDoc] = FBAutoDoc(
        proxy_class="featurebyte.ValueBeyondEndpointImputation"
    )

    # instance variables
    type: Literal[
        CleaningOperationType.LESS_THAN,
        CleaningOperationType.LESS_THAN_OR_EQUAL,
        CleaningOperationType.GREATER_THAN,
        CleaningOperationType.GREATER_THAN_OR_EQUAL,
    ] = Field(
        frozen=True,
        description="Determines how the boundary values are treated.\n"
        "- If type is `less_than`, any value that is less than the end_point "
        "value will be replaced with imputed_value.\n"
        "- If type is `less_than_or_equal`, any value that is less than or "
        "equal to the end_point value will be replaced with imputed_value.\n"
        "- If type is `greater_than`, any value that is greater than the end_"
        "point value will be replaced with imputed_value.\n"
        "- If type is `greater_than_or_equal`, any value that is greater "
        "than or equal to the end_point value will be replaced with "
        "imputed_value.",
    )
    end_point: Scalar = Field(description="The value that marks the boundary.")

    supported_dtypes: ClassVar[Optional[Set[DBVarType]]] = {
        DBVarType.INT,
        DBVarType.FLOAT,
        DBVarType.DATE,
        DBVarType.TIME,
        DBVarType.TIMESTAMP,
        DBVarType.TIMESTAMP_TZ,
    }

    def cast(self, dtype: DBVarType) -> None:
        super().cast(dtype)
        self.end_point = self._cast_scalar_parameter_to_dtype(self.end_point, dtype)

    def derive_sdk_code(self) -> ObjectClass:
        return ClassEnum.VALUE_BEYOND_ENDPOINT_IMPUTATION(
            type=self.type, end_point=self.end_point, imputed_value=self.imputed_value
        )

    def check_condition(self, value: OptionalScalar) -> bool:
        operation_map = {
            CleaningOperationType.LESS_THAN: lambda x: x < self.end_point,
            CleaningOperationType.LESS_THAN_OR_EQUAL: lambda x: x <= self.end_point,
            CleaningOperationType.GREATER_THAN: lambda x: x > self.end_point,
            CleaningOperationType.GREATER_THAN_OR_EQUAL: lambda x: x >= self.end_point,
        }
        try:
            return bool(operation_map[self.type](value))
        except TypeError:
            # TypeError exception implies that two value are incomparable
            return False

    def add_condition_operation(self, graph_node: "GraphNode", input_node: "Node") -> "Node":
        node_type_map = {
            CleaningOperationType.LESS_THAN: NodeType.LT,
            CleaningOperationType.LESS_THAN_OR_EQUAL: NodeType.LE,
            CleaningOperationType.GREATER_THAN: NodeType.GT,
            CleaningOperationType.GREATER_THAN_OR_EQUAL: NodeType.GE,
        }
        return graph_node.add_operation(
            node_type=node_type_map[self.type],
            node_params={"value": self.end_point},
            node_output_type=NodeOutputType.SERIES,
            input_nodes=[input_node],
        )


class StringValueImputation(BaseImputationCleaningOperation):
    """
    StringValueImputation class is used to declare the operation to impute the value when the value in the column is
    of astring type.

    If the imputed_value parameter is None, the values to impute are replaced by missing values and the corresponding
    rows are ignored during aggregation operations.

    Examples
    --------
    Create an imputation rule to replace string value with 0

    >>> fb.StringValueImputation(imputed_value=0)  # doctest: +SKIP
    """

    # class variables
    __fbautodoc__: ClassVar[FBAutoDoc] = FBAutoDoc(proxy_class="featurebyte.StringValueImputation")

    # instance variables
    type: Literal[CleaningOperationType.IS_STRING] = Field(
        CleaningOperationType.IS_STRING, frozen=True, repr=False
    )

    def derive_sdk_code(self) -> ObjectClass:
        return ClassEnum.STRING_VALUE_IMPUTATION(imputed_value=self.imputed_value)

    def check_condition(self, value: OptionalScalar) -> bool:
        return isinstance(value, str)

    def add_condition_operation(self, graph_node: "GraphNode", input_node: "Node") -> "Node":
        return graph_node.add_operation(
            node_type=NodeType.IS_STRING,
            node_params={},
            node_output_type=NodeOutputType.SERIES,
            input_nodes=[input_node],
        )


CLEANING_OPERATION_TYPES = [
    MissingValueImputation,
    DisguisedValueImputation,
    UnexpectedValueImputation,
    ValueBeyondEndpointImputation,
    StringValueImputation,
    AddTimestampSchema,
]
if TYPE_CHECKING:
    CleaningOperation = BaseCleaningOperation
else:
    CleaningOperation = Annotated[
        Union[tuple(CLEANING_OPERATION_TYPES)], Field(discriminator="type")
    ]

# construct function for cleaning operation deserialization
construct_cleaning_operation = construct_serialize_function(
    all_types=CLEANING_OPERATION_TYPES,
    annotated_type=CleaningOperation,
    discriminator_key="type",
)


class ColumnCleaningOperation(FeatureByteBaseModel):
    """
    The ColumnCleaningOperation object serves as a link between a table column and a specific cleaning configuration.
    It is utilized when creating a view in the manual mode that requires different configurations per colum. The
    column_cleaning_operations parameter takes a list of these configurations. For each configuration, the
    ColumnCleaningOperation object establishes the relationship between the colum involved and the corresponding
    cleaning operations.

    Examples
    --------
    Check table cleaning operation of this feature first:

    >>> feature = catalog.get_feature("InvoiceAmountAvg_60days")
    >>> feature.info()["table_cleaning_operation"]
    {'this': [], 'default': []}


    Create a new version of a feature with different table cleaning operations:

    >>> new_feature = feature.create_new_version(  # doctest: +SKIP
    ...     table_cleaning_operations=[
    ...         fb.TableCleaningOperation(
    ...             table_name="GROCERYINVOICE",
    ...             column_cleaning_operations=[
    ...                 fb.ColumnCleaningOperation(
    ...                     column_name="Amount",
    ...                     cleaning_operations=[fb.MissingValueImputation(imputed_value=0.0)],
    ...                 )
    ...             ],
    ...         )
    ...     ]
    ... )
    """

    # class variables
    __fbautodoc__: ClassVar[FBAutoDoc] = FBAutoDoc(
        proxy_class="featurebyte.ColumnCleaningOperation"
    )

    column_name: str = Field(
        description="Name of the column that requires cleaning. The cleaning operations specified in the second "
        "parameter will be applied to this column."
    )
    cleaning_operations: Sequence[CleaningOperation] = Field(
        description="Sequence (e.g., list) of cleaning operations "
        "that will be applied to the specified column. Each cleaning operation is an instance of one of the five "
        "classes that perform specific cleaning tasks on the data. When the cleaning_operations are executed, they "
        "will be applied to the specified column in the order that they appear in the list. Ensure that values "
        "imputed in earlier steps are not marked for cleaning in later operations."
    )


class TableCleaningOperation(FeatureByteBaseModel):
    """
    The TableCleaningOperation object serves as a link between a table and cleaning configurations for the columns in
    the table. It is utilized when creating a new version of a feature that requires different cleaning configurations.
    The table_cleaning_operations parameter takes a list of these configurations. For each configuration, the
    ColumnCleaningOperation object establishes the relationship between the table and the corresponding cleaning
    operations for the table.

    Examples
    --------
    Check table cleaning operation of this feature first:

    >>> feature = catalog.get_feature("InvoiceAmountAvg_60days")
    >>> feature.info()["table_cleaning_operation"]
    {'this': [], 'default': []}


    Create a new version of a feature with different table cleaning operations:

    >>> new_feature = feature.create_new_version(
    ...     table_cleaning_operations=[
    ...         fb.TableCleaningOperation(
    ...             table_name="GROCERYINVOICE",
    ...             column_cleaning_operations=[
    ...                 fb.ColumnCleaningOperation(
    ...                     column_name="Amount",
    ...                     cleaning_operations=[fb.MissingValueImputation(imputed_value=0.0)],
    ...                 )
    ...             ],
    ...         )
    ...     ]
    ... )
    """

    # class variables
    __fbautodoc__: ClassVar[FBAutoDoc] = FBAutoDoc(proxy_class="featurebyte.TableCleaningOperation")

    table_name: str = Field(
        description="Name of the table that requires cleaning. The cleaning operations specified in the second "
        "parameter will be applied to this table."
    )
    column_cleaning_operations: List[ColumnCleaningOperation] = Field(
        description="List of cleaning operations that need to be performed on the columns of the table. The "
        "relationship between each column and its respective cleaning operations is established using the "
        "ColumnCleaningOperation constructor. This constructor takes two inputs: the name of the column and the "
        "cleaning operations that should be applied to that column."
    )

    # pydantic validators
    _validate_unique_column_name = field_validator("column_cleaning_operations")(
        construct_unique_name_validator(field="column_name")
    )


class TableIdCleaningOperation(FeatureByteBaseModel):
    """
    The TableIdCleaningOperation object serves as a link between a table ID and cleaning configurations for the columns
    """

    table_id: PydanticObjectId
    column_cleaning_operations: List[ColumnCleaningOperation]

    # pydantic validators
    _validate_unique_column_name = field_validator("column_cleaning_operations")(
        construct_unique_name_validator(field="column_name")
    )
