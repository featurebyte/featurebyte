"""
This module contains UserDefinedFunction related models
"""

from __future__ import annotations

from typing import Any, ClassVar, List, Optional, Union

import pandas as pd
import pymongo
from pydantic import Field, field_validator
from sqlglot.expressions import select
from typeguard import check_type, typechecked

from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.enum import DBVarType
from featurebyte.models.base import (
    FeatureByteBaseDocumentModel,
    FeatureByteBaseModel,
    NameStr,
    PydanticObjectId,
    UniqueValuesConstraint,
)
from featurebyte.query_graph.model.graph import QueryGraphModel
from featurebyte.query_graph.node.function import (
    GenericFunctionNode,
    GenericFunctionNodeParameters,
    ValueFunctionParameterInput,
)
from featurebyte.query_graph.node.scalar import TimestampValue
from featurebyte.query_graph.sql.ast.base import SQLNodeContext
from featurebyte.query_graph.sql.ast.function import GenericFunctionNode as GenericFunctionSQLNode
from featurebyte.query_graph.sql.common import SQLType
from featurebyte.query_graph.sql.source_info import SourceInfo
from featurebyte.typing import Scalar, Timestamp

# supported function parameter input DBVarType to Python type mapping
function_parameter_dtype_to_python_type = {
    DBVarType.BOOL: bool,
    DBVarType.VARCHAR: str,
    DBVarType.FLOAT: float,
    DBVarType.INT: int,
    DBVarType.TIMESTAMP: pd.Timestamp,
    DBVarType.TIMESTAMP_TZ: pd.Timestamp,
}


def get_default_test_value(dtype: DBVarType) -> Union[Scalar, Timestamp]:
    """
    Get default test value for this type. This is used to generate the test input value
    for user-defined functions.

    Parameters
    ----------
    dtype: DBVarType
        Variable type

    Returns
    -------
    Union[Scalar, Timestamp]

    Raises
    ------
    TypeError
        If the type is not supported
    """
    mapping = {
        DBVarType.BOOL: False,
        DBVarType.VARCHAR: "test",
        DBVarType.FLOAT: 1.0,
        DBVarType.INT: 1,
        DBVarType.TIMESTAMP: pd.Timestamp("2021-01-01"),
        DBVarType.TIMESTAMP_TZ: pd.Timestamp("2021-01-01"),
    }
    value = mapping.get(dtype)
    if value is None:
        supported_dtypes = list(mapping.keys())
        raise TypeError(f"Unsupported dtype: {dtype}, supported dtypes: {supported_dtypes}")
    return value


def get_signature_type_str(dtype: DBVarType) -> str:
    """
    Get the type string for the function signature

    Parameters
    ----------
    dtype: DBVarType
        Data type

    Returns
    -------
    str
    """
    if dtype in DBVarType.supported_timestamp_types():
        return "Timestamp"

    output = DBVarType(dtype).to_type_str()
    if output:
        return output
    return dtype.lower()


class FunctionParameter(FeatureByteBaseModel):
    """
    FunctionParameter is used to define the input parameters of a UserDefinedFunction.

    Parameters
    ----------
    name: str
        Name of the function input parameter (required)
    dtype: DBVarType
        Data type of the function input parameter (required)
    default_value: Optional[Union[Scalar, Timestamp]]
        Default value of the function input parameter (optional). Default value is used to make the function
        input parameter optional.
    test_value: Optional[Union[Scalar, Timestamp]]
        Test value of the function input parameter (optional). Test value is used to check the execution of the
        function during the creation & update of the UserDefinedFunction.

    Examples
    --------
    Create a float function parameter:
    >>> fb.FunctionParameter(name="param1", dtype=fb.enum.DBVarType.FLOAT)
    FunctionParameter(name='param1', dtype='FLOAT', default_value=None, test_value=None)

    Create an integer function parameter with default value 1:
    >>> fb.FunctionParameter(name="param1", dtype=fb.enum.DBVarType.INT, default_value=1)
    FunctionParameter(name='param1', dtype='INT', default_value=1, test_value=None)
    """

    # class variables
    __fbautodoc__: ClassVar[FBAutoDoc] = FBAutoDoc(proxy_class="featurebyte.FunctionParameter")

    # instance variables
    name: str
    dtype: DBVarType
    default_value: Optional[Union[Scalar, Timestamp]] = Field(default=None)
    test_value: Optional[Union[Scalar, Timestamp]] = Field(default=None)

    @property
    def has_default_value(self) -> bool:
        """
        Whether the parameter has default value

        Returns
        -------
        bool
        """
        return self.default_value is not None

    @property
    def has_test_value(self) -> bool:
        """
        Whether the parameter has test value

        Returns
        -------
        bool
        """
        return self.test_value is not None

    @property
    def signature(self) -> str:
        """
        Return the signature of the function parameter

        Returns
        -------
        str
        """
        param_type = get_signature_type_str(self.dtype)
        if self.has_default_value:
            return f"{self.name}: {param_type} = {self.default_value}"
        return f"{self.name}: {param_type}"

    @typechecked
    def __init__(
        self,
        name: str,
        dtype: Union[DBVarType, str],
        default_value: Optional[Union[Scalar, pd.Timestamp]] = None,
        test_value: Optional[Union[Scalar, pd.Timestamp]] = None,
    ) -> None:
        expected_type = function_parameter_dtype_to_python_type.get(DBVarType(dtype))
        if expected_type is None:
            supported_dtypes = list(function_parameter_dtype_to_python_type.keys())
            raise TypeError(f"Unsupported dtype: {dtype}, supported dtypes: {supported_dtypes}")

        # check default value and test value type
        if default_value is not None:
            check_type(value=default_value, expected_type=expected_type)

        if test_value is not None:
            check_type(value=test_value, expected_type=expected_type)

        super().__init__(
            name=name,
            dtype=dtype,
            default_value=default_value,
            test_value=test_value,
        )


class UserDefinedFunctionModel(FeatureByteBaseDocumentModel):
    """
    UserDefinedFunction model stores user defined function information

    name: str
        Name of the UDF to be used when calling through FeatureByte SDK
    sql_function_name: str
        Name of the function used to call in the SQL query
    function_parameters: List[FunctionParameter]
        List of function parameter specification
    catalog_id: Optional[PydanticObjectId]
        Catalog id of the function (if any), if not provided, it can be used across all catalogs
    """

    name: NameStr
    sql_function_name: str
    function_parameters: List[FunctionParameter]
    output_dtype: DBVarType
    signature: str = Field(default_factory=str)
    catalog_id: Optional[PydanticObjectId] = Field(default=None)
    feature_store_id: PydanticObjectId

    @field_validator("name", "sql_function_name")
    @classmethod
    def _validate_function_name(cls, value: str) -> str:
        # check that name or function name is a valid identifier
        if not value.isidentifier():
            raise ValueError(f'"{value}" is not a valid identifier')
        return value

    @field_validator("function_parameters")
    @classmethod
    def _validate_function_parameters(
        cls, value: List[FunctionParameter]
    ) -> List[FunctionParameter]:
        # check that function parameter name is unique and valid
        func_names = set()
        for func_param in value:
            if func_param.name in func_names:
                raise ValueError(f'Function parameter name "{func_param.name}" is not unique')
            if not func_param.name.isidentifier():
                raise ValueError(f'Function parameter name "{func_param.name}" is not valid')
            func_names.add(func_param.name)
        return value

    def generate_signature(self) -> str:
        """
        Generate function signature for the function

        Returns
        -------
        str
        """
        # generate sdk function signature
        param_signature = ", ".join([param.signature for param in self.function_parameters])
        output_type = get_signature_type_str(self.output_dtype)
        return f"{self.name}({param_signature}) -> {output_type}"

    def generate_test_sql(self, source_info: SourceInfo) -> str:
        """
        Generate test SQL query for the function

        Parameters
        ----------
        source_info: SourceInfo
            Source type of the test SQL query

        Returns
        -------
        str
        """
        function_parameters = []
        value: Optional[Union[Scalar, TimestampValue]]
        for param in self.function_parameters:
            if param.has_test_value:
                test_value = param.test_value
            else:
                test_value = get_default_test_value(param.dtype)

            if param.dtype in DBVarType.supported_timestamp_types():
                value = TimestampValue.from_pandas_timestamp(test_value)
            else:
                value = test_value

            function_parameters.append(ValueFunctionParameterInput(value=value, dtype=param.dtype))

        node = GenericFunctionNode(
            name="generic_function_1",
            parameters=GenericFunctionNodeParameters(
                name=self.name,
                sql_function_name=self.sql_function_name,
                function_parameters=function_parameters,
                output_dtype=self.output_dtype,
                function_id=self.id,
            ),
        )
        sql_node = GenericFunctionSQLNode.build(
            context=SQLNodeContext(
                graph=QueryGraphModel(),
                query_node=node,
                input_sql_nodes=[],
                sql_type=SQLType.MATERIALIZE,
                source_info=source_info,
                to_filter_scd_by_current_flag=False,
                event_table_timestamp_filter=None,
                aggregation_specs=None,
                on_demand_entity_filters=None,
                partition_column_filters=None,
            )
        )
        sql_tree = select(sql_node.sql)
        return sql_tree.sql()

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        if not self.signature:
            self.signature = self.generate_signature()

    class Settings(FeatureByteBaseDocumentModel.Settings):
        """
        MongoDB settings
        """

        collection_name = "user_defined_function"
        unique_constraints: List[UniqueValuesConstraint] = [
            UniqueValuesConstraint(
                fields=("_id",),
                conflict_fields_signature={"id": ["_id"]},
                resolution_signature=None,
            ),
        ]

        indexes = FeatureByteBaseDocumentModel.Settings.indexes + [
            [
                ("name", pymongo.TEXT),
                ("description", pymongo.TEXT),
            ]
        ]
