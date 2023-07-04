"""
This module contains UserDefinedFunction related models
"""
from __future__ import annotations

from typing import Any, List, Optional, Union

from pydantic import Field, validator
from sqlglot.expressions import select

from featurebyte.common.typing import Scalar
from featurebyte.enum import DBVarType, SourceType
from featurebyte.models.base import (
    FeatureByteBaseDocumentModel,
    FeatureByteBaseModel,
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


class FunctionParameter(FeatureByteBaseModel):
    """
    FunctionParameter class

    name: str
        Name of the generic function parameter (used for documentation purpose)
    dtype: DBVarType
        Data type of the parameter
    default_value: Optional[Any]
        Default value of the parameter
    test_value: Optional[Any]
        Test value of the parameter
    has_default_value: bool
        Whether the parameter has default value
    has_test_value: bool
        Whether the parameter has test value
    """

    name: str
    dtype: DBVarType
    default_value: Optional[Scalar]
    test_value: Optional[Scalar]

    # attributes below are used for indicating whether the parameters have certain values
    has_default_value: bool
    has_test_value: bool

    @property
    def signature(self) -> str:
        """
        Return the signature of the function parameter

        Returns
        -------
        str
        """
        param_type = DBVarType(self.dtype).to_type_str()
        if self.has_default_value:
            return f"{self.name}: {param_type} = {self.default_value}"
        return f"{self.name}: {param_type}"


class UserDefinedFunctionModel(FeatureByteBaseDocumentModel):
    """
    UserDefinedFunction model stores user defined function information

    function_name: str
        Name of the function used to call in the SQL query
    function_parameters: List[FunctionParameter]
        List of function parameter specification
    catalog_id: Optional[PydanticObjectId]
        Catalog id of the function (if any), if not provided, it can be used across all catalogs
    """

    function_name: str
    function_parameters: List[FunctionParameter]
    output_dtype: DBVarType
    signature: str = Field(default_factory=str)
    catalog_id: Optional[PydanticObjectId]
    feature_store_id: PydanticObjectId

    @validator("function_name")
    @classmethod
    def _validate_function_name(cls, value: str) -> str:
        # check that function name is a valid function name
        if not value.isidentifier():
            raise ValueError(f'Function name "{value}" is not valid')
        return value

    @validator("function_parameters")
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

    def _generate_signature(self) -> str:
        # generate sdk function signature
        param_signature = ", ".join([param.signature for param in self.function_parameters])
        output_type = DBVarType(self.output_dtype).to_type_str()
        return f"{self.name}({param_signature}) -> {output_type}"

    @staticmethod
    def get_default_test_value(dtype: DBVarType) -> Union[Scalar, TimestampValue]:
        """
        Get default test value for this type. This is used to generate the test input value
        for user-defined functions.

        Parameters
        ----------
        dtype: DBVarType
            Variable type

        Returns
        -------
        Union[Scalar, TimestampValue]

        Raises
        ------
        ValueError
            If the type is not supported
        """
        mapping = {
            DBVarType.BOOL: False,
            DBVarType.VARCHAR: "test",
            DBVarType.FLOAT: 1.0,
            DBVarType.INT: 1,
            DBVarType.TIMESTAMP: TimestampValue(iso_format_str="2021-01-01 00:00:00"),
            DBVarType.TIMESTAMP_TZ: TimestampValue(iso_format_str="2021-01-01 00:00:00+00:00"),
        }
        value = mapping.get(dtype)
        if value is None:
            raise ValueError(f"Unsupported type {dtype}")
        return value  # type: ignore

    def generate_test_sql(self, source_type: SourceType) -> str:
        """
        Generate test SQL query for the function

        Parameters
        ----------
        source_type: SourceType
            Source type of the test SQL query

        Returns
        -------
        str
        """
        function_parameters = []
        value: Optional[Union[Scalar, TimestampValue]]
        for param in self.function_parameters:
            if param.has_test_value:
                value = param.test_value
            else:
                value = self.get_default_test_value(param.dtype)
            function_parameters.append(ValueFunctionParameterInput(value=value, dtype=param.dtype))

        node = GenericFunctionNode(
            name="generic_function_1",
            parameters=GenericFunctionNodeParameters(
                function_name=self.function_name,
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
                source_type=source_type,
                to_filter_scd_by_current_flag=False,
            )
        )
        sql_tree = select(sql_node.sql)
        return sql_tree.sql()

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        if not self.signature:
            self.signature = self._generate_signature()

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
