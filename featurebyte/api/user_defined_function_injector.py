"""
This module contains generic function construction logic for user defined functions.
"""

from __future__ import annotations

import inspect
import textwrap
from dataclasses import dataclass
from typing import Any, Callable, List, Tuple, Type, Union

from typeguard import check_type

from featurebyte.api.feature import Feature
from featurebyte.api.view import ViewColumn
from featurebyte.enum import DBVarType
from featurebyte.models.feature_store import FeatureStoreModel
from featurebyte.models.user_defined_function import (
    FunctionParameter,
    UserDefinedFunctionModel,
    function_parameter_dtype_to_python_type,
)
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.graph import GlobalQueryGraph
from featurebyte.query_graph.model.common_table import TabularSource
from featurebyte.query_graph.node import Node
from featurebyte.query_graph.node.function import (
    ColumnFunctionParameterInput,
    FunctionParameterInput,
    GenericFunctionNodeParameters,
    ValueFunctionParameterInput,
)
from featurebyte.query_graph.node.scalar import TimestampValue


def get_param_type_annotations(dtype: DBVarType) -> object:
    """
    Get parameter type annotations for a given dtype. This is used to dynamically add type annotations to the
    user-defined function.

    Parameters
    ----------
    dtype: DBVarType
        Data type of the parameter

    Returns
    -------
    object

    Raises
    ------
    TypeError
        When dtype is not supported
    """
    py_type = function_parameter_dtype_to_python_type.get(DBVarType(dtype))
    if py_type is None:
        supported_dtypes = list(function_parameter_dtype_to_python_type.keys())
        raise TypeError(f"Unsupported dtype: {dtype}, supported dtypes: {supported_dtypes}")
    return Union[py_type, ViewColumn, Feature]


class FunctionAccessor:
    """
    FunctionAccessor class contains all user-defined functions as its methods.
    Note: Do not add any methods to this class. All methods are dynamically added by the FunctionDescriptor class.
    """


FuncOutputType = Union[Type[ViewColumn], Type[Feature]]
FuncInputSeriesList = List[Union[ViewColumn, Feature]]


@dataclass
class FunctionParameterProcessorOutput:
    """
    FunctionParameterProcessOutput class contains the output of the function parameter processor.
    """

    node_function_parameters: List[FunctionParameterInput]
    input_node_names: List[str]
    feature_store: FeatureStoreModel
    tabular_source: TabularSource
    output_type: FuncOutputType


class FunctionParameterProcessor:
    """
    FunctionParameterProcessor class contains logic to process function parameters.
    """

    def __init__(self, function_parameters: List[FunctionParameter]):
        self.function_parameters = function_parameters

    @staticmethod
    def _extract_parameter_value(
        pos: int, func_param: FunctionParameter, *args: Any, **kwargs: Any
    ) -> Tuple[Any, Any]:
        if pos < len(args):
            value = args[pos]
        else:
            if func_param.name not in kwargs:
                if func_param.has_default_value:
                    value = func_param.default_value
                else:
                    raise ValueError(f'Parameter "{func_param.name}" is not provided.')
            else:
                value = kwargs.pop(func_param.name)
        return value, kwargs

    @staticmethod
    def _validate_feature_inputs(feature_inputs: List[Feature]) -> None:
        for index, feat in enumerate(feature_inputs, start=1):
            if feat.used_request_column:
                raise ValueError(
                    f'Error in feature #{index} ("{feat.name}"): This feature was created with a request column '
                    "and cannot be used as an input to this function. Please change the feature and try again."
                )

    @classmethod
    def _validate_series_inputs(cls, series_inputs: FuncInputSeriesList) -> None:
        expected_row_index_lineage = series_inputs[0].row_index_lineage
        check_row_index_lineage = not isinstance(series_inputs[0], Feature)
        expected_series_type = Feature if isinstance(series_inputs[0], Feature) else ViewColumn
        for series_input in series_inputs[1:]:
            if not isinstance(series_input, expected_series_type):
                # check if all view columns have matching type
                raise TypeError(
                    f'Input "{series_input.name}" has type {type(series_input).__name__} '
                    f"but expected {expected_series_type.__name__}."
                )

            if (
                check_row_index_lineage
                and series_input.row_index_lineage != expected_row_index_lineage
            ):
                # check if all view columns have matching row index lineage
                raise ValueError(
                    f'The row of the input ViewColumns "{series_inputs[0].name}" does not match '
                    f'the row of the input ViewColumns "{series_input.name}".'
                )

        if expected_series_type is Feature:
            cls._validate_feature_inputs(series_inputs)  # type: ignore

    def _extract_node_parameters(
        self, *args: Any, **kwargs: Any
    ) -> Tuple[List[FunctionParameterInput], FuncInputSeriesList]:
        if len(args) > len(self.function_parameters):
            raise ValueError(
                f"Too many arguments. Expected {len(self.function_parameters)} but got {len(args)}."
            )

        # extract input parameters based on the function parameter specification
        func_params = []
        series_inputs = []
        value: Any
        for i, param in enumerate(self.function_parameters):
            # extract the parameter value from the arguments
            value, kwargs = self._extract_parameter_value(i, param, *args, **kwargs)
            check_type(value=value, expected_type=get_param_type_annotations(param.dtype))

            # prepare generic function parameter
            func_param: FunctionParameterInput
            if isinstance(value, (ViewColumn, Feature)):
                if value.dtype != param.dtype:
                    raise TypeError(
                        f'Parameter "{param.name}" has dtype {param.dtype} but '
                        f"input ViewColumn or Feature (name: {value.name}) has dtype {value.dtype}."
                    )
                func_param = ColumnFunctionParameterInput(column_name=value.name, dtype=param.dtype)
                series_inputs.append(value)
            else:
                if param.dtype in DBVarType.supported_timestamp_types():
                    value = TimestampValue.from_pandas_timestamp(value)
                func_param = ValueFunctionParameterInput(value=value, dtype=param.dtype)

            func_params.append(func_param)

        # handle the remaining parameters
        if kwargs:
            raise ValueError(f"Unknown keyword arguments: {list(kwargs.keys())}")

        if not series_inputs:
            raise ValueError("At least one parameter must be a ViewColumn or Feature.")

        # validate the series inputs
        if len(series_inputs) > 1:
            self._validate_series_inputs(series_inputs)

        return func_params, series_inputs

    def process(self, *args: Any, **kwargs: Any) -> FunctionParameterProcessorOutput:
        """
        Process function parameters by validating the input parameters and extracting the parameters
        for further processing (e.g. creating a generic function node).

        Parameters
        ----------
        args: Any
            Positional arguments obtained from the function call
        kwargs: Any
            Keyword arguments obtained from the function call

        Returns
        -------
        FunctionParameterProcessorOutput
        """
        # extract input parameters based on the function parameter specification
        func_params, series_inputs = self._extract_node_parameters(*args, **kwargs)

        series_input = series_inputs[0]
        input_node_names = [series_input.node_name for series_input in series_inputs]

        output_type: FuncOutputType
        if isinstance(series_input, Feature):
            output_type = Feature
        else:
            output_type = ViewColumn

        return FunctionParameterProcessorOutput(
            node_function_parameters=func_params,
            input_node_names=input_node_names,
            feature_store=series_input.feature_store,
            tabular_source=series_input.tabular_source,
            output_type=output_type,
        )


class UserDefinedFunctionInjector:
    """
    UserDefinedFunctionInjector class contains logic to construct a dynamic function based on
    the user defined function and inject it into the FunctionAccessor class.
    """

    @staticmethod
    def _insert_generic_function_node(
        function_parameters: List[FunctionParameterInput],
        input_node_names: List[str],
        udf: UserDefinedFunctionModel,
    ) -> Node:
        node_params = GenericFunctionNodeParameters(
            name=udf.name,
            sql_function_name=udf.sql_function_name,
            function_parameters=function_parameters,
            output_dtype=udf.output_dtype,
            function_id=udf.id,
        )
        graph = GlobalQueryGraph()
        input_nodes = [
            graph.get_node_by_name(input_node_name) for input_node_name in input_node_names
        ]
        node = graph.add_operation(
            node_type=NodeType.GENERIC_FUNCTION,
            node_params=node_params.model_dump(by_alias=True),
            node_output_type=NodeOutputType.SERIES,
            input_nodes=input_nodes,
        )
        return node

    @staticmethod
    def _generate_docstring(udf: UserDefinedFunctionModel) -> str:
        parameters = "\n        ".join([
            f"{param.name} : {get_param_type_annotations(param.dtype)}"
            for param in udf.function_parameters
        ])
        docstring_template = f"""
        This function uses the feature store SQL function `{udf.sql_function_name}` to generate a new column
        or new feature. Note that the non-scalar input parameters must be homogeneous, i.e., they must
        be either all view columns or all features. The output type is determined by the input type.
        If all input parameters are view columns, then the output type is a view column. If all input
        parameters are features, then the output type is a feature. All view columns input parameters
        must have matching row alignment. Otherwise, an error is raised.

        Parameters
        ----------
        {parameters}

        Returns
        -------
        Union[ViewColumn, Feature]
            A new column or new feature generated by the feature store function `{udf.sql_function_name}`.
        """
        return textwrap.dedent(docstring_template).strip()

    @classmethod
    def create(cls, udf: UserDefinedFunctionModel) -> Callable[..., Union[ViewColumn, Feature]]:
        """
        Create a dynamic method based on the user defined function.

        Parameters
        ----------
        udf: UserDefinedFunctionModel
            The user defined function model which contains the function information

        Returns
        -------
        Callable[..., Union[ViewColumn, Feature]]
            A dynamic method containing the logic to generate a new column or new feature
        """

        def _user_defined_function(*args: Any, **kwargs: Any) -> Union[ViewColumn, Feature]:
            param_processor = FunctionParameterProcessor(udf.function_parameters)
            extracted_params = param_processor.process(*args, **kwargs)
            node = cls._insert_generic_function_node(
                function_parameters=extracted_params.node_function_parameters,
                input_node_names=extracted_params.input_node_names,
                udf=udf,
            )
            return extracted_params.output_type(
                name=None,
                node_name=node.name,
                feature_store=extracted_params.feature_store,
                tabular_source=extracted_params.tabular_source,
                dtype=udf.output_dtype,
            )

        return _user_defined_function

    @classmethod
    def create_and_add_function(
        cls, func_accessor: FunctionAccessor, udf: UserDefinedFunctionModel
    ) -> None:
        """
        Create a dynamic method based on the user defined function and add it to the function accessor.

        Parameters
        ----------
        func_accessor: FunctionAccessor
            The function accessor instance which contains the dynamic method
        udf: UserDefinedFunctionModel
            The user defined function model which contains the function information
        """
        # create & assign the dynamic method to the function accessor
        dynamic_method = cls.create(udf)
        setattr(func_accessor, udf.name, dynamic_method)
        method = getattr(func_accessor, udf.name)

        # override name, signature, docstring, etc of the dynamic method
        method.__name__ = udf.name
        method.__qualname__ = f"UDF.{udf.name}"
        method.__module__ = "featurebyte"
        method.__doc__ = cls._generate_docstring(udf)
        signature = inspect.signature(method)
        new_parameters = [
            inspect.Parameter(
                param.name,
                inspect.Parameter.POSITIONAL_OR_KEYWORD,
                annotation=get_param_type_annotations(param.dtype),
            )
            for param in udf.function_parameters
        ]
        new_signature = signature.replace(parameters=new_parameters)
        method.__signature__ = new_signature
