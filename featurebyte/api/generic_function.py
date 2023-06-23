"""
This module contains generic function construction logic for user defined functions.
"""
from __future__ import annotations

import types
from typing import Any, List, Tuple

import inspect

from featurebyte.api.view import ViewColumn
from featurebyte.core.series import Series
from featurebyte.enum import FunctionParameterInputForm
from featurebyte.models.feature_store import FeatureStoreModel
from featurebyte.models.user_defined_function import UserDefinedFunctionModel
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.graph import GlobalQueryGraph
from featurebyte.query_graph.model.common_table import TabularSource
from featurebyte.query_graph.node import Node
from featurebyte.query_graph.node.function import (
    FunctionParameterInput,
    GenericFunctionNodeParameters,
)


class FunctionAccessor:
    """
    FunctionAccessor class contains all user-defined functions as its methods.
    Note: Do not add any methods to this class. All methods are dynamically added by the FunctionDescriptor class.
    """


class UserDefinedFunctionRegistry:
    """
    UserDefinedFunctionRegistry class contains logic to register user-defined functions to the FunctionAccessor class.
    """

    @staticmethod
    def _process_keyword_arguments(
        udf: UserDefinedFunctionModel, **kwargs: Any
    ) -> Tuple[List[FunctionParameterInput], List[str], FeatureStoreModel, TabularSource]:
        # extract input parameters based on the function parameter specification
        func_params = []
        series_inputs: List[Series] = []
        for param in udf.function_parameters:
            if param.name not in kwargs:
                if param.has_default_value:
                    value = param.default_value
                else:
                    raise ValueError(f"Parameter {param.name} is not provided")
            else:
                value = kwargs.pop(param.name)

            if isinstance(value, Series):
                input_form = FunctionParameterInputForm.COLUMN
                series_inputs.append(value)
                column_name = value.name
                value = None
            else:
                input_form = FunctionParameterInputForm.VALUE
                column_name = None

            func_params.append(
                FunctionParameterInput(
                    value=value,
                    dtype=param.dtype,
                    input_form=input_form,
                    column_name=column_name,
                )
            )

        # handle the remaining parameters
        if kwargs:
            raise ValueError(f"Unknown parameters {kwargs}")

        if not series_inputs:
            raise ValueError("At least one parameter must be a series.")

        series_input = series_inputs[0]
        input_node_names = [series_input.node_name for series_input in series_inputs]
        return (
            func_params,
            input_node_names,
            series_input.feature_store,
            series_input.tabular_source,
        )

    @staticmethod
    def _insert_generic_function_node(
        function_parameters: List[FunctionParameterInput],
        input_node_names: List[str],
        udf: UserDefinedFunctionModel,
    ) -> Node:
        node_params = GenericFunctionNodeParameters(
            function_name=udf.function_name,
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
            node_params=node_params.dict(by_alias=True),
            node_output_type=NodeOutputType.SERIES,
            input_nodes=input_nodes,
        )
        return node

    @classmethod
    def register(cls, func_accessor: FunctionAccessor, udf: UserDefinedFunctionModel) -> None:
        def _method_wrapper(obj: FunctionAccessor, **kwargs: Any) -> ViewColumn:
            _ = obj
            (
                func_params,
                input_node_names,
                feature_store,
                tabular_source,
            ) = cls._process_keyword_arguments(udf, **kwargs)
            node = cls._insert_generic_function_node(
                function_parameters=func_params,
                input_node_names=input_node_names,
                udf=udf,
            )
            return ViewColumn(
                name=None,
                node_name=node.name,
                feature_store=feature_store,
                tabular_source=tabular_source,
                dtype=udf.output_dtype,
            )

        # assign the dynamic method to the function accessor
        assert udf.name is not None
        dynamic_func = types.MethodType(_method_wrapper, func_accessor)
        setattr(func_accessor, udf.name, dynamic_func)

        # override the signature of the dynamic method
        method = getattr(func_accessor, udf.name)
        signature = inspect.signature(method)
        parameters = [inspect.Parameter("self", inspect.Parameter.POSITIONAL_OR_KEYWORD)]
        for param in udf.function_parameters:
            parameters.append(
                inspect.Parameter(param.name, inspect.Parameter.POSITIONAL_OR_KEYWORD)
            )
        new_signature = signature.replace(parameters=parameters)
        method.__func__.__signature__ = new_signature
