"""
This module contains models used for sdk code extractor.
"""
from __future__ import annotations

from typing import Any, DefaultDict, Dict, List, Optional, Set, Tuple, Union

import ast
import json
import os
from collections import defaultdict
from enum import Enum

from black import FileMode, format_str
from bson import ObjectId
from jinja2 import Template
from pydantic import BaseModel, Field

from featurebyte.common.utils import get_version
from featurebyte.models.base import PydanticObjectId
from featurebyte.query_graph.enum import NodeOutputType
from featurebyte.query_graph.node.metadata.operation import NodeOutputCategory


class ValueStr(str):
    """
    ValueStr class is used to represent value in python code, for example: 1, 'a', 1.23, ['a']
    """

    @classmethod
    def create(cls, value: Any) -> ValueStr:
        """
        Create expression from a given value

        Parameters
        ----------
        value: Any
            Input value

        Returns
        -------
        ExpressionStr
        """
        if isinstance(value, str):
            return ValueStr(json.dumps(value))
        if isinstance(value, ObjectId):
            return ValueStr(f'"{value}"')
        return ValueStr(value)

    def as_input(self) -> str:
        """
        Used as an input in an expression

        Returns
        -------
        str
        """
        return str(self)


class VariableNameStr(str):
    """
    VariableNameStr class is used to represent variable name string in the code. It also includes
    indexed variable, for example: `event_table`, `event_table['timestamp']`.
    """

    def as_input(self) -> str:
        """
        Used as an input in an expression

        Returns
        -------
        str
        """
        return str(self)


class InfoDict(dict):  # type: ignore
    """
    InfoStr class is used to store the information about SDK code generation from the node to its connected node(s).
    """


class ExpressionStr(str):
    """
    ExpressionStr class is used to represent a combination of operations, variables, or values that will
    produce a result when evaluated, for example: `event_table['quantity'] * event_table['price_per_unit']`.
    """

    def as_input(self) -> str:
        """
        Used as an input in an expression

        Returns
        -------
        str
        """
        return f"({self})"


class StatementStr(str):
    """
    StatementStr class is used to represent an instruction that performs specific task, for example:
    `event_table['rate'] = 1.34`, `col = event_view["amount"]`.
    """


class CommentStr(str):
    """
    CommentStr class is used to represent a comment in the code. The comment will be formatted and
    prepended with `#`.
    """


# module import path str pair
ImportPathPair = Tuple[str, str]


class ObjectClass(BaseModel):
    """
    ObjectClass is used as a mock class object which are used to
    - capture the method/function input parameters
    - import objects
    """

    module_path: Optional[str]
    class_name: Optional[str]
    positional_args: List[Any]
    keyword_args: Dict[str, Any]
    callable_name: Optional[str] = Field(default=None)

    def __str__(self) -> str:
        params: List[Union[VariableNameStr, ExpressionStr, ValueStr, str]] = []
        for elem in self.positional_args:
            if not isinstance(elem, VariableNameStr) and not isinstance(elem, ExpressionStr):
                params.append(ValueStr.create(elem))
            else:
                params.append(elem)

        for key, value in self.keyword_args.items():
            if not isinstance(value, VariableNameStr) and not isinstance(value, ExpressionStr):
                params.append(f"{key}={ValueStr.create(value)}")
            else:
                params.append(f"{key}={value}")

        params_str = ", ".join(params)

        if self.class_name is not None:
            # Calling a class constructor or a classmethod
            if self.callable_name:
                return f"{self.class_name}.{self.callable_name}({params_str})"
            return f"{self.class_name}({params_str})"

        # Calling a function
        assert self.callable_name is not None
        return f"{self.callable_name}({params_str})"

    def __repr__(self) -> str:
        return str(self)

    @classmethod
    def _extract_import_helper(cls, obj: Any) -> Set[Tuple[str, str]]:
        output = set()
        if isinstance(obj, ObjectClass):
            output.update(cls._extract_import(obj))
        if isinstance(obj, list):
            for elem in obj:
                output.update(cls._extract_import_helper(elem))
        if isinstance(obj, dict):
            for value in obj.values():
                output.update(cls._extract_import_helper(value))
        return output

    @classmethod
    def _extract_import(cls, obj: ObjectClass) -> Set[ImportPathPair]:
        if obj.module_path is not None:
            assert obj.class_name is not None
            output = {(obj.module_path, obj.class_name)}
        else:
            output = set()
        for pos_arg in obj.positional_args:
            output.update(cls._extract_import_helper(pos_arg))
        for val_arg in obj.keyword_args.values():
            output.update(cls._extract_import_helper(val_arg))
        return output

    def extract_import(self) -> Set[ImportPathPair]:
        """
        Extract set of required (module_path, class_name) tuple for this object

        Returns
        -------
        Set[Tuple[str, str]]
        """
        return self._extract_import(self)


class ClassEnum(Enum):
    """
    ClassEnum is used to store the python package import tagging for specific classes or objects
    """

    # non-featurebyte related
    OBJECT_ID = ("bson", "ObjectId")
    PD_TIMESTAMP = ("pandas", "Timestamp")

    # feature store
    FEATURE_STORE = ("featurebyte", "FeatureStore")

    # database details
    SNOWFLAKE_DETAILS = ("featurebyte", "SnowflakeDetails")
    DATABRICK_DETAILS = ("featurebyte", "DatabricksDetails")
    SPARK_DETAILS = ("featurebyte", "SparkDetails")
    SQLITE_DETAILS = ("featurebyte.query_graph.node.schema", "SQLiteDetails")
    TESTDB_DETAILS = ("featurebyte.query_graph.node.schema", "TestDatabaseDetails")

    # table details & tabular source
    TABLE_DETAILS = ("featurebyte.query_graph.node.schema", "TableDetails")
    TABULAR_SOURCE = ("featurebyte.query_graph.model.common_table", "TabularSource")

    # table
    SOURCE_TABLE = ("featurebyte.api.source_table", "SourceTable")
    EVENT_TABLE = ("featurebyte", "EventTable")
    ITEM_TABLE = ("featurebyte", "ItemTable")
    DIMENSION_TABLE = ("featurebyte", "DimensionTable")
    SCD_TABLE = ("featurebyte", "SCDTable")

    # view
    EVENT_VIEW = ("featurebyte", "EventView")
    ITEM_VIEW = ("featurebyte", "ItemView")
    DIMENSION_VIEW = ("featurebyte", "DimensionView")
    SCD_VIEW = ("featurebyte", "SCDView")
    CHANGE_VIEW = ("featurebyte", "ChangeView")

    # imputations
    MISSING_VALUE_IMPUTATION = ("featurebyte", "MissingValueImputation")
    DISGUISED_VALUE_IMPUTATION = ("featurebyte", "DisguisedValueImputation")
    UNEXPECTED_VALUE_IMPUTATION = ("featurebyte", "UnexpectedValueImputation")
    VALUE_BEYOND_ENDPOINT_IMPUTATION = ("featurebyte", "ValueBeyondEndpointImputation")
    STRING_VALUE_IMPUTATION = ("featurebyte", "StringValueImputation")

    # others
    COLUMN_INFO = ("featurebyte.query_graph.model.column_info", "ColumnInfo")
    FEATURE_JOB_SETTING = ("featurebyte", "FeatureJobSetting")
    TO_TIMEDELTA = ("featurebyte", "to_timedelta")
    COLUMN_CLEANING_OPERATION = ("featurebyte", "ColumnCleaningOperation")
    REQUEST_COLUMN = ("featurebyte.api.request_column", "RequestColumn")
    USER_DEFINED_FUNCTION = ("featurebyte", "UserDefinedFunction")

    def __call__(
        self, *args: Any, _method_name: Optional[str] = None, **kwargs: Any
    ) -> ObjectClass:
        module_path, class_name = self.value
        return ObjectClass(
            module_path=module_path,
            class_name=class_name,
            callable_name=_method_name,
            positional_args=args,
            keyword_args=kwargs,
        )


def get_object_class_from_function_call(
    callable_name: str, *args: Any, **kwargs: Any
) -> ObjectClass:
    """
    Get an instance of ObjectClass to represent a function call

    Parameters
    ----------
    callable_name: str
        Name of the callable, typically a function name
    args: Any
        Positional arguments for the function call
    kwargs: Any
        Keyword arguments for the function call

    Returns
    -------
    ObjectClass

    Raises
    ------
    ValueError
        If module_path and class_name are not both None or both not None
    """
    module_path = kwargs.pop("module_path", None)
    class_name = kwargs.pop("class_name", None)
    if (module_path and not class_name) or (class_name and not module_path):
        raise ValueError("module_path and class_name should be both None or both not None")
    return ObjectClass(
        positional_args=args,
        keyword_args=kwargs,
        callable_name=callable_name,
        module_path=module_path,
        class_name=class_name,
    )


VarNameExpressionStr = Union[VariableNameStr, ExpressionStr]
VarNameExpressionInfo = Union[VariableNameStr, ExpressionStr, InfoDict]
RightHandSide = Union[ValueStr, VariableNameStr, ExpressionStr, ObjectClass]
StatementT = Union[  # pylint: disable=invalid-name
    StatementStr, CommentStr, Tuple[VariableNameStr, RightHandSide]
]


class CodeGenerationConfig(BaseModel):
    """
    CodeGenerationConfig is used to control the code generating style like whether to introduce a new variable to
    store some intermediate results.

    feature_store_id: PydanticObjectId
        Feature store ID used to construct unsaved table object
    feature_store_name: str
        Feature store name used to construct unsaved table object
    table_id_to_info: Dict[PydanticObjectId, Any]
        Mapping from table ID to table info (name, record_creation_timestamp_column, etc)
    final_output_name: str
        Variable name which contains final output
    to_use_saved_data: str
        When enabled, load the table object from the persistent, otherwise construct the table from
        feature store explicitly
    max_expression_length: int
        Maximum expression length used to decide whether to assign the expression into a variable
        to reduce overall statement's line width.
    """

    # values not controlled by the query graph (can be configured outside graph)
    # feature store ID & name
    feature_store_id: PydanticObjectId = Field(default_factory=ObjectId)
    feature_store_name: str = Field(default="feature_store")

    # table ID to table info (name, record_creation_timestamp_column, etc)
    table_id_to_info: Dict[PydanticObjectId, Dict[str, Any]] = Field(default_factory=dict)

    # output variable name used to store the final output
    final_output_name: str = Field(default="output")

    # other configurations
    to_use_saved_data: bool = Field(default=False)
    max_expression_length: int = Field(default=40)


class CodeGenerationContext(BaseModel):
    """
    CodeGenerationContext is used to store the context information during code generation. Code generation context
    include information that need to be passed to the next code generation step.
    """

    # Whether to output current node output as info dict or not. This is used to pass the information to the next
    # code generation step. For example, for the ConditionalNode -> AssignNode structure, ConditionalNode will pass
    # the mask variable & value to the AssignNode, and AssignNode will use the mask variable & value to generate the
    # assignment statement.
    as_info_dict: bool
    # Whether to copy the variable before generate the following SDK statement. This is used when the node operation
    # is in-place operation, and the variable is used in the following SDK statement. See AssignNode,
    # ConditionalNode and AliasNode for example.
    required_copy: bool


class VariableNameGenerator(BaseModel):
    """
    VariableNameGenerator class is used to generate the variable name given the characteristics of the
    value to be stored.
    """

    var_name_counter: DefaultDict[str, int] = Field(default_factory=lambda: defaultdict(int))
    node_name_to_var_name: Dict[str, VariableNameStr] = Field(default_factory=dict)
    func_id_to_var_name: Dict[PydanticObjectId, VariableNameStr] = Field(default_factory=dict)

    def generate_variable_name(
        self,
        node_output_type: NodeOutputType,
        node_output_category: NodeOutputCategory,
        node_name: Optional[str],
    ) -> VariableNameStr:
        """
        Generate a valid variable name (no name collision) based on node output type (series or frame) &
        category (view or feature) characteristic.

        Parameters
        ----------
        node_output_type: NodeOutputType
            Node output type (series or frame)
        node_output_category: NodeOutputCategory
            Node output category (view or feature)
        node_name: Optional[str]
            If not None, the generated variable name will be associated with the node name. If the node name
            already exists, its associated variable name will be returned.

        Returns
        -------
        VariableNameStr
        """
        if node_output_category == NodeOutputCategory.VIEW:
            if node_output_type == NodeOutputType.FRAME:
                pre_variable_name = "view"
            else:
                pre_variable_name = "col"
        else:
            if node_output_type == NodeOutputType.FRAME:
                pre_variable_name = "grouped"
            else:
                pre_variable_name = "feat"

        return self.convert_to_variable_name(
            variable_name_prefix=pre_variable_name, node_name=node_name
        )

    def convert_to_variable_name(
        self,
        variable_name_prefix: str,
        node_name: Optional[str],
        function_id: Optional[PydanticObjectId] = None,
    ) -> VariableNameStr:
        """
        Convert an input variable name into a valid variable name (no name collision).

        Parameters
        ----------
        variable_name_prefix: str
            Variable name before name collision check
        node_name: Optional[str]
            If not None, the generated variable name will be associated with the node name. If the node name
            already exists, its associated variable name will be returned.
        function_id: Optional[PydanticObjectId]
            If not None, the generated variable name will be associated with the function ID. If the function ID
            already exists, its associated variable name will be returned.

        Returns
        -------
        VariableNameStr
        """
        assert not (node_name and function_id), "node_name and function_id cannot be both not None"
        if node_name is not None and node_name in self.node_name_to_var_name:
            return self.node_name_to_var_name[node_name]
        if function_id is not None and function_id in self.func_id_to_var_name:
            return self.func_id_to_var_name[function_id]

        count = self.var_name_counter[variable_name_prefix]
        self.var_name_counter[variable_name_prefix] += 1
        var_name = VariableNameStr(variable_name_prefix)
        if count:
            var_name = VariableNameStr(f"{variable_name_prefix}_{count}")

        if node_name is not None:
            self.node_name_to_var_name[node_name] = var_name
        if function_id is not None:
            self.func_id_to_var_name[function_id] = var_name
        return var_name


class UnusedVariableFinder(ast.NodeVisitor):
    """UnusedVariableFinder class is used to find the unused variables in the generated SDK code."""

    def __init__(self) -> None:
        self.variables: Set[str] = set()
        self.used_variables: Set[str] = set()

    def visit_Name(self, node: ast.Name) -> None:  # pylint: disable=invalid-name
        """
        Visit the Name node in the ast.

        Parameters
        ----------
        node: ast.Name
            Ast name node
        """
        if isinstance(node.ctx, ast.Store):
            # add variable to set of variables
            self.variables.add(node.id)
        elif isinstance(node.ctx, (ast.Load, ast.Del)):
            # add variable to set of used variables
            self.used_variables.add(node.id)

    def get_unused_variables(self) -> Set[str]:
        """
        Get the unused variables found in the ast.

        Returns
        -------
        Set[str]
        """
        return self.variables - self.used_variables


class CodeGenerator(BaseModel):
    """
    SDKCodeGenerator class is used to generate the SDK codes from a list of import tags and statements.

    A statement could be one
    of the following two forms:

        a) Statement: Statement("event_view['ratio'] = 1.34")
        b) Tuple[VariableName, Expression]: (VariableName("col"), Expression("event_view['amount']"))
    """

    statements: List[StatementT] = Field(default_factory=list)

    @staticmethod
    def _get_template() -> Template:
        template_path = os.path.join(os.path.dirname(__file__), "templates/sdk_code.tpl")
        with open(template_path, mode="r", encoding="utf-8") as file_handle:
            return Template(file_handle.read())

    def add_statements(self, statements: List[StatementT]) -> None:
        """
        Add statement to the list of statements

        Parameters
        ----------
        statements: List[Union[Statement, Tuple[VariableNameStr, ExpressionStr]]]
            Statements to be inserted
        """
        self.statements.extend(statements)

    def _generate(
        self, unused_variables: Optional[Set[str]] = None
    ) -> Tuple[str, Set[ImportPathPair]]:
        # process statements & extract required imports
        statement_lines = []
        import_pairs = set()
        for statement in self.statements:
            if isinstance(statement, tuple):
                var_name, right_hand_side = statement
                if unused_variables and var_name in unused_variables:
                    continue
                if isinstance(right_hand_side, ObjectClass):
                    import_pairs.update(right_hand_side.extract_import())
                statement_lines.append(f"{var_name} = {right_hand_side}")
            elif isinstance(statement, CommentStr):
                statement_lines.append(f"\n# {statement}")
            elif isinstance(statement, str):
                statement_lines.append(statement)
        statements = "\n".join(statement_lines)
        return statements, import_pairs

    def generate(
        self,
        to_format: bool = False,
        output_var_name: Optional[str] = None,
        remove_unused_variables: bool = True,
    ) -> str:
        """
        Generate code as string using list of stored statements

        Parameters
        ----------
        to_format: bool
            Whether to format the final code
        output_var_name: Optional[str]
            Output variable name, default to "output"
        remove_unused_variables: bool
            Whether to skip removing unused variables

        Returns
        -------
        str
        """
        # generate statements and extract required imports
        statements, import_pairs = self._generate()

        if remove_unused_variables:
            # extract unused variables
            tree = ast.parse(statements)
            finder = UnusedVariableFinder()
            finder.visit(tree)
            keep_var_names = {output_var_name} if output_var_name else {"output"}
            unused_variables = finder.get_unused_variables().difference(keep_var_names)

            # regenerate statements by removing unused variables
            statements, import_pairs = self._generate(unused_variables=unused_variables)

        # process imports and generate import statements
        import_statements = []
        for module_path, name in sorted(import_pairs):
            import_statements.append(f"from {module_path} import {name}")
        imports = "\n".join(import_statements)

        # prepare final SDK code
        code = self._get_template().render(
            header_comment=f"# Generated by SDK version: {get_version()}",
            imports=imports,
            statements=statements,
        )
        if to_format:
            return format_str(code, mode=FileMode(line_length=80))
        return code
