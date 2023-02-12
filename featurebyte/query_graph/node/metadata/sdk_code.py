"""
This module contains models used for sdk code extractor.
"""
from __future__ import annotations

from typing import DefaultDict, List, Sequence, Set, Tuple, Union

import json
import os
from collections import defaultdict
from enum import Enum

from jinja2 import Environment, FileSystemLoader, Template
from pydantic import BaseModel, Field

from featurebyte.common.typing import Scalar
from featurebyte.common.utils import get_version
from featurebyte.query_graph.enum import NodeOutputType
from featurebyte.query_graph.node.metadata.operation import NodeOutputCategory


class ValueStr(str):
    """
    ValueStr class is used to represent value in python code, for example: 1, 'a', 1.23, ['a']
    """

    @classmethod
    def create(cls, value: Union[Scalar, Sequence[Scalar]]) -> ValueStr:
        """
        Create expression from a given value

        Parameters
        ----------
        value: Union[Scalar, Sequence[Scalar]]
            Input value

        Returns
        -------
        ExpressionStr
        """
        if isinstance(value, str):
            return ValueStr(json.dumps(value))
        return ValueStr(f"{value}")

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
    indexed variable, for example: `event_data`, `event_data['timestamp']`.
    """

    def as_input(self) -> str:
        """
        Used as an input in an expression

        Returns
        -------
        str
        """
        return str(self)


class ExpressionStr(str):
    """
    ExpressionStr class is used to represent a combination of operations, variables, or values that will
    produce a result when evaluated, for example: `event_data['quantity'] * event_data['price_per_unit']`.
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
    `event_data['rate'] = 1.34`, `col = event_view["amount"]`.
    """


VarNameExpression = Union[VariableNameStr, ExpressionStr]
RightHandSideT = Union[ValueStr, VariableNameStr, ExpressionStr]
StatementStrT = Union[StatementStr, Tuple[VariableNameStr, RightHandSideT]]


class ImportTag(Enum):
    """
    ImportTag enum is used to store the python package import tagging for specific classes or objects
    """

    EVENT_DATA = ("featurebyte.api.event_data", "EventData")


class StyleConfig(BaseModel):
    """
    StyleConfig is used to control the code generating style like whether to introduce a new variable to
    store some intermediate results.
    """

    max_expression_length: int = Field(default=60)


class VariableNameGenerator(BaseModel):
    """
    VariableNameGenerator class is used to generate the variable name given the characteristics of the
    value to be stored.
    """

    var_name_counter: DefaultDict[str, int] = Field(default_factory=lambda: defaultdict(int))

    def generate_variable_name(
        self,
        node_output_type: NodeOutputType,
        node_output_category: NodeOutputCategory,
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

        return self.convert_to_variable_name(pre_variable_name=pre_variable_name)

    def convert_to_variable_name(self, pre_variable_name: str) -> VariableNameStr:
        """
        Convert an input variable name into a valid variable name (no name collision).

        Parameters
        ----------
        pre_variable_name: str
            Variable name before name collision check

        Returns
        -------
        VariableNameStr
        """
        count = self.var_name_counter[pre_variable_name]
        self.var_name_counter[pre_variable_name] += 1
        if count:
            return VariableNameStr(f"{pre_variable_name}_{count}")
        return VariableNameStr(pre_variable_name)


class CodeGenerator(BaseModel):
    """
    SDKCodeGenerator class is used to generate the SDK codes from a list of import tags and statements.

    A statement could be one
    of the following two forms:

        a) Statement: Statement("event_view['ratio'] = 1.34")
        b) Tuple[VariableName, Expression]: (VariableName("col"), Expression("event_view['amount']"))
    """

    imports: Set[ImportTag] = Field(default_factory=set)
    statements: List[StatementStrT] = Field(default_factory=list)

    @staticmethod
    def _get_template() -> Template:
        template_path = os.path.join(os.path.dirname(__file__), "templates")
        environment = Environment(loader=FileSystemLoader(template_path))
        return environment.get_template("sdk_code.tpl")

    def add_statements(self, statements: List[StatementStrT], imports: List[ImportTag]) -> None:
        """
        Add statement to the list of statements

        Parameters
        ----------
        statements: List[Union[Statement, Tuple[VariableNameStr, ExpressionStr]]]
            Statements to be inserted
        imports: List[ImportTag]
            List of import tags to be inserted
        """
        self.statements.extend(statements)
        self.imports.update(imports)

    def generate(self) -> str:
        """
        Generate code as string using list of stored statements

        Returns
        -------
        str
        """
        import_lines = []
        for import_tag in self.imports:
            package_path, name = import_tag.value
            import_lines.append(f"from {package_path} import {name}")
        imports = "\n".join(import_lines)

        statement_lines = []
        for statement in self.statements:
            if isinstance(statement, tuple):
                var_name, var_name_or_expr = statement
                statement_lines.append(f"{var_name} = {var_name_or_expr}")
            else:
                statement_lines.append(statement)
        statements = "\n".join(statement_lines)

        code = self._get_template().render(
            header_comment=f"# Generated by SDK version: {get_version()}",
            imports=imports,
            statements=statements,
        )
        return code
