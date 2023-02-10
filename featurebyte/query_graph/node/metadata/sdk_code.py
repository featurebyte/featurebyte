"""
This module contains models used for sdk code extractor.
"""
from __future__ import annotations

from typing import DefaultDict, List, Optional, Tuple, Union

from collections import defaultdict

from pydantic import BaseModel, Field

from featurebyte.common.typing import Scalar
from featurebyte.enum import TableDataType
from featurebyte.query_graph.enum import NodeOutputType
from featurebyte.query_graph.node.metadata.operation import NodeOutputCategory


class LiteralStr(str):
    """
    LiteralStr class is used to represent literal value, for example: 1, 'a', 1.23
    """

    @classmethod
    def create(cls, value: Scalar) -> LiteralStr:
        """
        Create expression from a given scalar value

        Parameters
        ----------
        value: Scalar
            Scalar value

        Returns
        -------
        ExpressionStr
        """
        if isinstance(value, str):
            return LiteralStr(f"'{value}'")
        return LiteralStr(f"{value}")

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
    VariableName class is used to represent variable name string in the code. It also includes
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
    Expression class is used to represent a combination of operations, variables, or values that will
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


class Statement(str):
    """
    Statement class is used to represent an instruction that performs specific task, for example:
    `event_data['rate'] = 1.34`, `col = event_view["amount"]`.
    """


VarNameExpression = Union[VariableNameStr, ExpressionStr]
StatementT = Union[Statement, Tuple[VariableNameStr, VarNameExpression]]


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
        data_type: Optional[TableDataType] = None,
        is_data: bool = False,
    ) -> VariableNameStr:
        data_type_map = {
            TableDataType.EVENT_DATA: "event_",
            TableDataType.ITEM_DATA: "item_",
            TableDataType.SCD_DATA: "scd_",
            TableDataType.DIMENSION_DATA: "dimension_",
            TableDataType.GENERIC: "",
        }
        if data_type in data_type_map:
            suffix = "data" if is_data else "view"
            base_var_name = f"{data_type_map[data_type]}{suffix}"
        elif node_output_category == NodeOutputCategory.VIEW:
            if node_output_type == NodeOutputType.FRAME:
                base_var_name = "view"
            else:
                base_var_name = "col"
        else:
            if node_output_type == NodeOutputType.FRAME:
                base_var_name = "grouped"
            else:
                base_var_name = "feat"

        count = self.var_name_counter[base_var_name]
        self.var_name_counter[base_var_name] += 1
        if count:
            return VariableNameStr(f"{base_var_name}_{count}")
        return VariableNameStr(base_var_name)


class CodeGenerator(BaseModel):
    """
    SDKCodeGenerator class is used to generate the SDK codes from a list of statements. A statement could be one
    of the following two forms:

        a) Statement: Statement("event_view['ratio'] = 1.34")
        b) Tuple[VariableName, Expression]: (VariableName("col"), Expression("event_view['amount']"))
    """

    statements: List[StatementT] = Field(default_factory=list)

    def add_statements(self, statements: List[StatementT]) -> None:
        """
        Add statement to the list of statements

        Parameters
        ----------
        statements: List[Union[Statement, Tuple[VariableNameStr, ExpressionStr]]]
            Statements to be inserted
        """
        self.statements.extend(statements)

    def generate(self, to_format: bool = False) -> str:
        """
        Generate code as string using list of stored statements

        Parameters
        ----------
        to_format: bool
            Whether to format the codes

        Returns
        -------
        str
        """
        code_lines = []
        for statement in self.statements:
            if isinstance(statement, tuple):
                var_name, var_name_or_expr = statement
                code_lines.append(f"{var_name} = {var_name_or_expr}")
            else:
                code_lines.append(statement)

        codes = "\n".join(code_lines)

        if to_format:
            from black import FileMode, format_str

            codes = format_str(codes, mode=FileMode())
        return codes
