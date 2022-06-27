"""
Series class
"""
from __future__ import annotations

from typing import Any, Optional, Tuple

from pydantic import Field, PrivateAttr

from featurebyte.core.generic import BaseFrame, QueryObject
from featurebyte.core.mixin import OpsMixin
from featurebyte.enum import DBVarType
from featurebyte.query_graph.enum import NodeOutputType, NodeType


class Series(QueryObject, OpsMixin):
    """
    Implement operations to manipulate database column
    """

    name: Optional[str] = Field(default=None)
    var_type: DBVarType
    lineage: Tuple[str, ...]
    _parent_frame: Optional[BaseFrame] = PrivateAttr(default=None)

    @property
    def parent_frame(self) -> Optional[BaseFrame]:
        """
        Parent Frame object of the current series

        Returns
        -------
        BaseFrame
        """
        return self._parent_frame

    def set_parent_frame(self, frame: BaseFrame) -> Series:
        """
        Set parent frame object

        Parameters
        ----------
        frame: BaseFrame

        Returns
        -------
        Series
        """
        self._parent_frame = frame
        return self

    def __repr__(self) -> str:
        return (
            f"{type(self).__name__}[{self.var_type}](name={self.name}, node.name={self.node.name})"
        )

    def __str__(self) -> str:
        return repr(self)

    def __getitem__(self, item: Series) -> Series:
        if isinstance(item, Series):
            node = self._add_filter_operation(
                item=self, mask=item, node_output_type=NodeOutputType.SERIES
            )
            return type(self)(
                tabular_source=self.tabular_source,
                node=node,
                name=self.name,
                var_type=self.var_type,
                lineage=self._append_to_lineage(self.lineage, node.name),
                row_index_lineage=self._append_to_lineage(self.row_index_lineage, node.name),
                parent_frame=self.parent_frame,
            )
        raise KeyError(f"Series indexing with value '{item}' not supported!")

    @staticmethod
    def _is_assignment_valid(left_dbtype: DBVarType, right_value: Any) -> bool:
        """
        Check whether the right value python builtin type can be assigned to left value database type.

        Parameters
        ----------
        left_dbtype: DBVarType
            target database variable type
        right_value: Any
            value to be assigned to the left object

        Returns
        -------
        bool
            whether the assignment operation is valid in terms of variable type
        """
        valid_assignment_map = {
            DBVarType.BOOL: (bool,),
            DBVarType.INT: (int,),
            DBVarType.FLOAT: (int, float),
            DBVarType.CHAR: (),
            DBVarType.VARCHAR: (str,),
            DBVarType.DATE: (),
        }
        return isinstance(right_value, valid_assignment_map[left_dbtype])

    def __setitem__(self, key: Series, value: int | float | str | bool) -> None:
        if isinstance(key, Series) and self.is_supported_scalar_pytype(value):
            if self.row_index_lineage != key.row_index_lineage:
                raise ValueError(f"Row indices between '{self}' and '{key}' are not aligned!")
            if key.var_type != DBVarType.BOOL:
                raise TypeError("Only boolean Series filtering is supported!")
            if not self._is_assignment_valid(self.var_type, value):
                raise ValueError(f"Setting key '{key}' with value '{value}' not supported!")
            self.node = self.graph.add_operation(
                node_type=NodeType.COND_ASSIGN,
                node_params={"value": value},
                node_output_type=NodeOutputType.SERIES,
                input_nodes=[self.node, key.node],
            )
            self.lineage = self._append_to_lineage(self.lineage, self.node.name)
        else:
            raise TypeError(f"Setting key '{key}' with value '{value}' not supported!")

    @staticmethod
    def _is_a_series_of_var_type(item: Any, var_type: DBVarType) -> bool:
        """
        Check whether the input item is Series type and has the specified variable type

        Parameters
        ----------
        item: Any
            input item
        var_type: DBVarType
            specified database variable type

        Returns
        -------
        bool
            whether the input item is a Series with the specified database variable type
        """
        return isinstance(item, Series) and item.var_type == var_type

    def _binary_op(
        self,
        other: int | float | str | bool | Series,
        node_type: NodeType,
        output_var_type: DBVarType,
        right_op: bool = False,
    ) -> Series:
        """
        Apply binary operation between self & other objects

        Parameters
        ----------
        other: int | float | str | bool | Series
            right value of the binary operator
        node_type: NodeType
            binary operator node type
        output_var_type: DBVarType
            output of the variable type
        right_op: bool
            whether the binary operation is from right object or not

        Returns
        -------
        Series
            output of the binary operation
        """
        node_params: dict[str, Any] = {"right_op": right_op} if right_op else {}
        if self.is_supported_scalar_pytype(other):
            node_params["value"] = other
            node = self.graph.add_operation(
                node_type=node_type,
                node_params=node_params,
                node_output_type=NodeOutputType.SERIES,
                input_nodes=[self.node],
            )
            return type(self)(
                tabular_source=self.tabular_source,
                node=node,
                name=None,
                var_type=output_var_type,
                lineage=self._append_to_lineage(self.lineage, node.name),
                row_index_lineage=self.row_index_lineage,
            )

        node = self.graph.add_operation(
            node_type=node_type,
            node_params={},
            node_output_type=NodeOutputType.SERIES,
            input_nodes=[self.node, other.node],  # type: ignore
        )
        return type(self)(
            tabular_source=self.tabular_source,
            node=node,
            name=None,
            var_type=output_var_type,
            lineage=self._append_to_lineage(self.lineage, node.name),
            row_index_lineage=self.row_index_lineage,
        )

    def _binary_logical_op(self, other: bool | Series, node_type: NodeType) -> Series:
        """
        Apply binary logical operation between self & other objects

        Parameters
        ----------
        other: bool | Series
            right value of the binary logical operator
        node_type: NodeType
            binary logical operator node type

        Returns
        -------
        Series
            output of the binary logical operation

        Raises
        ------
        TypeError
            if the left value or right value of the operator are not boolean
        """
        if self.var_type == DBVarType.BOOL:
            if isinstance(other, bool) or self._is_a_series_of_var_type(other, DBVarType.BOOL):
                return self._binary_op(
                    other=other, node_type=node_type, output_var_type=DBVarType.BOOL
                )

        raise TypeError(f"Not supported operation '{node_type}' between '{self}' and '{other}'!")

    def __and__(self, other: bool | Series) -> Series:
        return self._binary_logical_op(other, NodeType.AND)

    def __or__(self, other: bool | Series) -> Series:
        return self._binary_logical_op(other, NodeType.OR)

    def _binary_relational_op(
        self, other: int | float | str | bool | Series, node_type: NodeType
    ) -> Series:
        """
        Apply binary relational operation between self & other objects

        Parameters
        ----------
        other: int | float | str | bool | Series
            right value of the binary relational operator
        node_type: NodeType
            binary relational operator node type

        Returns
        -------
        Series
            output of the binary relational operation

        Raises
        ------
        TypeError
            if the left value type of the operator is not consistent with the right value type
        """
        if self._is_a_series_of_var_type(other, self.var_type) or (
            self.pytype_dbtype_map.get(type(other)) == self.var_type
        ):
            return self._binary_op(other=other, node_type=node_type, output_var_type=DBVarType.BOOL)

        raise TypeError(f"Not supported operation '{node_type}' between '{self}' and '{other}'!")

    def __eq__(self, other: int | float | str | bool | Series) -> Series:  # type: ignore
        return self._binary_relational_op(other, NodeType.EQ)

    def __ne__(self, other: int | float | str | bool | Series) -> Series:  # type: ignore
        return self._binary_relational_op(other, NodeType.NE)

    def __lt__(self, other: int | float | str | bool | Series) -> Series:
        return self._binary_relational_op(other, NodeType.LT)

    def __le__(self, other: int | float | str | bool | Series) -> Series:
        return self._binary_relational_op(other, NodeType.LE)

    def __gt__(self, other: int | float | str | bool | Series) -> Series:
        return self._binary_relational_op(other, NodeType.GT)

    def __ge__(self, other: int | float | str | bool | Series) -> Series:
        return self._binary_relational_op(other, NodeType.GE)

    def _binary_arithmetic_op(
        self, other: int | float | Series, node_type: NodeType, right_op: bool = False
    ) -> Series:
        """
        Apply binary arithmetic operation between self & other objects

        Parameters
        ----------
        other: int | float | Series
            right value of the binary arithmetic operator
        node_type: NodeType
            binary arithmetic operator node type
        right_op: bool
            whether the binary operation is from right object or not

        Returns
        -------
        Series
            output of the binary arithmetic operation

        Raises
        ------
        TypeError
            if the arithmetic operation between left value and right value is not supported
        """
        supported_types = {DBVarType.INT, DBVarType.FLOAT}
        if self.var_type not in supported_types:
            raise TypeError(f"{self} does not support operation '{node_type}'.")
        if (isinstance(other, Series) and other.var_type in supported_types) or isinstance(
            other, (int, float)
        ):
            output_var_type = DBVarType.FLOAT
            if (
                self.var_type == DBVarType.INT
                and (isinstance(other, int) or self._is_a_series_of_var_type(other, DBVarType.INT))
                and node_type not in {NodeType.DIV}
            ):
                output_var_type = DBVarType.INT
            return self._binary_op(
                other=other,
                node_type=node_type,
                output_var_type=output_var_type,
                right_op=right_op,
            )

        raise TypeError(f"Not supported operation '{node_type}' between '{self}' and '{other}'!")

    def __add__(self, other: int | float | Series) -> Series:
        return self._binary_arithmetic_op(other, NodeType.ADD)

    def __radd__(self, other: int | float | Series) -> Series:
        return self._binary_arithmetic_op(other, NodeType.ADD, right_op=True)

    def __sub__(self, other: int | float | Series) -> Series:
        return self._binary_arithmetic_op(other, NodeType.SUB)

    def __rsub__(self, other: int | float | Series) -> Series:
        return self._binary_arithmetic_op(other, NodeType.SUB, right_op=True)

    def __mul__(self, other: int | float | Series) -> Series:
        return self._binary_arithmetic_op(other, NodeType.MUL)

    def __rmul__(self, other: int | float | Series) -> Series:
        return self._binary_arithmetic_op(other, NodeType.MUL, right_op=True)

    def __truediv__(self, other: int | float | Series) -> Series:
        return self._binary_arithmetic_op(other, NodeType.DIV)

    def __rtruediv__(self, other: int | float | Series) -> Series:
        return self._binary_arithmetic_op(other, NodeType.DIV, right_op=True)

    def preview_sql(self, limit: int = 10) -> str:
        """
        Generate SQL query to preview the transformed column

        Parameters
        ----------
        limit: int
            maximum number of return rows

        Returns
        -------
        pd.DataFrame | None
        """
        columns = []
        if self.name:
            columns.append(self.name)
        return self._preview_sql(columns=columns, limit=limit)

    def as_entity(self, tag_name: str) -> None:
        """
        Set the series as entity with tag name

        Parameters
        ----------
        tag_name: str
            Tag name of the entity

        Raises
        ------
        ValueError
        """
        if self.name is None:
            raise ValueError("Series object does not have name!")
        if self._parent_frame is None:
            raise ValueError("Series object does not have parent frame object!")
        self._parent_frame.column_entity_map[self.name] = tag_name
