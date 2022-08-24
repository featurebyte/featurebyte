"""
Series class
"""
from __future__ import annotations

from typing import Any, Optional, Type, Union

from pydantic import Field, StrictStr, root_validator
from typeguard import typechecked

from featurebyte.core.accessor.datetime import DtAccessorMixin
from featurebyte.core.accessor.string import StrAccessorMixin
from featurebyte.core.generic import QueryObject
from featurebyte.core.mixin import OpsMixin, ParentMixin
from featurebyte.core.util import series_unary_operation
from featurebyte.enum import DBVarType
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.graph import GlobalQueryGraph


class Series(QueryObject, OpsMixin, ParentMixin, StrAccessorMixin, DtAccessorMixin):
    """
    Implement operations to manipulate database column
    """

    name: Optional[StrictStr] = Field(default=None)
    var_type: DBVarType = Field(allow_mutation=False)

    def __repr__(self) -> str:
        return (
            f"{type(self).__name__}[{self.var_type}](name={self.name}, node.name={self.node.name})"
        )

    def __str__(self) -> str:
        return repr(self)

    @typechecked
    def __getitem__(self, item: Series) -> Series:
        node = self._add_filter_operation(
            item=self, mask=item, node_output_type=NodeOutputType.SERIES
        )
        return type(self)(
            feature_store=self.feature_store,
            tabular_source=self.tabular_source,
            node=node,
            name=self.name,
            var_type=self.var_type,
            row_index_lineage=self._append_to_lineage(self.row_index_lineage, node.name),
        )

    def _binary_op_series_params(self, other: Series | None = None) -> dict[str, Any]:
        """
        Parameters that will be passed to series-like constructor in _binary_op method


        Parameters
        ----------
        other: Series
            Other Series object

        Returns
        -------
        dict[str, Any]
        """
        _ = other
        return {}

    def unary_op_series_params(self) -> dict[str, Any]:
        """
        Additional parameters that will be passed to unary operation output constructor

        Returns
        -------
        dict[str, Any]
        """
        return {}

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
        valid_assignment_map: dict[DBVarType, tuple[type[Any], ...]] = {
            DBVarType.BOOL: (bool,),
            DBVarType.INT: (int,),
            DBVarType.FLOAT: (int, float),
            DBVarType.CHAR: (),
            DBVarType.VARCHAR: (str,),
            DBVarType.DATE: (),
        }
        return isinstance(right_value, valid_assignment_map[left_dbtype])

    @typechecked
    def __setitem__(self, key: Series, value: Union[int, float, str, bool]) -> None:
        if self.row_index_lineage != key.row_index_lineage:
            raise ValueError(f"Row indices between '{self}' and '{key}' are not aligned!")
        if key.var_type != DBVarType.BOOL:
            raise TypeError("Only boolean Series filtering is supported!")
        if not self._is_assignment_valid(self.var_type, value):
            raise ValueError(f"Setting key '{key}' with value '{value}' not supported!")

        self.node = self.graph.add_operation(
            node_type=NodeType.CONDITIONAL,
            node_params={"value": value},
            node_output_type=NodeOutputType.SERIES,
            input_nodes=[self.node, key.node],
        )

        # For Series with a parent, apply the change to the parent (either an EventView or a
        # FeatureGroup)
        if self.parent is not None:
            # Update the EventView column / Feature by doing an assign operation
            self.parent[self.name] = self
            # Update the current node as a PROJECT / ALIAS from the parent. This is to allow
            # readable column name during series preview
            self.node = self.parent[self.name].node

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
        if isinstance(other, Series):
            node = self.graph.add_operation(
                node_type=node_type,
                node_params={},
                node_output_type=NodeOutputType.SERIES,
                input_nodes=[self.node, other.node],
            )
            return type(self)(
                feature_store=self.feature_store,
                tabular_source=self.tabular_source,
                node=node,
                name=None,
                var_type=output_var_type,
                row_index_lineage=self.row_index_lineage,
                **self._binary_op_series_params(other),
            )
        node_params["value"] = other
        node = self.graph.add_operation(
            node_type=node_type,
            node_params=node_params,
            node_output_type=NodeOutputType.SERIES,
            input_nodes=[self.node],
        )
        return type(self)(
            feature_store=self.feature_store,
            tabular_source=self.tabular_source,
            node=node,
            name=None,
            var_type=output_var_type,
            row_index_lineage=self.row_index_lineage,
            **self._binary_op_series_params(),
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
        if self.var_type != DBVarType.BOOL or (
            isinstance(other, Series) and other.var_type != DBVarType.BOOL
        ):
            raise TypeError(
                f"Not supported operation '{node_type}' between '{self}' and '{other}'!"
            )
        return self._binary_op(other=other, node_type=node_type, output_var_type=DBVarType.BOOL)

    @typechecked
    def __and__(self, other: Union[bool, Series]) -> Series:
        return self._binary_logical_op(other, NodeType.AND)

    @typechecked
    def __or__(self, other: Union[bool, Series]) -> Series:
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
        if not self._is_a_series_of_var_type(other, self.var_type) and (
            self.pytype_dbtype_map.get(type(other)) != self.var_type
        ):
            raise TypeError(
                f"Not supported operation '{node_type}' between '{self}' and '{other}'!"
            )
        return self._binary_op(other=other, node_type=node_type, output_var_type=DBVarType.BOOL)

    @typechecked
    def __eq__(self, other: Union[int, float, str, bool, Series]) -> Series:  # type: ignore
        return self._binary_relational_op(other, NodeType.EQ)

    @typechecked
    def __ne__(self, other: Union[int, float, str, bool, Series]) -> Series:  # type: ignore
        return self._binary_relational_op(other, NodeType.NE)

    @typechecked
    def __lt__(self, other: Union[int, float, str, bool, Series]) -> Series:  # type: ignore
        return self._binary_relational_op(other, NodeType.LT)

    @typechecked
    def __le__(self, other: Union[int, float, str, bool, Series]) -> Series:  # type: ignore
        return self._binary_relational_op(other, NodeType.LE)

    @typechecked
    def __gt__(self, other: Union[int, float, str, bool, Series]) -> Series:
        return self._binary_relational_op(other, NodeType.GT)

    @typechecked
    def __ge__(self, other: Union[int, float, str, bool, Series]) -> Series:
        return self._binary_relational_op(other, NodeType.GE)

    def _binary_arithmetic_op(
        self, other: int | float | str | Series, node_type: NodeType, right_op: bool = False
    ) -> Series:
        """
        Apply binary arithmetic operation between self & other objects

        Parameters
        ----------
        other: int | float | str | Series
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

    @typechecked
    def __add__(self, other: Union[int, float, str, Series]) -> Series:
        is_other_string_like = isinstance(other, str)
        is_other_string_like |= isinstance(other, Series) and other.var_type in DBVarType.VARCHAR
        if self.var_type == DBVarType.VARCHAR and is_other_string_like:
            return self._binary_op(
                other=other, node_type=NodeType.CONCAT, output_var_type=DBVarType.VARCHAR
            )
        return self._binary_arithmetic_op(other, NodeType.ADD)

    @typechecked
    def __radd__(self, other: Union[int, float, str, Series]) -> Series:
        is_other_string_like = isinstance(other, str)
        is_other_string_like |= isinstance(other, Series) and other.var_type in DBVarType.VARCHAR
        if self.var_type == DBVarType.VARCHAR and is_other_string_like:
            return self._binary_op(
                other=other,
                node_type=NodeType.CONCAT,
                output_var_type=DBVarType.VARCHAR,
                right_op=True,
            )
        return self._binary_arithmetic_op(other, NodeType.ADD, right_op=True)

    @typechecked
    def __sub__(self, other: Union[int, float, Series]) -> Series:
        return self._binary_arithmetic_op(other, NodeType.SUB)

    @typechecked
    def __rsub__(self, other: Union[int, float, Series]) -> Series:
        return self._binary_arithmetic_op(other, NodeType.SUB, right_op=True)

    @typechecked
    def __mul__(self, other: Union[int, float, Series]) -> Series:
        return self._binary_arithmetic_op(other, NodeType.MUL)

    @typechecked
    def __rmul__(self, other: Union[int, float, Series]) -> Series:
        return self._binary_arithmetic_op(other, NodeType.MUL, right_op=True)

    @typechecked
    def __truediv__(self, other: Union[int, float, Series]) -> Series:
        return self._binary_arithmetic_op(other, NodeType.DIV)

    @typechecked
    def __rtruediv__(self, other: Union[int, float, Series]) -> Series:
        return self._binary_arithmetic_op(other, NodeType.DIV, right_op=True)

    def __invert__(self) -> Series:
        return series_unary_operation(
            input_series=self,
            node_type=NodeType.NOT,
            output_var_type=DBVarType.BOOL,
            node_params={},
            **self.unary_op_series_params(),
        )

    def isnull(self) -> Series:
        """
        Returns a boolean Series indicating whether each value is missing

        Returns
        -------
        Series
        """
        return series_unary_operation(
            input_series=self,
            node_type=NodeType.IS_NULL,
            output_var_type=DBVarType.BOOL,
            node_params={},
            **self.unary_op_series_params(),
        )

    def notnull(self) -> Series:
        """
        Returns a boolean Series indicating whether each value is not null

        Returns
        -------
        Series
        """
        return ~self.isnull()

    @typechecked
    def fillna(self, other: Union[int, float, str, bool]) -> None:
        """
        Replace missing values with the provided value in-place

        Parameters
        ----------
        other : Union[int, float, str, bool]
            Value to replace missing values
        """
        self[self.isnull()] = other

    def astype(self, new_type: Union[Type[int], Type[float], Type[str], str]) -> Series:
        """
        Convert Series to have a new type

        Parameters
        ----------
        new_type : Union[int, float, str, bool]
            Desired type after conversion
        """
        known_str_to_type = {
            "int": int,
            "float": float,
            "str": str,
        }
        if isinstance(new_type, str):
            if new_type in known_str_to_type:
                new_type = known_str_to_type[new_type]
            else:
                raise TypeError(f"Type conversion not supported for {new_type}")

        if new_type is int:
            type_name = "int"
            output_var_type = DBVarType.INT
        elif new_type is float:
            type_name = "float"
            output_var_type = DBVarType.FLOAT
        elif new_type is str:
            type_name = "str"
            output_var_type = DBVarType.VARCHAR
        else:
            raise TypeError(f"Type conversion not supported for {new_type}")

        node_params = {"type": type_name}
        return series_unary_operation(
            input_series=self,
            node_type=NodeType.CAST,
            output_var_type=output_var_type,
            node_params=node_params,
            **self.unary_op_series_params(),
        )

    @typechecked
    def preview_sql(self, limit: int = 10) -> str:
        """
        Generate SQL query to preview the transformed column

        Parameters
        ----------
        limit: int
            maximum number of return rows

        Returns
        -------
        SQL query
        """
        columns = []
        if self.name:
            columns.append(self.name)
        return self._preview_sql(columns=columns, limit=limit)

    @root_validator()
    @classmethod
    def _convert_query_graph_to_global_query_graph(cls, values: dict[str, Any]) -> dict[str, Any]:
        if not isinstance(values["graph"], GlobalQueryGraph):
            global_graph, node_name_map = GlobalQueryGraph().load(values["graph"])
            values["graph"] = global_graph
            values["node"] = global_graph.get_node_by_name(node_name_map[values["node"].name])
            values["row_index_lineage"] = tuple(
                node_name_map[node_name] for node_name in values["row_index_lineage"]
            )
        return values

    def dict(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        if isinstance(self.graph, GlobalQueryGraph):
            target_columns = set()
            if self.name:
                target_columns.add(self.name)
            pruned_graph, node_name_map = self.graph.prune(
                target_node=self.node, target_columns=target_columns
            )
            mapped_node = pruned_graph.get_node_by_name(node_name_map[self.node.name])
            new_object = self.copy()
            new_object.node = mapped_node
            new_object.row_index_lineage = tuple(
                node_name_map[node_name] for node_name in new_object.row_index_lineage
            )
            # Use the __dict__ assignment method to skip pydantic validation check. Otherwise, it will trigger
            # `_convert_query_graph_to_global_query_graph` validation check and convert the pruned graph into
            # global one.
            new_object.__dict__["graph"] = pruned_graph
            return new_object.dict(*args, **kwargs)
        return super().dict(*args, **kwargs)
