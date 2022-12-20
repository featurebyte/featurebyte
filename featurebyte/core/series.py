"""
Series class
"""
from __future__ import annotations

from typing import Any, Callable, Literal, Optional, Type, TypeVar, Union

from functools import wraps

import pandas as pd
from pydantic import Field, StrictStr, root_validator
from typeguard import typechecked

from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.common.typing import is_scalar_nan
from featurebyte.core.accessor.datetime import DtAccessorMixin
from featurebyte.core.accessor.string import StrAccessorMixin
from featurebyte.core.generic import QueryObject
from featurebyte.core.mixin import OpsMixin, ParentMixin
from featurebyte.core.util import series_binary_operation, series_unary_operation
from featurebyte.enum import DBVarType
from featurebyte.query_graph.algorithm import dfs_traversal
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.graph import GlobalQueryGraph, QueryGraph

FuncT = TypeVar("FuncT", bound=Callable[..., "Series"])


def numeric_only(func: FuncT) -> FuncT:
    """
    Decorator for methods that can only be applied to numeric Series

    Parameters
    ----------
    func : FuncT
        Method to decorate

    Returns
    -------
    callable
    """

    @wraps(func)
    def wrapped(self: Series, *args: Any, **kwargs: Any) -> Series:
        op_name = func.__name__
        if not self.is_numeric:
            raise TypeError(f"{op_name} is only available to numeric Series; got {self.dtype}")
        return func(self, *args, **kwargs)

    return wrapped  # type: ignore


class Series(QueryObject, OpsMixin, ParentMixin, StrAccessorMixin, DtAccessorMixin):
    """
    Implement operations to manipulate database column
    """

    # documentation metadata
    __fbautodoc__ = FBAutoDoc(
        section=["Series"],
        proxy_class="featurebyte.Series",
    )

    name: Optional[StrictStr] = Field(default=None)
    dtype: DBVarType = Field(allow_mutation=False)

    def __repr__(self) -> str:
        return f"{type(self).__name__}[{self.dtype}](name={self.name}, node_name={self.node_name})"

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
            node_name=node.name,
            name=self.name,
            dtype=self.dtype,
            **self.unary_op_series_params(),
        )

    def binary_op_series_params(self, other: Series | None = None) -> dict[str, Any]:
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
            DBVarType.INT: (int, float),
            DBVarType.FLOAT: (int, float),
            DBVarType.CHAR: (),
            DBVarType.VARCHAR: (str,),
            DBVarType.DATE: (),
        }
        return isinstance(right_value, valid_assignment_map[left_dbtype])

    @typechecked
    def __setitem__(self, key: Series, value: Union[int, float, str, bool, None]) -> None:
        if self.row_index_lineage != key.row_index_lineage:
            raise ValueError(f"Row indices between '{self}' and '{key}' are not aligned!")
        if key.dtype != DBVarType.BOOL:
            raise TypeError("Only boolean Series filtering is supported!")
        if not self._is_assignment_valid(self.dtype, value) and not is_scalar_nan(value):
            raise ValueError(
                f"Conditionally updating '{self}' with value '{value}' is not supported!"
            )

        node = self.graph.add_operation(
            node_type=NodeType.CONDITIONAL,
            node_params={"value": value},
            node_output_type=NodeOutputType.SERIES,
            input_nodes=[self.node, key.node],
        )
        self.node_name = node.name
        if isinstance(value, float) and self.dtype != DBVarType.FLOAT:
            # convert dtype to float if the assign value is float
            self.__dict__["dtype"] = DBVarType.FLOAT

        # For Series with a parent, apply the change to the parent (either an EventView or a
        # FeatureGroup)
        if self.parent is not None:
            # Update the EventView column / Feature by doing an assign operation
            self.parent[self.name] = self
            # Update the current node as a PROJECT / ALIAS from the parent. This is to allow
            # readable column name during series preview
            self.node_name = self.parent[self.name].node_name

    @staticmethod
    def _is_a_series_of_var_type(
        item: Any, var_type: Union[DBVarType, tuple[DBVarType, ...]]
    ) -> bool:
        """
        Check whether the input item is Series type and has any of the specified variable types

        Parameters
        ----------
        item: Any
            input item
        var_type: DBVarType | tuple[DBVarType, ...]
            specified database variable type(s)

        Returns
        -------
        bool
            whether the input item is a Series with the specified database variable type
        """
        if isinstance(var_type, tuple):
            var_type_lst = list(var_type)
        else:
            var_type_lst = [var_type]
        return isinstance(item, Series) and any(item.dtype == var_type for var_type in var_type_lst)

    def _binary_op(
        self,
        other: int | float | str | bool | Series,
        node_type: NodeType,
        output_var_type: DBVarType,
        right_op: bool = False,
        additional_node_params: dict[str, Any] | None = None,
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
        additional_node_params : dict[str, Any] | None
            additional parameters to include as node parameters

        Returns
        -------
        Series
            output of the binary operation

        Raises
        ------
        TypeError
            If the other series has incompatible type
        """
        if isinstance(other, Series):
            binary_op_series_params = self.binary_op_series_params(other)
        else:
            binary_op_series_params = self.binary_op_series_params()
        if isinstance(other, Series) and self.__class__ != other.__class__:
            # Checking strict equality of types when both sides are Series is intentional. It is to
            # handle cases such as when self is EventViewColumn and other is Feature - they are both
            # Series but such operations are not allowed.
            raise TypeError(
                f"Operation between {self.__class__.__name__} and {other.__class__.__name__} is not"
                " supported"
            )
        return series_binary_operation(
            input_series=self,
            other=other,
            node_type=node_type,
            output_var_type=output_var_type,
            right_op=right_op,
            additional_node_params=additional_node_params,
            **binary_op_series_params,
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
        if self.dtype != DBVarType.BOOL or (
            isinstance(other, Series) and other.dtype != DBVarType.BOOL
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
        # some types can be compared directly
        supported_var_types_map = {
            DBVarType.INT: (DBVarType.INT, DBVarType.FLOAT),
            DBVarType.FLOAT: (DBVarType.INT, DBVarType.FLOAT),
        }
        supported_var_types = supported_var_types_map.get(self.dtype, (self.dtype,))
        if not self._is_a_series_of_var_type(other, supported_var_types) and (
            self.pytype_dbtype_map.get(type(other)) not in supported_var_types
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
        if self.dtype not in supported_types:
            raise TypeError(f"{self} does not support operation '{node_type}'.")
        if (isinstance(other, Series) and other.dtype in supported_types) or isinstance(
            other, (int, float)
        ):
            output_var_type = DBVarType.FLOAT
            if (
                self.dtype == DBVarType.INT
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
    def _date_diff_op(self, other: Series, right_op: bool = False) -> Series:
        """
        Apply date difference operation between two date Series

        Parameters
        ----------
        other : Series
            right value of the operation
        right_op: bool
            whether the binary operation is from right object or not

        Returns
        -------
        Series
            output of the date difference operation
        """
        return self._binary_op(
            other=other,
            node_type=NodeType.DATE_DIFF,
            output_var_type=DBVarType.TIMEDELTA,
            right_op=right_op,
            additional_node_params={},
        )

    @typechecked
    def _date_add_op(self, other: Union[Series, pd.Timedelta], right_op: bool = False) -> Series:
        """
        Increment date by timedelta

        Parameters
        ----------
        other : Series
            right value of the operation
        right_op: bool
            whether the binary operation is from right object or not

        Returns
        -------
        Series
            output of the date difference operation
        """
        if isinstance(other, pd.Timedelta):
            other = other.total_seconds()
        return self._binary_op(
            other=other,
            node_type=NodeType.DATE_ADD,
            output_var_type=DBVarType.TIMESTAMP,
            right_op=right_op,
            additional_node_params={},
        )

    @typechecked
    def __add__(self, other: Union[int, float, str, pd.Timedelta, Series]) -> Series:
        is_other_string_like = isinstance(other, str)
        is_other_string_like |= isinstance(other, Series) and other.dtype in DBVarType.VARCHAR
        if self.dtype == DBVarType.VARCHAR and is_other_string_like:
            return self._binary_op(
                other=other, node_type=NodeType.CONCAT, output_var_type=DBVarType.VARCHAR
            )
        if self.is_datetime and (
            self._is_a_series_of_var_type(other, DBVarType.TIMEDELTA)
            or isinstance(other, pd.Timedelta)
        ):
            return self._date_add_op(other=other)
        return self._binary_arithmetic_op(other, NodeType.ADD)

    @typechecked
    def __radd__(self, other: Union[int, float, str, pd.Timedelta, Series]) -> Series:
        is_other_string_like = isinstance(other, str)
        is_other_string_like |= isinstance(other, Series) and other.dtype in DBVarType.VARCHAR
        if self.dtype == DBVarType.VARCHAR and is_other_string_like:
            return self._binary_op(
                other=other,
                node_type=NodeType.CONCAT,
                output_var_type=DBVarType.VARCHAR,
                right_op=True,
            )
        if self.is_datetime or isinstance(other, pd.Timedelta):
            # Intentionally not set right_op=True because it is irrelevant for date add operation.
            # SQL generation makes the assumption that "other" is always on the right side.
            return self._date_add_op(other=other)
        return self._binary_arithmetic_op(other, NodeType.ADD, right_op=True)

    @typechecked
    def __sub__(self, other: Union[int, float, Series]) -> Series:
        if self.is_datetime and isinstance(other, Series) and other.is_datetime:
            return self._date_diff_op(other)
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

    @typechecked
    def __mod__(self, other: Union[int, float, Series]) -> Series:
        return self._binary_arithmetic_op(other, NodeType.MOD)

    @typechecked
    def __rmod__(self, other: Union[int, float, Series]) -> Series:
        return self._binary_arithmetic_op(other, NodeType.MOD, right_op=True)

    def __invert__(self) -> Series:
        return series_unary_operation(
            input_series=self,
            node_type=NodeType.NOT,
            output_var_type=DBVarType.BOOL,
            node_params={},
            **self.unary_op_series_params(),
        )

    @typechecked
    def __pow__(self, other: Union[int, float, Series]) -> Series:
        return self.pow(other)

    @property
    def is_datetime(self) -> bool:
        """
        Returns whether Series has a datetime like variable type

        Returns
        -------
        bool
        """
        return self.dtype in (DBVarType.TIMESTAMP, DBVarType.DATE)

    @property
    def is_numeric(self) -> bool:
        """
        Returns whether Series has a numeric variable type

        Returns
        -------
        bool
        """
        return self.dtype in (DBVarType.INT, DBVarType.FLOAT)

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

    @typechecked
    def astype(
        self, new_type: Union[Type[int], Type[float], Type[str], Literal["int", "float", "str"]]
    ) -> Series:
        """
        Convert Series to have a new type

        Parameters
        ----------
        new_type : Union[Type[int], Type[float], Type[str], Literal["int", "float", "str"]])
            Desired type after conversion. Type can be provided directly or as a string

        Returns
        -------
        Series
            A new Series with converted variable type

        Raises
        ------
        TypeError
            if the Series dtype does not support type conversion
        """
        supported_source_dtype = {DBVarType.BOOL, DBVarType.INT, DBVarType.FLOAT, DBVarType.VARCHAR}
        if self.dtype not in supported_source_dtype:
            raise TypeError(f"astype not supported for {self.dtype}")

        known_str_to_type = {
            "int": int,
            "float": float,
            "str": str,
        }
        if isinstance(new_type, str):
            # new_type is typechecked and must be a valid key
            new_type = known_str_to_type[new_type]

        if new_type is int:
            type_name = "int"
            output_var_type = DBVarType.INT
        elif new_type is float:
            type_name = "float"
            output_var_type = DBVarType.FLOAT
        else:
            type_name = "str"
            output_var_type = DBVarType.VARCHAR

        node_params = {"type": type_name, "from_dtype": self.dtype}
        return series_unary_operation(
            input_series=self,
            node_type=NodeType.CAST,
            output_var_type=output_var_type,
            node_params=node_params,
            **self.unary_op_series_params(),
        )

    @numeric_only
    def abs(self) -> Series:
        """
        Computes the absolute value of the current Series

        Returns
        -------
        Series
        """
        return series_unary_operation(
            input_series=self,
            node_type=NodeType.ABS,
            output_var_type=self.dtype,
            node_params={},
            **self.unary_op_series_params(),
        )

    @numeric_only
    def sqrt(self: Series) -> Series:
        """
        Computes the square root of the current Series

        Returns
        -------
        Series
        """
        return series_unary_operation(
            input_series=self,
            node_type=NodeType.SQRT,
            output_var_type=DBVarType.FLOAT,
            node_params={},
            **self.unary_op_series_params(),
        )

    @numeric_only
    def pow(self: Series, other: int | float | Series) -> Series:
        """
        Computes the exponential power of the current Series

        Parameters
        ----------
        other : int | float | Series
            Power to raise to

        Returns
        -------
        Series
        """
        return self._binary_op(
            other=other,
            node_type=NodeType.POWER,
            output_var_type=DBVarType.FLOAT,
        )

    @numeric_only
    def log(self) -> Series:
        """
        Compute the natural logarithm of the Series

        Returns
        -------
        Series
        """
        return series_unary_operation(
            input_series=self,
            node_type=NodeType.LOG,
            output_var_type=DBVarType.FLOAT,
            node_params={},
            **self.unary_op_series_params(),
        )

    @numeric_only
    def exp(self) -> Series:
        """
        Compute the exponential of the Series

        Returns
        -------
        Series
        """
        return series_unary_operation(
            input_series=self,
            node_type=NodeType.EXP,
            output_var_type=DBVarType.FLOAT,
            node_params={},
            **self.unary_op_series_params(),
        )

    @numeric_only
    def floor(self: Series) -> Series:
        """
        Round the Series to the nearest equal or smaller integer

        Returns
        -------
        Series
        """
        return series_unary_operation(
            input_series=self,
            node_type=NodeType.FLOOR,
            output_var_type=DBVarType.INT,
            node_params={},
            **self.unary_op_series_params(),
        )

    @numeric_only
    def ceil(self: Series) -> Series:
        """
        Round the Series to the nearest equal or larger integer

        Returns
        -------
        Series
        """
        return series_unary_operation(
            input_series=self,
            node_type=NodeType.CEIL,
            output_var_type=DBVarType.INT,
            node_params={},
            **self.unary_op_series_params(),
        )

    @property
    def node_types_lineage(self) -> list[NodeType]:
        """
        Returns a list of node types that is part of the lineage of this Series

        Returns
        -------
        list[NodeType]
        """
        out = []
        series_dict = self.dict()
        pruned_graph = QueryGraph(**series_dict["graph"])
        pruned_node = pruned_graph.get_node_by_name(series_dict["node_name"])
        for node in dfs_traversal(pruned_graph, pruned_node):
            out.append(node.type)
        return out

    @root_validator
    @classmethod
    def _convert_query_graph_to_global_query_graph(cls, values: dict[str, Any]) -> dict[str, Any]:
        if not isinstance(values["graph"], GlobalQueryGraph):
            global_graph, node_name_map = GlobalQueryGraph().load(values["graph"])
            values["graph"] = global_graph
            values["node_name"] = node_name_map[values["node_name"]]
        return values

    def dict(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        if isinstance(self.graph, GlobalQueryGraph):
            pruned_graph, node_name_map = self.graph.prune(target_node=self.node)
            mapped_node = pruned_graph.get_node_by_name(node_name_map[self.node.name])
            new_object = self.copy()
            new_object.node_name = mapped_node.name
            # Use the __dict__ assignment method to skip pydantic validation check. Otherwise, it will trigger
            # `_convert_query_graph_to_global_query_graph` validation check and convert the pruned graph into
            # global one.
            new_object.__dict__["graph"] = pruned_graph
            return new_object.dict(*args, **kwargs)
        return super().dict(*args, **kwargs)

    def copy(self, *args: Any, **kwargs: Any) -> Series:
        # Copying a Series should prevent it from modifying the parent Frame
        kwargs.pop("deep", None)
        return super().copy(*args, **kwargs, deep=True)
