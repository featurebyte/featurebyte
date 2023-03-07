"""
Series class
"""
from __future__ import annotations

from typing import Any, Callable, Literal, Optional, Sequence, Type, TypeVar, Union

from functools import wraps

import pandas as pd
from pydantic import Field, StrictStr
from typeguard import typechecked

from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.common.typing import Scalar, ScalarSequence, is_scalar_nan
from featurebyte.core.accessor.datetime import DtAccessorMixin
from featurebyte.core.accessor.string import StrAccessorMixin
from featurebyte.core.generic import QueryObject
from featurebyte.core.mixin import OpsMixin, ParentMixin
from featurebyte.core.util import SeriesBinaryOperator, series_unary_operation
from featurebyte.enum import DBVarType
from featurebyte.query_graph.enum import NodeOutputType, NodeType

FrozenSeriesT = TypeVar("FrozenSeriesT", bound="FrozenSeries")
FuncT = TypeVar("FuncT", bound=Callable[..., "FrozenSeriesT"])


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
    def wrapped(self: FrozenSeriesT, *args: Any, **kwargs: Any) -> FrozenSeriesT:
        op_name = func.__name__
        if not self.is_numeric:
            raise TypeError(f"{op_name} is only available to numeric Series; got {self.dtype}")
        return func(self, *args, **kwargs)

    return wrapped  # type: ignore


class DefaultSeriesBinaryOperator(SeriesBinaryOperator):
    """
    Default series binary operator
    """

    def validate_inputs(self) -> None:
        """
        Validate the input series, and other parameter

        Raises
        ------
        TypeError
            If the other series has incompatible type
        """
        if isinstance(self.other, Series) and self.input_series.__class__ != self.other.__class__:
            # Checking strict equality of types when both sides are Series is intentional. It is to
            # handle cases such as when self is EventViewColumn and other is Feature - they are both
            # Series but such operations are not allowed.
            raise TypeError(
                f"Operation between {self.input_series.__class__.__name__} and {self.other.__class__.__name__} is not"
                " supported"
            )


class FrozenSeries(QueryObject, OpsMixin, ParentMixin, StrAccessorMixin, DtAccessorMixin):
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
    def __getitem__(self: FrozenSeriesT, item: FrozenSeriesT) -> FrozenSeriesT:
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

    def binary_op_series_params(
        self: FrozenSeriesT, other: Scalar | FrozenSeriesT | ScalarSequence
    ) -> dict[str, Any]:
        """
        Parameters that will be passed to series-like constructor in _binary_op method


        Parameters
        ----------
        other: Scalar | FrozenSeriesT | ScalarSequence
            Other object

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

    def _assert_assignment_valid(self, value: Any) -> None:
        """
        Assert that the given python builtin type can be assigned based on self's database type.

        Parameters
        ----------
        value: Any
            value to be assigned to self during conditional assignment

        Raises
        ------
        ValueError
            When the assignment is not valid
        """
        # Always fine to update value as missing
        if is_scalar_nan(value):
            return

        # Otherwise, the validity is based on DBVarType
        valid_assignment_map: dict[DBVarType, tuple[type[Any], ...]] = {
            DBVarType.BOOL: (bool,),
            DBVarType.INT: (int, float),
            DBVarType.FLOAT: (int, float),
            DBVarType.VARCHAR: (str,),
        }
        accepted_types = valid_assignment_map.get(self.dtype)
        if accepted_types is None:
            raise ValueError(
                f"Conditionally updating '{self}' of type {self.dtype} is not supported!"
            )
        if isinstance(value, Series):
            type_of_series = valid_assignment_map.get(value.dtype)
            if type_of_series != accepted_types:
                raise ValueError(
                    f"Conditionally updating '{self}' with type '{type(value).__name__}' is not allowed."
                )

        elif not isinstance(value, accepted_types):
            raise ValueError(
                f"Conditionally updating '{self}' with value '{value}' is not supported!"
            )

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
        self: FrozenSeriesT,
        other: Scalar | FrozenSeriesT | ScalarSequence,
        node_type: NodeType,
        output_var_type: DBVarType,
        right_op: bool = False,
        additional_node_params: dict[str, Any] | None = None,
    ) -> FrozenSeriesT:
        """
        Apply binary operation between self & other objects

        Parameters
        ----------
        other: Scalar | FrozenSeriesT | ScalarSequence
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
        FrozenSeriesT
            output of the binary operation
        """
        series_operator = DefaultSeriesBinaryOperator(self, other)
        return series_operator.operate(
            node_type=node_type,
            output_var_type=output_var_type,
            right_op=right_op,
            additional_node_params=additional_node_params,
        )

    def _binary_logical_op(
        self: FrozenSeriesT, other: bool | FrozenSeriesT, node_type: NodeType
    ) -> FrozenSeriesT:
        """
        Apply binary logical operation between self & other objects

        Parameters
        ----------
        other: bool | FrozenSeriesT
            right value of the binary logical operator
        node_type: NodeType
            binary logical operator node type

        Returns
        -------
        FrozenSeriesT
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
    def __and__(self: FrozenSeriesT, other: Union[bool, FrozenSeriesT]) -> FrozenSeriesT:
        return self._binary_logical_op(other, NodeType.AND)

    @typechecked
    def __or__(self: FrozenSeriesT, other: Union[bool, FrozenSeriesT]) -> FrozenSeriesT:
        return self._binary_logical_op(other, NodeType.OR)

    def _binary_relational_op(
        self: FrozenSeriesT, other: int | float | str | bool | FrozenSeriesT, node_type: NodeType
    ) -> FrozenSeriesT:
        """
        Apply binary relational operation between self & other objects

        Parameters
        ----------
        other: int | float | str | bool | FrozenSeriesT
            right value of the binary relational operator
        node_type: NodeType
            binary relational operator node type

        Returns
        -------
        FrozenSeriesT
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
    def __eq__(self, other: Union[int, float, str, bool, FrozenSeries]) -> FrozenSeries:  # type: ignore
        return self._binary_relational_op(other, NodeType.EQ)

    @typechecked
    def __ne__(self, other: Union[int, float, str, bool, FrozenSeries]) -> FrozenSeries:  # type: ignore
        return self._binary_relational_op(other, NodeType.NE)

    @typechecked
    def __lt__(self, other: Union[int, float, str, bool, FrozenSeries]) -> FrozenSeries:  # type: ignore
        return self._binary_relational_op(other, NodeType.LT)

    @typechecked
    def __le__(self, other: Union[int, float, str, bool, FrozenSeries]) -> FrozenSeries:  # type: ignore
        return self._binary_relational_op(other, NodeType.LE)

    @typechecked
    def __gt__(self, other: Union[int, float, str, bool, FrozenSeries]) -> FrozenSeries:
        return self._binary_relational_op(other, NodeType.GT)

    @typechecked
    def __ge__(self, other: Union[int, float, str, bool, FrozenSeries]) -> FrozenSeries:
        return self._binary_relational_op(other, NodeType.GE)

    def _binary_arithmetic_op(
        self,
        other: int | float | str | FrozenSeries,
        node_type: NodeType,
        right_op: bool = False,
    ) -> FrozenSeries:
        """
        Apply binary arithmetic operation between self & other objects

        Parameters
        ----------
        other: int | float | str | FrozenSeries
            right value of the binary arithmetic operator
        node_type: NodeType
            binary arithmetic operator node type
        right_op: bool
            whether the binary operation is from right object or not

        Returns
        -------
        FrozenSeries
            output of the binary arithmetic operation

        Raises
        ------
        TypeError
            if the arithmetic operation between left value and right value is not supported
        """
        supported_types = {DBVarType.INT, DBVarType.FLOAT}
        if self.dtype not in supported_types:
            raise TypeError(f"{self} does not support operation '{node_type}'.")
        if (isinstance(other, FrozenSeries) and other.dtype in supported_types) or isinstance(
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
    def _date_diff_op(self, other: FrozenSeries, right_op: bool = False) -> FrozenSeries:
        """
        Apply date difference operation between two date Series

        Parameters
        ----------
        other : FrozenSeries
            right value of the operation
        right_op: bool
            whether the binary operation is from right object or not

        Returns
        -------
        FrozenSeries
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
    def _date_add_op(
        self, other: Union[FrozenSeries, pd.Timedelta], right_op: bool = False
    ) -> FrozenSeries:
        """
        Increment date by timedelta

        Parameters
        ----------
        other : FrozenSeries
            right value of the operation
        right_op: bool
            whether the binary operation is from right object or not

        Returns
        -------
        FrozenSeries
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
    def __add__(self, other: Union[int, float, str, pd.Timedelta, FrozenSeries]) -> FrozenSeries:
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
    def __radd__(self, other: Union[int, float, str, pd.Timedelta, FrozenSeries]) -> FrozenSeries:
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
    def __sub__(self, other: Union[int, float, FrozenSeries]) -> FrozenSeries:
        if self.is_datetime and isinstance(other, FrozenSeries) and other.is_datetime:
            return self._date_diff_op(other)
        return self._binary_arithmetic_op(other, NodeType.SUB)

    @typechecked
    def __rsub__(self, other: Union[int, float, FrozenSeries]) -> FrozenSeries:
        return self._binary_arithmetic_op(other, NodeType.SUB, right_op=True)

    @typechecked
    def __mul__(self, other: Union[int, float, FrozenSeries]) -> FrozenSeries:
        return self._binary_arithmetic_op(other, NodeType.MUL)

    @typechecked
    def __rmul__(self, other: Union[int, float, FrozenSeries]) -> FrozenSeries:
        return self._binary_arithmetic_op(other, NodeType.MUL, right_op=True)

    @typechecked
    def __truediv__(self, other: Union[int, float, FrozenSeries]) -> FrozenSeries:
        return self._binary_arithmetic_op(other, NodeType.DIV)

    @typechecked
    def __rtruediv__(self, other: Union[int, float, FrozenSeries]) -> FrozenSeries:
        return self._binary_arithmetic_op(other, NodeType.DIV, right_op=True)

    @typechecked
    def __mod__(self, other: Union[int, float, FrozenSeries]) -> FrozenSeries:
        return self._binary_arithmetic_op(other, NodeType.MOD)

    @typechecked
    def __rmod__(self, other: Union[int, float, FrozenSeries]) -> FrozenSeries:
        return self._binary_arithmetic_op(other, NodeType.MOD, right_op=True)

    def __invert__(self: FrozenSeriesT) -> FrozenSeriesT:
        return series_unary_operation(
            input_series=self,
            node_type=NodeType.NOT,
            output_var_type=DBVarType.BOOL,
            node_params={},
            **self.unary_op_series_params(),
        )

    @typechecked
    def __pow__(self, other: Union[int, float, FrozenSeries]) -> FrozenSeries:
        return self.pow(other)

    @property
    def is_datetime(self) -> bool:
        """
        Returns whether Series has a datetime like variable type

        Returns
        -------
        bool
        """
        return self.dtype in (DBVarType.TIMESTAMP, DBVarType.TIMESTAMP_TZ, DBVarType.DATE)

    @property
    def is_numeric(self) -> bool:
        """
        Returns whether Series has a numeric variable type

        Returns
        -------
        bool
        """
        return self.dtype in (DBVarType.INT, DBVarType.FLOAT)

    def isnull(self: FrozenSeriesT) -> FrozenSeriesT:
        """
        Returns a boolean Series indicating whether each value is missing

        Returns
        -------
        FrozenSeriesT
        """
        return series_unary_operation(
            input_series=self,
            node_type=NodeType.IS_NULL,
            output_var_type=DBVarType.BOOL,
            node_params={},
            **self.unary_op_series_params(),
        )

    def notnull(self: FrozenSeriesT) -> FrozenSeriesT:
        """
        Returns a boolean Series indicating whether each value is not null

        Returns
        -------
        FrozenSeriesT
        """
        return ~self.isnull()

    @typechecked
    def astype(
        self,
        new_type: Union[Type[int], Type[float], Type[str], Literal["int", "float", "str"]],
    ) -> FrozenSeries:
        """
        Convert Series to have a new type

        Parameters
        ----------
        new_type : Union[Type[int], Type[float], Type[str], Literal["int", "float", "str"]])
            Desired type after conversion. Type can be provided directly or as a string

        Returns
        -------
        FrozenSeries
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
            output_var_type = DBVarType.INT
        elif new_type is float:
            output_var_type = DBVarType.FLOAT
        else:
            output_var_type = DBVarType.VARCHAR

        node_params = {"type": output_var_type.to_type_str(), "from_dtype": self.dtype}
        return series_unary_operation(
            input_series=self,
            node_type=NodeType.CAST,
            output_var_type=output_var_type,
            node_params=node_params,
            **self.unary_op_series_params(),
        )

    @numeric_only
    def abs(self: FrozenSeriesT) -> FrozenSeriesT:
        """
        Computes the absolute value of the current Series

        Returns
        -------
        FrozenSeriesT
        """
        return series_unary_operation(
            input_series=self,
            node_type=NodeType.ABS,
            output_var_type=self.dtype,
            node_params={},
            **self.unary_op_series_params(),
        )

    @numeric_only
    def sqrt(self: FrozenSeriesT) -> FrozenSeriesT:
        """
        Computes the square root of the current Series

        Returns
        -------
        FrozenSeriesT
        """
        return series_unary_operation(
            input_series=self,
            node_type=NodeType.SQRT,
            output_var_type=DBVarType.FLOAT,
            node_params={},
            **self.unary_op_series_params(),
        )

    @numeric_only
    def pow(self: FrozenSeriesT, other: int | float | FrozenSeriesT) -> FrozenSeriesT:
        """
        Computes the exponential power of the current Series

        Parameters
        ----------
        other : int | float | Series
            Power to raise to

        Returns
        -------
        FrozenSeriesT
        """
        return self._binary_op(
            other=other,
            node_type=NodeType.POWER,
            output_var_type=DBVarType.FLOAT,
        )

    @numeric_only
    def log(self: FrozenSeriesT) -> FrozenSeriesT:
        """
        Compute the natural logarithm of the Series

        Returns
        -------
        FrozenSeriesT
        """
        return series_unary_operation(
            input_series=self,
            node_type=NodeType.LOG,
            output_var_type=DBVarType.FLOAT,
            node_params={},
            **self.unary_op_series_params(),
        )

    @numeric_only
    def exp(self: FrozenSeriesT) -> FrozenSeriesT:
        """
        Compute the exponential of the Series

        Returns
        -------
        FrozenSeriesT
        """
        return series_unary_operation(
            input_series=self,
            node_type=NodeType.EXP,
            output_var_type=DBVarType.FLOAT,
            node_params={},
            **self.unary_op_series_params(),
        )

    @numeric_only
    def floor(self: FrozenSeriesT) -> FrozenSeriesT:
        """
        Round the Series to the nearest equal or smaller integer

        Returns
        -------
        FrozenSeriesT
        """
        return series_unary_operation(
            input_series=self,
            node_type=NodeType.FLOOR,
            output_var_type=DBVarType.INT,
            node_params={},
            **self.unary_op_series_params(),
        )

    @numeric_only
    def ceil(self: FrozenSeriesT) -> FrozenSeriesT:
        """
        Round the Series to the nearest equal or larger integer

        Returns
        -------
        FrozenSeriesT
        """
        return series_unary_operation(
            input_series=self,
            node_type=NodeType.CEIL,
            output_var_type=DBVarType.INT,
            node_params={},
            **self.unary_op_series_params(),
        )

    def copy(self: FrozenSeriesT, *args: Any, **kwargs: Any) -> FrozenSeriesT:
        # Copying a Series should prevent it from modifying the parent Frame
        kwargs.pop("deep", None)
        return super().copy(*args, **kwargs, deep=True)

    def validate_isin_operation(
        self: FrozenSeriesT, other: Union[FrozenSeriesT, Sequence[Union[bool, int, float, str]]]
    ) -> None:
        """
        Optional validation that can be added by subclasses if needed.

        Parameters
        ----------
        other: Union[FrozenSeriesT, Sequence[Union[bool, int, float, str]]]
            other input to check whether the current series is in
        """
        _ = other

    @typechecked
    def isin(
        self, other: Union[FrozenSeries, ScalarSequence], right_op: bool = False
    ) -> FrozenSeries:
        """
        Identify if values in a series is in another series, or a pre-defined sequence.

        Parameters
        ----------
        other: Union[FrozenSeries, ScalarSequence]
            other input to check whether the current series is in
        right_op: bool
            right op

        Returns
        -------
        FrozenSeries
            updated series

        Raises
        ------
        ValueError
            raised when the other input is not a dictionary series

        Examples
        --------
        Check to see if the feature values are of values [1, 2, 3]

        >>> lookup_feature.isin([1, 2, 3]) # doctest: +SKIP

        Check to see if a lookup feature values are the keys of a dictionary feature

        >>> lookup_feature.isin(dictionary_feature) # doctest: +SKIP

        Check to see if the values in a series are of values [True, False]

        >>> series.isin([True, False]) # doctest: +SKIP
        """
        self.validate_isin_operation(other)

        # convert to dictionary keys if the other input is a series.
        other_series = other
        if isinstance(other, FrozenSeries):
            if other.dtype != DBVarType.OBJECT:
                raise ValueError(
                    "we can only operate on other series if the other series is a dictionary series."
                )
            other_series = series_unary_operation(
                input_series=other,
                node_type=NodeType.DICTIONARY_KEYS,
                output_var_type=DBVarType.ARRAY,
                node_params={},
                **other.unary_op_series_params(),
            )

        # perform the is in check when the other series is an array
        additional_node_params = {}
        # we only need to assign value if we have been passed in a sequence.
        if not isinstance(other, FrozenSeries):
            additional_node_params["value"] = other

        return self._binary_op(
            other=other_series,
            node_type=NodeType.IS_IN,
            output_var_type=DBVarType.BOOL,
            right_op=right_op,
            additional_node_params=additional_node_params,
        )


class Series(FrozenSeries):
    """
    Series class
    """

    @typechecked
    def __setitem__(
        self, key: FrozenSeries, value: Union[int, float, str, bool, None, FrozenSeries]
    ) -> None:
        if self.row_index_lineage != key.row_index_lineage:
            raise ValueError(f"Row indices between '{self}' and '{key}' are not aligned!")
        if key.dtype != DBVarType.BOOL:
            raise TypeError("Only boolean Series filtering is supported!")

        self._assert_assignment_valid(value)
        node_params = {}
        input_nodes = [self.node, key.node]
        if isinstance(value, Series):
            input_nodes.append(value.node)
        else:
            node_params = {"value": value}

        node = self.graph.add_operation(
            node_type=NodeType.CONDITIONAL,
            node_params=node_params,
            node_output_type=NodeOutputType.SERIES,
            input_nodes=input_nodes,
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

    @typechecked
    def fillna(self, other: Scalar) -> None:
        """
        Replace missing values with the provided value in-place

        Parameters
        ----------
        other: Scalar
            Value to replace missing values
        """
        self[self.isnull()] = other
