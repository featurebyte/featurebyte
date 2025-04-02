"""
Series class
"""

from __future__ import annotations

from functools import wraps
from typing import Any, Callable, ClassVar, Optional, Type, TypeVar, Union

import pandas as pd
from pydantic import Field, StrictStr
from typeguard import typechecked
from typing_extensions import Literal

from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.core.accessor.datetime import DtAccessorMixin
from featurebyte.core.accessor.string import StrAccessorMixin
from featurebyte.core.accessor.vector import VectorAccessorMixin
from featurebyte.core.generic import QueryObject
from featurebyte.core.mixin import OpsMixin, ParentMixin
from featurebyte.core.util import SeriesBinaryOperator, series_unary_operation
from featurebyte.enum import DBVarType
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.model.dtype import DBVarTypeInfo
from featurebyte.query_graph.model.timestamp_schema import TimeZoneColumn
from featurebyte.typing import Scalar, ScalarSequence, Timestamp, is_scalar_nan

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

    @wraps(func)  # type: ignore[arg-type]
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
        ValueError
            If the feature job settings of both series are inconsistent
        """
        if (
            isinstance(self.other, Series)
            and self.input_series.output_category != self.other.output_category
        ):
            # Checking strict equality of types when both sides are Series is intentional. It is to
            # handle cases such as when self is EventViewColumn and other is Feature - they are both
            # Series but such operations are not allowed.
            raise TypeError(
                f"Operation between {self.input_series.__class__.__name__} and {self.other.__class__.__name__} is not"
                " supported"
            )

        if isinstance(self.other, Series):
            # Check that the feature job settings of both series are consistent
            table_id_feature_job_settings = (
                self.input_series.graph.extract_table_id_feature_job_settings(
                    target_node=self.input_series.node
                )
            )
            other_table_id_feature_job_settings = (
                self.other.graph.extract_table_id_feature_job_settings(target_node=self.other.node)
            )
            table_id_to_feature_job_settings = {
                table_id_feature_job_setting.table_id: table_id_feature_job_setting.feature_job_setting
                for table_id_feature_job_setting in table_id_feature_job_settings
            }
            for table_id_feature_job_setting in other_table_id_feature_job_settings:
                table_id = table_id_feature_job_setting.table_id
                other_feature_job_setting = table_id_feature_job_setting.feature_job_setting
                this_feature_job_setting = table_id_to_feature_job_settings.get(table_id)
                if (
                    this_feature_job_setting
                    and this_feature_job_setting != other_feature_job_setting
                ):
                    error_message = (
                        f"Feature job setting (table ID: {table_id}) of "
                        f"feature {self.input_series.name} ({this_feature_job_setting}) "
                        f"and feature {self.other.name} ({other_feature_job_setting}) are not consistent. "
                        "Binary feature operations are only supported when the feature job settings are consistent."
                    )
                    raise ValueError(error_message)


class FrozenSeries(
    QueryObject,
    OpsMixin,
    ParentMixin,
    StrAccessorMixin,
    DtAccessorMixin,
    VectorAccessorMixin,
):
    """
    FrozenSeries class used for representing a series in a query graph with the ability to perform
    certain column related operations and expressions. This class is immutable as it does not support
    in-place modification of the column in the query graph.
    """

    # class variables
    __fbautodoc__: ClassVar[FBAutoDoc] = FBAutoDoc(proxy_class="featurebyte.Series")

    # instance variables
    name: Optional[StrictStr] = Field(default=None)
    dtype: DBVarType = Field(frozen=True, description="variable type of the series")

    def __repr__(self) -> str:
        return f"{type(self).__name__}[{self.dtype}](name={self.name}, node_name={self.node_name})"

    def __str__(self) -> str:
        return repr(self)

    @typechecked
    def __getitem__(self: FrozenSeriesT, item: FrozenSeries) -> FrozenSeriesT:
        node = self._add_filter_operation(
            item=self, mask=item, node_output_type=NodeOutputType.SERIES
        )
        return type(self)(
            feature_store=self.feature_store,
            tabular_source=self.tabular_source,
            node_name=node.name,
            name=self.name,
            dtype=self.dtype,
        )

    @property
    def dtype_info(self) -> DBVarTypeInfo:
        """
        Get the DBVarTypeInfo of the series

        Returns
        -------
        DBVarTypeInfo
        """
        return self.operation_structure.series_output_dtype_info

    @property
    def associated_timezone_column_name(self) -> Optional[str]:
        """
        Get the associated timezone column name

        Returns
        -------
        Optional[str]
        """
        if self.dtype_info and self.dtype_info.timestamp_schema:
            if isinstance(self.dtype_info.timestamp_schema.timezone, TimeZoneColumn):
                return self.dtype_info.timestamp_schema.timezone.column_name
        return None

    @property
    def binary_op_output_class_priority(self) -> int:
        """
        Determines whether type(self) will be used as the output class (lower means higher
        priority).

        This is used to ensure combining a Feature and RequestColumn always produces a Feature
        regardless of the operands' ordering.

        Returns
        -------
        int
        """
        return 0

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
        other: Scalar | FrozenSeries | ScalarSequence,
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
        self: FrozenSeriesT, other: bool | FrozenSeries, node_type: NodeType
    ) -> FrozenSeriesT:
        """
        Apply binary logical operation between self & other objects

        Parameters
        ----------
        other: bool | FrozenSeries
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
    def __and__(self: FrozenSeriesT, other: Union[bool, FrozenSeries]) -> FrozenSeriesT:
        return self._binary_logical_op(other, NodeType.AND)

    @typechecked
    def __or__(self: FrozenSeriesT, other: Union[bool, FrozenSeries]) -> FrozenSeriesT:
        return self._binary_logical_op(other, NodeType.OR)

    def _binary_relational_op(
        self: FrozenSeriesT, other: Scalar | Timestamp | FrozenSeries, node_type: NodeType
    ) -> FrozenSeriesT:
        """
        Apply binary relational operation between self & other objects

        Parameters
        ----------
        other: int | float | str | bool | FrozenSeries
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
            DBVarType.DATE: (DBVarType.TIMESTAMP,),
            DBVarType.TIMESTAMP: (DBVarType.TIMESTAMP,),
            DBVarType.TIMESTAMP_TZ: (DBVarType.TIMESTAMP,),
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
    def __eq__(self: FrozenSeriesT, other: Union[Scalar, Timestamp, FrozenSeries]) -> FrozenSeriesT:  # type: ignore
        return self._binary_relational_op(other, NodeType.EQ)

    @typechecked
    def __ne__(self: FrozenSeriesT, other: Union[Scalar, Timestamp, FrozenSeries]) -> FrozenSeriesT:  # type: ignore
        return self._binary_relational_op(other, NodeType.NE)

    @typechecked
    def __lt__(self: FrozenSeriesT, other: Union[Scalar, Timestamp, FrozenSeries]) -> FrozenSeriesT:
        return self._binary_relational_op(other, NodeType.LT)

    @typechecked
    def __le__(self: FrozenSeriesT, other: Union[Scalar, Timestamp, FrozenSeries]) -> FrozenSeriesT:
        return self._binary_relational_op(other, NodeType.LE)

    @typechecked
    def __gt__(self: FrozenSeriesT, other: Union[Scalar, Timestamp, FrozenSeries]) -> FrozenSeriesT:
        return self._binary_relational_op(other, NodeType.GT)

    @typechecked
    def __ge__(self: FrozenSeriesT, other: Union[Scalar, Timestamp, FrozenSeries]) -> FrozenSeriesT:
        return self._binary_relational_op(other, NodeType.GE)

    def _binary_arithmetic_op(
        self: FrozenSeriesT,
        other: int | float | str | FrozenSeries,
        node_type: NodeType,
        right_op: bool = False,
    ) -> FrozenSeriesT:
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
        FrozenSeriesT
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
    def _date_diff_op(
        self: FrozenSeriesT, other: FrozenSeries, right_op: bool = False
    ) -> FrozenSeriesT:
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
        FrozenSeriesT
            output of the date difference operation
        """
        return self._binary_op(
            other=other,
            node_type=NodeType.DATE_DIFF,
            output_var_type=DBVarType.TIMEDELTA,
            right_op=right_op,
            additional_node_params={
                "left_timestamp_metadata": self.dtype_info and self.dtype_info.metadata,
                "right_timestamp_metadata": other.dtype_info and other.dtype_info.metadata,
            },
        )

    @typechecked
    def _date_add_op(
        self: FrozenSeriesT, other: Union[FrozenSeries, pd.Timedelta], right_op: bool = False
    ) -> FrozenSeriesT:
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
        FrozenSeriesT
            output of the date difference operation

        Raises
        ------
        NotImplementedError
            If the input series has TIMESTAMP_TZ_TUPLE data type
        """
        bin_op_other = other
        if isinstance(other, pd.Timedelta):
            bin_op_other = other.total_seconds()

        has_timezone_tz_tuple = self.dtype == DBVarType.TIMESTAMP_TZ_TUPLE
        if isinstance(other, FrozenSeries):
            has_timezone_tz_tuple |= other.dtype == DBVarType.TIMESTAMP_TZ_TUPLE

        if has_timezone_tz_tuple:
            raise NotImplementedError(
                "Date add operation is not supported for TIMESTAMP_TZ_TUPLE data type."
            )

        return self._binary_op(
            other=bin_op_other,
            node_type=NodeType.DATE_ADD,
            output_var_type=DBVarType.TIMESTAMP,
            right_op=right_op,
            additional_node_params={
                "left_timestamp_metadata": self.dtype_info and self.dtype_info.metadata,
            },
        )

    @typechecked
    def __add__(
        self: FrozenSeriesT, other: Union[int, float, str, pd.Timedelta, FrozenSeries]
    ) -> FrozenSeriesT:
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
    def __radd__(
        self: FrozenSeriesT, other: Union[int, float, str, pd.Timedelta, FrozenSeries]
    ) -> FrozenSeriesT:
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
    def __sub__(self: FrozenSeriesT, other: Union[int, float, FrozenSeries]) -> FrozenSeriesT:
        if self.is_datetime and isinstance(other, FrozenSeries) and other.is_datetime:
            return self._date_diff_op(other)
        return self._binary_arithmetic_op(other, NodeType.SUB)

    @typechecked
    def __rsub__(self: FrozenSeriesT, other: Union[int, float, FrozenSeries]) -> FrozenSeriesT:
        return self._binary_arithmetic_op(other, NodeType.SUB, right_op=True)

    @typechecked
    def __mul__(self: FrozenSeriesT, other: Union[int, float, FrozenSeries]) -> FrozenSeriesT:
        return self._binary_arithmetic_op(other, NodeType.MUL)

    @typechecked
    def __rmul__(self: FrozenSeriesT, other: Union[int, float, FrozenSeries]) -> FrozenSeriesT:
        return self._binary_arithmetic_op(other, NodeType.MUL, right_op=True)

    @typechecked
    def __truediv__(self: FrozenSeriesT, other: Union[int, float, FrozenSeries]) -> FrozenSeriesT:
        return self._binary_arithmetic_op(other, NodeType.DIV)

    @typechecked
    def __rtruediv__(self: FrozenSeriesT, other: Union[int, float, FrozenSeries]) -> FrozenSeriesT:
        return self._binary_arithmetic_op(other, NodeType.DIV, right_op=True)

    @typechecked
    def __mod__(self: FrozenSeriesT, other: Union[int, float, FrozenSeries]) -> FrozenSeriesT:
        return self._binary_arithmetic_op(other, NodeType.MOD)

    @typechecked
    def __rmod__(self: FrozenSeriesT, other: Union[int, float, FrozenSeries]) -> FrozenSeriesT:
        return self._binary_arithmetic_op(other, NodeType.MOD, right_op=True)

    def __invert__(self: FrozenSeriesT) -> FrozenSeriesT:
        return series_unary_operation(
            input_series=self,
            node_type=NodeType.NOT,
            output_var_type=DBVarType.BOOL,
            node_params={},
        )

    @typechecked
    def __pow__(self: FrozenSeriesT, other: Union[int, float, FrozenSeries]) -> FrozenSeriesT:
        return self.pow(other)

    @property
    def is_datetime(self) -> bool:
        """
        Returns whether Series has a datetime like variable type

        Returns
        -------
        bool

        Examples
        --------
        >>> view = catalog.get_view("GROCERYINVOICE")

        >>> print(view["Timestamp"].is_datetime)
        True
        >>> print(view["Amount"].is_datetime)
        False
        """
        return (
            self.dtype
            in (
                DBVarType.TIMESTAMP,
                DBVarType.TIMESTAMP_TZ,
                DBVarType.DATE,
                DBVarType.TIMESTAMP_TZ_TUPLE,
            )
            or self.dtype_info.timestamp_schema is not None
        )

    @property
    def is_numeric(self) -> bool:
        """
        Returns whether Series has a numeric variable type

        Returns
        -------
        bool

        Examples
        --------
        >>> view = catalog.get_view("GROCERYINVOICE")

        >>> print(view["Amount"].is_numeric)
        True
        >>> print(view["Timestamp"].is_numeric)
        False
        """
        return self.dtype in (DBVarType.INT, DBVarType.FLOAT)

    def isnull(self: FrozenSeriesT) -> FrozenSeriesT:
        """
        Returns a boolean Series indicating whether each element is missing.

        Returns
        -------
        FrozenSeriesT
            Column or Feature with boolean values

        Examples
        --------
        Filter a View based on whether a column has null values:

        >>> view = catalog.get_view("GROCERYINVOICE")
        >>> view_filtered = view[view["Amount"].isnull()]
        """
        return series_unary_operation(
            input_series=self,
            node_type=NodeType.IS_NULL,
            output_var_type=DBVarType.BOOL,
            node_params={},
        )

    def notnull(self: FrozenSeriesT) -> FrozenSeriesT:
        """
        Returns a boolean Series indicating whether each element is not null.

        Returns
        -------
        FrozenSeriesT
            Column or Feature with boolean values

        Examples
        --------
        Filter a View by removing rows where a column has null values:

        >>> view = catalog.get_view("GROCERYINVOICE")
        >>> view_filtered = view[view["Amount"].notnull()]
        """
        return ~self.isnull()

    @typechecked
    def astype(
        self: FrozenSeriesT,
        new_type: Union[Type[int], Type[float], Type[str], Literal["int", "float", "str"]],
    ) -> FrozenSeriesT:
        """
        Convert a Series to have a new type.

        This is useful for converting a series between a string, and numerical types, or vice-versa.

        Parameters
        ----------
        new_type : Union[Type[int], Type[float], Type[str], Literal["int", "float", "str"]])
            Desired type after conversion. Type can be provided directly, or as a string.

        Returns
        -------
        FrozenSeriesT
            A new Series with converted variable type.

        Raises
        ------
        TypeError
            If the Series dtype does not support type conversion.

        Examples
        --------
        Convert a numerical series to a string series, and back to an int series.

        >>> event_view = catalog.get_view("GROCERYINVOICE")
        >>> event_view["Amount"] = event_view["Amount"].astype(str)
        >>> event_view["Amount"] = event_view["Amount"].astype(int)
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
        )

    @numeric_only
    def abs(self: FrozenSeriesT) -> FrozenSeriesT:
        """
        Computes the absolute numeric value of each element.

        Returns
        -------
        FrozenSeriesT
            Column or Feature with absolute values

        Examples
        --------
        Compute absolute values for a Column in a View:

        >>> view = catalog.get_view("GROCERYCUSTOMER")
        >>> view["LongitudeAbs"] = view["Longitude"].abs()
        >>> view.preview(5).filter(regex="Longitude")
           Longitude  LongitudeAbs
        0   5.209627      5.209627
        1   7.423195      7.423195
        2   2.990572      2.990572
        3 -61.069866     61.069866
        4   3.120654      3.120654

        Compute absolute values for a Feature:

        >>> feature = catalog.get_feature("InvoiceCount_60days")
        >>> feature_abs = feature.abs()
        """
        return series_unary_operation(
            input_series=self,
            node_type=NodeType.ABS,
            output_var_type=self.dtype,
            node_params={},
        )

    @numeric_only
    def sqrt(self: FrozenSeriesT) -> FrozenSeriesT:
        """
        Computes the square root of each element.

        Returns
        -------
        FrozenSeriesT
            Column or Feature with square root values

        Examples
        --------
        Compute square root values for a Column in a View:

        >>> view = catalog.get_view("GROCERYINVOICE")
        >>> view["AmountSqrt"] = view["Amount"].sqrt()
        >>> view.preview(5).filter(regex="Amount")
           Amount  AmountSqrt
        0   10.68    3.268027
        1   38.04    6.167658
        2    1.99    1.410674
        3   37.21    6.100000
        4    1.20    1.095445


        Compute square root values for a Feature:

        >>> feature = catalog.get_feature("InvoiceCount_60days")
        >>> feature_sqrt = feature.sqrt()
        """
        return series_unary_operation(
            input_series=self,
            node_type=NodeType.SQRT,
            output_var_type=DBVarType.FLOAT,
            node_params={},
        )

    @numeric_only
    def pow(self: FrozenSeriesT, other: int | float | FrozenSeries) -> FrozenSeriesT:
        """
        Computes the exponential power of each element.

        Parameters
        ----------
        other : int | float | FrozenSeries
            Power to raise to

        Returns
        -------
        FrozenSeriesT
            Column or Feature with exponential power values

        Examples
        --------
        Compute exponential power values for a Column in a View:

        >>> view = catalog.get_view("GROCERYINVOICE")
        >>> view["Amount^2"] = view["Amount"].pow(2)
        >>> view.preview(5).filter(regex="Amount")
           Amount   Amount^2
        0   10.68   114.0624
        1   38.04  1447.0416
        2    1.99     3.9601
        3   37.21  1384.5841
        4    1.20     1.4400


        Compute exponential power values for a Feature:

        >>> feature = catalog.get_feature("InvoiceCount_60days")
        >>> feature_pow = feature.pow(2)
        """
        return self._binary_op(
            other=other,
            node_type=NodeType.POWER,
            output_var_type=DBVarType.FLOAT,
        )

    @numeric_only
    def log(self: FrozenSeriesT) -> FrozenSeriesT:
        """
        Computes the natural logarithm of each element.

        Returns
        -------
        FrozenSeriesT
            Column or Feature with natural logarithm values

        Examples
        --------
        Compute natural logarithm values for a Column in a View:

        >>> view = catalog.get_view("GROCERYINVOICE")
        >>> view["AmountLog"] = view["Amount"].log()
        >>> view.preview(5).filter(regex="Amount")
          Amount  AmountLog
        0   10.68   2.368373
        1   38.04   3.638638
        2    1.99   0.688135
        3   37.21   3.616578
        4    1.20   0.182322


        Compute natural logarithm values for a Feature:

        >>> feature = catalog.get_feature("InvoiceCount_60days")
        >>> feature_log = (feature + 1.0).log()
        """
        return series_unary_operation(
            input_series=self,
            node_type=NodeType.LOG,
            output_var_type=DBVarType.FLOAT,
            node_params={},
        )

    @numeric_only
    def exp(self: FrozenSeriesT) -> FrozenSeriesT:
        """
        Computes the exponential value of each element.

        Returns
        -------
        FrozenSeriesT
            Column or Feature with exponential values

        Examples
        --------
        Compute exponential values for a Column in a View:

        >>> view = catalog.get_view("INVOICEITEMS")
        >>> view["QuantityExp"] = view["Quantity"].exp()
        >>> view.preview(5).filter(regex="Quantity")
           Quantity  QuantityExp
        0       2.0     7.389056
        1       1.0     2.718282
        2       1.0     2.718282
        3       1.0     2.718282
        4       2.0     7.389056


        Compute exponential values for a Feature:

        >>> feature = catalog.get_feature("InvoiceCount_60days")
        >>> feature_exp = feature.exp()
        """
        return series_unary_operation(
            input_series=self,
            node_type=NodeType.EXP,
            output_var_type=DBVarType.FLOAT,
            node_params={},
        )

    @numeric_only
    def floor(self: FrozenSeriesT) -> FrozenSeriesT:
        """
        Rounds each element to the nearest equal or smaller integer.

        Returns
        -------
        FrozenSeriesT
            Column or Feature with rounded values

        Examples
        --------
        Round values for a Column in a View:

        >>> view = catalog.get_view("GROCERYINVOICE")
        >>> column = view["Amount"].floor()
        >>> view["AmountFloor"] = view["Amount"].floor()
        >>> view.preview(5).filter(regex="Amount")
           Amount  AmountFloor
        0   10.68          10
        1   38.04          38
        2    1.99           1
        3   37.21          37
        4    1.20           1


        Round values for a Feature:

        >>> feature = catalog.get_feature("InvoiceCount_60days")
        >>> feature_floor = feature.floor()
        """
        return series_unary_operation(
            input_series=self,
            node_type=NodeType.FLOOR,
            output_var_type=DBVarType.INT,
            node_params={},
        )

    @numeric_only
    def ceil(self: FrozenSeriesT) -> FrozenSeriesT:
        """
        Rounds each element to the nearest equal or larger integer.

        Returns
        -------
        FrozenSeriesT
            Series or Feature with rounded values

        Examples
        --------
        Compute rounded values for a Column in a View:

        >>> view = catalog.get_view("GROCERYINVOICE")
        >>> view["AmountCeil"] = view["Amount"].ceil()
        >>> view.preview(5).filter(regex="Amount")
           Amount  AmountCeil
        0   10.68          11
        1   38.04          39
        2    1.99           2
        3   37.21          38
        4    1.20           2


        Compute rounded values for a Feature:

        >>> feature = catalog.get_feature("InvoiceCount_60days")
        >>> feature_ceil = feature.ceil()
        """
        return series_unary_operation(
            input_series=self,
            node_type=NodeType.CEIL,
            output_var_type=DBVarType.INT,
            node_params={},
        )

    @numeric_only
    def cos(self: FrozenSeriesT) -> FrozenSeriesT:
        """
        Get the cos value of each element in the Series.

        Returns
        -------
        FrozenSeriesT
            Series or Feature with rounded values

        Examples
        --------
        >>> view = catalog.get_view("GROCERYINVOICE")
        >>> view["AmountCos"] = view["Amount"].cos()  # doctest: +SKIP
        """
        return series_unary_operation(
            input_series=self,
            node_type=NodeType.COS,
            output_var_type=DBVarType.FLOAT,
            node_params={},
        )

    @numeric_only
    def sin(self: FrozenSeriesT) -> FrozenSeriesT:
        """
        Get the sin value of each element in the Series.

        Returns
        -------
        FrozenSeriesT
            Series or Feature with rounded values

        Examples
        --------
        >>> view = catalog.get_view("GROCERYINVOICE")
        >>> view["AmountCos"] = view["Amount"].sin()  # doctest: +SKIP
        """
        return series_unary_operation(
            input_series=self,
            node_type=NodeType.SIN,
            output_var_type=DBVarType.FLOAT,
            node_params={},
        )

    @numeric_only
    def tan(self: FrozenSeriesT) -> FrozenSeriesT:
        """
        Get the tan value of each element in the Series.

        Returns
        -------
        FrozenSeriesT
            Series or Feature with rounded values

        Examples
        --------
        >>> view = catalog.get_view("GROCERYINVOICE")
        >>> view["AmountCos"] = view["Amount"].tan()  # doctest: +SKIP
        """
        return series_unary_operation(
            input_series=self,
            node_type=NodeType.TAN,
            output_var_type=DBVarType.FLOAT,
            node_params={},
        )

    @numeric_only
    def acos(self: FrozenSeriesT) -> FrozenSeriesT:
        """
        Get the arccos value of each element in the Series.

        Returns
        -------
        FrozenSeriesT
            Series or Feature with rounded values

        Examples
        --------
        >>> view = catalog.get_view("GROCERYINVOICE")
        >>> view["AmountCos"] = view["Amount"].acos()  # doctest: +SKIP
        """
        return series_unary_operation(
            input_series=self,
            node_type=NodeType.ACOS,
            output_var_type=DBVarType.FLOAT,
            node_params={},
        )

    @numeric_only
    def asin(self: FrozenSeriesT) -> FrozenSeriesT:
        """
        Get the arcsin value of each element in the Series.

        Returns
        -------
        FrozenSeriesT
            Series or Feature with rounded values

        Examples
        --------
        >>> view = catalog.get_view("GROCERYINVOICE")
        >>> view["AmountAsin"] = view["Amount"].asin()  # doctest: +SKIP
        """
        return series_unary_operation(
            input_series=self,
            node_type=NodeType.ASIN,
            output_var_type=DBVarType.FLOAT,
            node_params={},
        )

    @numeric_only
    def atan(self: FrozenSeriesT) -> FrozenSeriesT:
        """
        Get the arctan value of each element in the Series.

        Returns
        -------
        FrozenSeriesT
            Series or Feature with rounded values

        Examples
        --------
        >>> view = catalog.get_view("GROCERYINVOICE")
        >>> view["AmountAtan"] = view["Amount"].atan()  # doctest: +SKIP
        """
        return series_unary_operation(
            input_series=self,
            node_type=NodeType.ATAN,
            output_var_type=DBVarType.FLOAT,
            node_params={},
        )

    def copy(self: FrozenSeriesT, *args: Any, **kwargs: Any) -> FrozenSeriesT:
        # Copying a Series should prevent it from modifying the parent Frame
        kwargs.pop("deep", None)
        return super().copy(*args, **kwargs, deep=True)

    @typechecked
    def isin(self: FrozenSeriesT, other: Union[FrozenSeries, ScalarSequence]) -> FrozenSeriesT:
        """
        Identifies if each element is contained in a sequence of values represented by the `other` parameter.

        Parameters
        ----------
        other: Union[FrozenSeries, ScalarSequence]
            The sequence of values to check for membership. `other` can be a predefined list of values.

        Returns
        -------
        FrozenSeriesT
            Column or Feature with boolean values

        Raises
        ------
        ValueError
            raised when `other` is a Feature object but is not a dictionary feature.

        Examples
        --------

        Check to see if the values in a series are in a list of values, and use the result to filter
        the original view:

        >>> view = catalog.get_table("GROCERYPRODUCT").get_view()
        >>> condition = view["ProductGroup"].isin(["Sauces", "Fromages", "Fruits"])
        >>> view[condition].sample(5, seed=0)
                             GroceryProductGuid ProductGroup
        0  45cd58ba-efec-463a-9107-0633168a215e     Fromages
        1  97e6afc9-1033-4fb3-b2a2-3d62261e1d17     Fromages
        2  fb26ed22-524e-4c9e-9ea2-03c266e7f9b9     Fromages
        3  a817d904-bc58-4048-978d-c13857969a69       Fruits
        4  00abe6d0-e3f7-4f29-b0ab-69ea5581ab02       Sauces

        Create a new feature that checks whether a lookup feature is contained in the keys of a
        dictionary feature:

        >>> lookup_feature = catalog.get_feature("ProductGroupLookup")
        >>> dictionary_feature = catalog.get_feature("CustomerProductGroupCounts_7d")
        >>> new_feature = lookup_feature.isin(dictionary_feature)
        >>> new_feature.name = "CustomerHasProductGroup_7d"
        """
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
            additional_node_params=additional_node_params,
        )


class Series(FrozenSeries):
    """
    Series is a mutable version of FrozenSeries. It is used to represent a single column of a Frame.
    This class supports in-place column modification, and is the primary interface for column.
    """

    def validate_series_operation(self, other_series: Series) -> bool:
        """
        Validate the other series for series operation. This method is when performing the __setitem__ operation
        between two series.

        Parameters
        ----------
        other_series: Series
            The other series to validate

        Returns
        -------
        bool
        """
        _ = other_series
        return True

    @typechecked
    def __setitem__(
        self, key: FrozenSeries, value: Union[int, float, str, bool, None, FrozenSeries]
    ) -> None:
        if isinstance(value, Series):
            if not self.validate_series_operation(value) or not value.validate_series_operation(
                self
            ):
                raise TypeError(
                    f"Operation between {self.__class__.__name__} and {value.__class__.__name__} "
                    f"is not supported"
                )

        if self.row_index_lineage != key.row_index_lineage:
            raise ValueError(f"Row indices between '{self}' and '{key}' are not aligned!")
        if key.dtype != DBVarType.BOOL:
            raise TypeError("Only boolean Series filtering is supported!")

        self._assert_assignment_valid(value)
        original_name = self.name
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
        else:
            # This could be for a target object. Set name to add an alias node so that the name
            # is preserved.
            if original_name is not None:
                self.name = original_name

    @typechecked
    def fillna(self, other: Scalar) -> None:
        """
        Replaces missing value in each element with the provided value in-place.

        Parameters
        ----------
        other: Scalar
            Value to replace missing values

        Examples
        --------

        Fill missing values in a column with 0:

        >>> view = catalog.get_view("GROCERYINVOICE")
        >>> view["Amount"].fillna(0)


        Fill missing values in a feature with 0:

        >>> feature = catalog.get_feature("InvoiceAmountAvg_60days")
        >>> feature.fillna(0)
        """
        self[self.isnull()] = other
