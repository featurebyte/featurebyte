"""
This module contains string accessor class
"""
from __future__ import annotations

from typing import TYPE_CHECKING, List, Optional, Tuple, TypeVar

from typeguard import typechecked

from featurebyte.core.util import series_unary_operation
from featurebyte.enum import DBVarType
from featurebyte.query_graph.enum import NodeType
from featurebyte.query_graph.node.string import Side

if TYPE_CHECKING:
    from featurebyte.core.series import Series
else:
    Series = TypeVar("Series")


class StrAccessorMixin:
    """
    StrAccessorMixin class
    """

    # pylint: disable=too-few-public-methods

    @property
    def str(self: Series) -> StringAccessor:  # type: ignore
        """
        str accessor object

        Returns
        -------
        StringAccessor
        """
        return StringAccessor(self)


class StringAccessor:
    """
    StringAccessor class used to manipulate string type Series object
    """

    # documentation metadata
    __fbautodoc__: List[str] = ["Series"]
    __fbautodoc_proxy_class__: Tuple[str, str] = ("featurebyte.core.series.Series", "str")

    def __init__(self, obj: Series):
        if obj.dtype != DBVarType.VARCHAR:
            raise AttributeError("Can only use .str accessor with VARCHAR values!")
        self._obj = obj

    def __getitem__(self, item: slice) -> Series:
        if not isinstance(item, slice):
            raise TypeError(
                f'type of argument "item" must be slice; got {type(item).__name__} instead'
            )
        return self.slice(start=item.start, stop=item.stop, step=item.step)

    def len(self) -> Series:
        """
        Compute len of the string for each row

        Returns
        -------
        Series
        """
        return series_unary_operation(
            input_series=self._obj,
            node_type=NodeType.LENGTH,
            output_var_type=DBVarType.INT,
            node_params={},
            **self._obj.unary_op_series_params(),
        )

    def lower(self) -> Series:
        """
        Convert the string value into the lower case

        Returns
        -------
        Series
        """
        return series_unary_operation(
            input_series=self._obj,
            node_type=NodeType.STR_CASE,
            output_var_type=DBVarType.VARCHAR,
            node_params={"case": "lower"},
            **self._obj.unary_op_series_params(),
        )

    def upper(self) -> Series:
        """
        Convert the string value into the upper case

        Returns
        -------
        Series
        """
        return series_unary_operation(
            input_series=self._obj,
            node_type=NodeType.STR_CASE,
            output_var_type=DBVarType.VARCHAR,
            node_params={"case": "upper"},
            **self._obj.unary_op_series_params(),
        )

    @typechecked
    def strip(self, to_strip: Optional[str] = None) -> Series:
        """
        Trim the white space(s) on the left & right string boundaries

        Parameters
        ----------
        to_strip: Optional[str]
            Characters to remove, white space is used by default

        Returns
        -------
        Series
        """
        return series_unary_operation(
            input_series=self._obj,
            node_type=NodeType.TRIM,
            output_var_type=DBVarType.VARCHAR,
            node_params={"character": to_strip, "side": "both"},
            **self._obj.unary_op_series_params(),
        )

    @typechecked
    def lstrip(self, to_strip: Optional[str] = None) -> Series:
        """
        Trim the white space(s) on the left string boundaries

        Parameters
        ----------
        to_strip: Optional[str]
            Characters to remove, white space is used by default

        Returns
        -------
        Series
        """
        return series_unary_operation(
            input_series=self._obj,
            node_type=NodeType.TRIM,
            output_var_type=DBVarType.VARCHAR,
            node_params={"character": to_strip, "side": "left"},
            **self._obj.unary_op_series_params(),
        )

    @typechecked
    def rstrip(self, to_strip: Optional[str] = None) -> Series:
        """
        Trim the white space(s) on the right string boundaries

        Parameters
        ----------
        to_strip: Optional[str]
            Characters to remove, white space is used by default

        Returns
        -------
        Series
        """
        return series_unary_operation(
            input_series=self._obj,
            node_type=NodeType.TRIM,
            output_var_type=DBVarType.VARCHAR,
            node_params={"character": to_strip, "side": "right"},
            **self._obj.unary_op_series_params(),
        )

    @typechecked
    def replace(self, pat: str, repl: str) -> Series:
        """
        Replace the substring within the original string

        Parameters
        ----------
        pat: str
            Substring to match
        repl: str
            Replacement string

        Returns
        -------
        Series
        """
        return series_unary_operation(
            input_series=self._obj,
            node_type=NodeType.REPLACE,
            output_var_type=DBVarType.VARCHAR,
            node_params={"pattern": pat, "replacement": repl},
            **self._obj.unary_op_series_params(),
        )

    @typechecked
    def pad(self, width: int, side: Side = "left", fillchar: str = " ") -> Series:
        """
        Pad the string up to the specified width size

        Parameters
        ----------
        width: int
            Minimum width of resulting string, additional characters will be filled with `fillchar`
        side: Side
            Padding direction
        fillchar: str
            Character used to pad

        Returns
        -------
        Series
        """
        return series_unary_operation(
            input_series=self._obj,
            node_type=NodeType.PAD,
            output_var_type=DBVarType.VARCHAR,
            node_params={"side": side, "length": width, "pad": fillchar},
            **self._obj.unary_op_series_params(),
        )

    @typechecked
    def contains(self, pat: str, case: bool = True) -> Series:
        """
        Compute a boolean flag where each value is a result of test whether the substring is inside the value

        Parameters
        ----------
        pat: str
            Substring pattern
        case: bool
            Whether the check is case-sensitive or not

        Returns
        -------
        Series
        """
        return series_unary_operation(
            input_series=self._obj,
            node_type=NodeType.STR_CONTAINS,
            output_var_type=DBVarType.BOOL,
            node_params={"pattern": pat, "case": bool(case)},
            **self._obj.unary_op_series_params(),
        )

    @typechecked
    def slice(
        self, start: Optional[int] = None, stop: Optional[int] = None, step: Optional[int] = None
    ) -> Series:
        """
        Slice substring from each element in the series

        Parameters
        ----------
        start: Optional[int]
            Starting position for slice operation
        stop: Optional[int]
            Ending position for slice operation
        step: Optional[int]
            Step size for slice operation (only size 1 is supported)

        Returns
        -------
        Series

        Raises
        ------
        ValueError
            When the step size is neither None nor 1
        """
        if step is not None and step != 1:
            raise ValueError("Can only use step size equals to 1.")

        start = start or 0
        length = None if stop is None else stop - start
        return series_unary_operation(
            input_series=self._obj,
            node_type=NodeType.SUBSTRING,
            output_var_type=DBVarType.VARCHAR,
            node_params={"start": start, "length": length},
            **self._obj.unary_op_series_params(),
        )
