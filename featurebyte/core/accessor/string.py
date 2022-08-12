"""
This modul contains string accessor class
"""
# pylint: disable=too-few-public-methods

from __future__ import annotations

from typing import TYPE_CHECKING, Literal, Optional

from pydantic import StrictInt, StrictStr, parse_obj_as

from featurebyte.core.util import series_unary_operation
from featurebyte.enum import DBVarType
from featurebyte.query_graph.enum import NodeType

if TYPE_CHECKING:
    from featurebyte.core.series import Series


Side = Literal["left", "right", "both"]


class StrAccessorMixin:
    """
    StrAccessorMixin class
    """

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

    def __init__(self, obj: Series):
        if obj.var_type != DBVarType.VARCHAR:
            raise AttributeError("Can only use .str accessor with VARCHAR values!")
        self._obj = obj

    def __getitem__(self, item: slice) -> Series:
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
            node_type=NodeType.STRCASE,
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
            node_type=NodeType.STRCASE,
            output_var_type=DBVarType.VARCHAR,
            node_params={"case": "upper"},
            **self._obj.unary_op_series_params(),
        )

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
        char_to_strip: Optional[str] = parse_obj_as(Optional[StrictStr], to_strip)  # type: ignore
        return series_unary_operation(
            input_series=self._obj,
            node_type=NodeType.TRIM,
            output_var_type=DBVarType.VARCHAR,
            node_params={"character": char_to_strip, "side": "both"},
            **self._obj.unary_op_series_params(),
        )

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
        char_to_strip: Optional[str] = parse_obj_as(Optional[StrictStr], to_strip)  # type: ignore
        return series_unary_operation(
            input_series=self._obj,
            node_type=NodeType.TRIM,
            output_var_type=DBVarType.VARCHAR,
            node_params={"character": char_to_strip, "side": "left"},
            **self._obj.unary_op_series_params(),
        )

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
        char_to_strip: Optional[str] = parse_obj_as(Optional[StrictStr], to_strip)  # type: ignore
        return series_unary_operation(
            input_series=self._obj,
            node_type=NodeType.TRIM,
            output_var_type=DBVarType.VARCHAR,
            node_params={"character": char_to_strip, "side": "right"},
            **self._obj.unary_op_series_params(),
        )

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
        pattern: str = parse_obj_as(StrictStr, pat)
        replacement: str = parse_obj_as(StrictStr, repl)
        return series_unary_operation(
            input_series=self._obj,
            node_type=NodeType.REPLACE,
            output_var_type=DBVarType.VARCHAR,
            node_params={"pattern": pattern, "replacement": replacement},
            **self._obj.unary_op_series_params(),
        )

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
        width_size: int = parse_obj_as(StrictInt, width)
        pad_side: Side = parse_obj_as(Side, side)  # type: ignore
        filled_char: str = parse_obj_as(StrictStr, fillchar)
        return series_unary_operation(
            input_series=self._obj,
            node_type=NodeType.PAD,
            output_var_type=DBVarType.VARCHAR,
            node_params={"side": pad_side, "length": width_size, "pad": filled_char},
            **self._obj.unary_op_series_params(),
        )

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
        pattern: str = parse_obj_as(StrictStr, pat)
        return series_unary_operation(
            input_series=self._obj,
            node_type=NodeType.STRCONTAINS,
            output_var_type=DBVarType.BOOL,
            node_params={"pattern": pattern, "case": bool(case)},
            **self._obj.unary_op_series_params(),
        )

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
        start_pos: Optional[int] = parse_obj_as(Optional[StrictInt], start)  # type: ignore
        stop_pos: Optional[int] = parse_obj_as(Optional[StrictInt], stop)  # type: ignore
        step_size: Optional[int] = parse_obj_as(Optional[StrictInt], step)  # type: ignore
        if step_size is not None and step_size != 1:
            raise ValueError("Can only use step size equals to 1.")

        start_pos = start_pos or 0
        length = None if stop_pos is None else stop_pos - start_pos
        return series_unary_operation(
            input_series=self._obj,
            node_type=NodeType.SUBSTRING,
            output_var_type=DBVarType.VARCHAR,
            node_params={"start": start_pos, "length": length},
            **self._obj.unary_op_series_params(),
        )
