"""
This module contains string accessor class
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Optional, TypeVar

from typeguard import typechecked

from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.core.util import series_unary_operation
from featurebyte.enum import DBVarType
from featurebyte.query_graph.enum import NodeType
from featurebyte.query_graph.node.string import Side

if TYPE_CHECKING:
    from featurebyte.core.series import FrozenSeries
else:
    FrozenSeries = TypeVar("FrozenSeries")


class StrAccessorMixin:
    """
    StrAccessorMixin class
    """

    @property
    def str(self: FrozenSeries) -> StringAccessor:  # type: ignore
        """
        str accessor object

        Returns
        -------
        StringAccessor
        """
        return StringAccessor(self)


class StringAccessor:
    """
    StringAccessor class used to manipulate string type FrozenSeries object
    """

    # documentation metadata
    __fbautodoc__ = FBAutoDoc(
        section=["Series"],
        proxy_class="featurebyte.Series",
        accessor_name="str",
    )

    def __init__(self, obj: FrozenSeries):
        if obj.dtype != DBVarType.VARCHAR:
            raise AttributeError("Can only use .str accessor with VARCHAR values!")
        self._obj = obj

    def __getitem__(self, item: slice) -> FrozenSeries:
        if not isinstance(item, slice):
            raise TypeError(
                f'type of argument "item" must be slice; got {type(item).__name__} instead'
            )
        return self.slice(start=item.start, stop=item.stop, step=item.step)

    def len(self) -> FrozenSeries:
        """
        Compute len of the string for each row

        Returns
        -------
        FrozenSeries
        """
        return series_unary_operation(
            input_series=self._obj,
            node_type=NodeType.LENGTH,
            output_var_type=DBVarType.INT,
            node_params={},
            **self._obj.unary_op_series_params(),
        )

    def lower(self) -> FrozenSeries:
        """
        Convert the string value into the lower case

        Returns
        -------
        FrozenSeries
        """
        return series_unary_operation(
            input_series=self._obj,
            node_type=NodeType.STR_CASE,
            output_var_type=DBVarType.VARCHAR,
            node_params={"case": "lower"},
            **self._obj.unary_op_series_params(),
        )

    def upper(self) -> FrozenSeries:
        """
        Convert the string value into the upper case

        Returns
        -------
        FrozenSeries
        """
        return series_unary_operation(
            input_series=self._obj,
            node_type=NodeType.STR_CASE,
            output_var_type=DBVarType.VARCHAR,
            node_params={"case": "upper"},
            **self._obj.unary_op_series_params(),
        )

    @typechecked
    def strip(self, to_strip: Optional[str] = None) -> FrozenSeries:
        """
        Trim the white space(s) on the left & right string boundaries

        Parameters
        ----------
        to_strip: Optional[str]
            Characters to remove, white space is used by default

        Returns
        -------
        FrozenSeries
        """
        return series_unary_operation(
            input_series=self._obj,
            node_type=NodeType.TRIM,
            output_var_type=DBVarType.VARCHAR,
            node_params={"character": to_strip, "side": "both"},
            **self._obj.unary_op_series_params(),
        )

    @typechecked
    def lstrip(self, to_strip: Optional[str] = None) -> FrozenSeries:
        """
        Trim the white space(s) on the left string boundaries

        Parameters
        ----------
        to_strip: Optional[str]
            Characters to remove, white space is used by default

        Returns
        -------
        FrozenSeries
        """
        return series_unary_operation(
            input_series=self._obj,
            node_type=NodeType.TRIM,
            output_var_type=DBVarType.VARCHAR,
            node_params={"character": to_strip, "side": "left"},
            **self._obj.unary_op_series_params(),
        )

    @typechecked
    def rstrip(self, to_strip: Optional[str] = None) -> FrozenSeries:
        """
        Trim the white space(s) on the right string boundaries

        Parameters
        ----------
        to_strip: Optional[str]
            Characters to remove, white space is used by default

        Returns
        -------
        FrozenSeries
        """
        return series_unary_operation(
            input_series=self._obj,
            node_type=NodeType.TRIM,
            output_var_type=DBVarType.VARCHAR,
            node_params={"character": to_strip, "side": "right"},
            **self._obj.unary_op_series_params(),
        )

    @typechecked
    def replace(self, pat: str, repl: str) -> FrozenSeries:
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
        FrozenSeries
        """
        return series_unary_operation(
            input_series=self._obj,
            node_type=NodeType.REPLACE,
            output_var_type=DBVarType.VARCHAR,
            node_params={"pattern": pat, "replacement": repl},
            **self._obj.unary_op_series_params(),
        )

    @typechecked
    def pad(self, width: int, side: Side = "left", fillchar: str = " ") -> FrozenSeries:
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
        FrozenSeries
        """
        return series_unary_operation(
            input_series=self._obj,
            node_type=NodeType.PAD,
            output_var_type=DBVarType.VARCHAR,
            node_params={"side": side, "length": width, "pad": fillchar},
            **self._obj.unary_op_series_params(),
        )

    @typechecked
    def contains(self, pat: str, case: bool = True) -> FrozenSeries:
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
        FrozenSeries
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
    ) -> FrozenSeries:
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
        FrozenSeries

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
