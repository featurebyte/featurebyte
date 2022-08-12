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
        return series_unary_operation(
            input_series=self._obj,
            node_type=NodeType.LENGTH,
            output_var_type=DBVarType.INT,
            node_params={},
            **self._obj.unary_op_series_params(),
        )

    def lower(self) -> Series:
        return series_unary_operation(
            input_series=self._obj,
            node_type=NodeType.STRCASE,
            output_var_type=DBVarType.VARCHAR,
            node_params={"case": "lower"},
            **self._obj.unary_op_series_params(),
        )

    def upper(self) -> Series:
        return series_unary_operation(
            input_series=self._obj,
            node_type=NodeType.STRCASE,
            output_var_type=DBVarType.VARCHAR,
            node_params={"case": "upper"},
            **self._obj.unary_op_series_params(),
        )

    def strip(self, to_strip: Optional[str] = None) -> Series:
        to_strip = parse_obj_as(Optional[StrictStr], to_strip)
        return series_unary_operation(
            input_series=self._obj,
            node_type=NodeType.TRIM,
            output_var_type=DBVarType.VARCHAR,
            node_params={"character": to_strip, "side": "both"},
            **self._obj.unary_op_series_params(),
        )

    def lstrip(self, to_strip: Optional[str] = None) -> Series:
        to_strip = parse_obj_as(Optional[StrictStr], to_strip)
        return series_unary_operation(
            input_series=self._obj,
            node_type=NodeType.TRIM,
            output_var_type=DBVarType.VARCHAR,
            node_params={"character": to_strip, "side": "left"},
            **self._obj.unary_op_series_params(),
        )

    def rstrip(self, to_strip: Optional[str] = None) -> Series:
        to_strip = parse_obj_as(Optional[StrictStr], to_strip)
        return series_unary_operation(
            input_series=self._obj,
            node_type=NodeType.TRIM,
            output_var_type=DBVarType.VARCHAR,
            node_params={"character": to_strip, "side": "right"},
            **self._obj.unary_op_series_params(),
        )

    def replace(self, pat: str, repl: str) -> Series:
        pat = parse_obj_as(StrictStr, pat)
        repl = parse_obj_as(StrictStr, repl)
        return series_unary_operation(
            input_series=self._obj,
            node_type=NodeType.REPLACE,
            output_var_type=DBVarType.VARCHAR,
            node_params={"pattern": pat, "replacement": repl},
            **self._obj.unary_op_series_params(),
        )

    def pad(self, width: int, side: Side = "left", fillchar: str = " "):
        width = parse_obj_as(StrictInt, width)
        side = parse_obj_as(Side, side)
        fillchar = parse_obj_as(StrictStr, fillchar)
        return series_unary_operation(
            input_series=self._obj,
            node_type=NodeType.PAD,
            output_var_type=DBVarType.VARCHAR,
            node_params={"side": side, "length": width, "pad": fillchar},
            **self._obj.unary_op_series_params(),
        )

    def contains(self, pat: str, case: bool = True):
        pat = parse_obj_as(StrictStr, pat)
        case = True if case else False
        return series_unary_operation(
            input_series=self._obj,
            node_type=NodeType.STRCONTAINS,
            output_var_type=DBVarType.BOOL,
            node_params={"pattern": pat, "case": case},
            **self._obj.unary_op_series_params(),
        )

    def slice(self, start=None, stop=None, step=None) -> Series:
        start = parse_obj_as(Optional[StrictInt], start)
        stop = parse_obj_as(Optional[StrictInt], stop)
        step = parse_obj_as(Optional[StrictInt], step)
        if step is not None and step != 1:
            raise ValueError("Can only use step size equals to 1.")

        start = start or 0
        length = stop - start if stop else None
        return series_unary_operation(
            input_series=self._obj,
            node_type=NodeType.SUBSTRING,
            output_var_type=DBVarType.VARCHAR,
            node_params={"start": start, "length": length},
            **self._obj.unary_op_series_params(),
        )
