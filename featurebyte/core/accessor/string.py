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
        Compute len of each string element.

        Returns
        -------
        FrozenSeries
            A new Column or Feature object.

        Examples
        --------

        Compute the length of each string element in the BrowserUserAgent column:

        >>> view = catalog.get_table("GROCERYCUSTOMER").get_view()
        >>> view["BrowserUserAgentLength"] = view["BrowserUserAgent"].str.len()
        >>> view.preview(5).filter(regex="BrowserUserAgent")
                                            BrowserUserAgent  BrowserUserAgentLength
        0  Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKi...                     109
        1  Mozilla/5.0 (Windows NT 10.0; Win64; x64) Appl...                     115
        2  Mozilla/5.0 (Windows NT 10.0; Win64; x64) Appl...                     115
        3  Mozilla/5.0 (Windows NT 10.0; Win64; x64) Appl...                     115
        4  Mozilla/5.0 (Windows NT 10.0; Win64; x64) Appl...                     114
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
        Convert each string element to lower case.

        Returns
        -------
        FrozenSeries
            A new Column or Feature object.

        Examples
        --------

        Convert the BrowserUserAgent column to lower case:

        >>> view = catalog.get_table("GROCERYCUSTOMER").get_view()
        >>> view["BrowserUserAgentLower"] = view["BrowserUserAgent"].str.lower()
        >>> view.preview(5).filter(regex="BrowserUserAgent")
                                            BrowserUserAgent                              BrowserUserAgentLower
        0  Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKi...  mozilla/5.0 (windows nt 6.2; wow64) applewebki...
        1  Mozilla/5.0 (Windows NT 10.0; Win64; x64) Appl...  mozilla/5.0 (windows nt 10.0; win64; x64) appl...
        2  Mozilla/5.0 (Windows NT 10.0; Win64; x64) Appl...  mozilla/5.0 (windows nt 10.0; win64; x64) appl...
        3  Mozilla/5.0 (Windows NT 10.0; Win64; x64) Appl...  mozilla/5.0 (windows nt 10.0; win64; x64) appl...
        4  Mozilla/5.0 (Windows NT 10.0; Win64; x64) Appl...  mozilla/5.0 (windows nt 10.0; win64; x64) appl...
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
        Convert each string element to upper case.

        Returns
        -------
        FrozenSeries
            A new Column or Feature object.

        Examples
        --------

        Convert the BrowserUserAgent column to upper case:

        >>> view = catalog.get_table("GROCERYCUSTOMER").get_view()
        >>> view["BrowserUserAgentUpper"] = view["BrowserUserAgent"].str.upper()
        >>> view.preview(5).filter(regex="BrowserUserAgent")
                                            BrowserUserAgent                              BrowserUserAgentUpper
        0  Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKi...  MOZILLA/5.0 (WINDOWS NT 6.2; WOW64) APPLEWEBKI...
        1  Mozilla/5.0 (Windows NT 10.0; Win64; x64) Appl...  MOZILLA/5.0 (WINDOWS NT 10.0; WIN64; X64) APPL...
        2  Mozilla/5.0 (Windows NT 10.0; Win64; x64) Appl...  MOZILLA/5.0 (WINDOWS NT 10.0; WIN64; X64) APPL...
        3  Mozilla/5.0 (Windows NT 10.0; Win64; x64) Appl...  MOZILLA/5.0 (WINDOWS NT 10.0; WIN64; X64) APPL...
        4  Mozilla/5.0 (Windows NT 10.0; Win64; x64) Appl...  MOZILLA/5.0 (WINDOWS NT 10.0; WIN64; X64) APPL...
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
        Remove leading and trailing characters (whitespaces by default) from each string element.

        Parameters
        ----------
        to_strip: Optional[str]
            Characters to remove, whitespace is used by default.

        Returns
        -------
        FrozenSeries
            A new Column or Feature object.

        Examples
        --------

        Remove leading and trailing "M" characters from the Title column:

        >>> view = catalog.get_table("GROCERYCUSTOMER").get_view()
        >>> view["TitleStrip"] = view["Title"].str.strip("M")
        >>> view.preview(5).filter(regex="Title")
          Title TitleStrip
        0   Mr.         r.
        1   Mr.         r.
        2   Mr.         r.
        3  Mrs.        rs.
        4   Mr.         r.
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
        Remove leading characters (whitespaces by default) from each string element.

        Parameters
        ----------
        to_strip: Optional[str]
            Characters to remove, whitespace is used by default.

        Returns
        -------
        FrozenSeries
            A new Column or Feature object.

        Examples
        --------

        Remove leading "M" characters from the Title column:

        >>> view = catalog.get_table("GROCERYCUSTOMER").get_view()
        >>> view["TitleStrip"] = view["Title"].str.lstrip("M")
        >>> view.preview(5).filter(regex="Title")
          Title TitleStrip
        0   Mr.         r.
        1   Mr.         r.
        2   Mr.         r.
        3  Mrs.        rs.
        4   Mr.         r.
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
        Remove leading characters (whitespaces by default) from each string element.

        Parameters
        ----------
        to_strip: Optional[str]
            Characters to remove, whitespace is used by default.

        Returns
        -------
        FrozenSeries
            A new Column or Feature object.

        Examples
        --------

        Remove leading "M" characters from the Title column:

        >>> view = catalog.get_table("GROCERYCUSTOMER").get_view()
        >>> view["TitleStrip"] = view["Title"].str.rstrip(".")
        >>> view.preview(5).filter(regex="Title")
          Title TitleStrip
        0   Mr.         Mr
        1   Mr.         Mr
        2   Mr.         Mr
        3  Mrs.        Mrs
        4   Mr.         Mr
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
        Replace the substring within each string element.

        Parameters
        ----------
        pat: str
            Substring to match.
        repl: str
            Replacement string.

        Returns
        -------
        FrozenSeries
            A new Column or Feature object.

        Examples
        --------

        Replace "Windows" with "Win" in the BrowserUserAgent column:

        >>> view = catalog.get_table("GROCERYCUSTOMER").get_view()
        >>> view["BrowserUserAgentNew"] = view["BrowserUserAgent"].str.replace("Windows", "Win")
        >>> view.preview(5).filter(regex="BrowserUserAgent")
                                            BrowserUserAgent                                BrowserUserAgentNew
        0  Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKi...  Mozilla/5.0 (Win NT 6.2; WOW64) AppleWebKit/53...
        1  Mozilla/5.0 (Windows NT 10.0; Win64; x64) Appl...  Mozilla/5.0 (Win NT 10.0; Win64; x64) AppleWeb...
        2  Mozilla/5.0 (Windows NT 10.0; Win64; x64) Appl...  Mozilla/5.0 (Win NT 10.0; Win64; x64) AppleWeb...
        3  Mozilla/5.0 (Windows NT 10.0; Win64; x64) Appl...  Mozilla/5.0 (Win NT 10.0; Win64; x64) AppleWeb...
        4  Mozilla/5.0 (Windows NT 10.0; Win64; x64) Appl...  Mozilla/5.0 (Win NT 10.0; Win64; x64) AppleWeb...
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
        Pad each string element up to the specified width.

        Parameters
        ----------
        width: int
            Minimum width of resulting string. Additional characters will be filled with `fillchar`.
        side: Side
            Padding direction.
        fillchar: str
            Character used to pad.

        Returns
        -------
        FrozenSeries
            A new Column or Feature object.

        Examples
        --------

        Pad the Title column to 10 characters:

        >>> view = catalog.get_table("GROCERYCUSTOMER").get_view()
        >>> view["PostalCodePadded"] = view["PostalCode"].str.pad(10, fillchar="0")
        >>> view.preview(5).filter(regex="PostalCode")
          PostalCode PostalCodePadded
        0      44230       0000044230
        1      78700       0000078700
        2      78180       0000078180
        3      13002       0000013002
        4      91100       0000091100
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
        Check whether each string element contains the provided substring pattern.

        Parameters
        ----------
        pat: str
            Substring pattern.
        case: bool
            Whether the check is case-sensitive.

        Returns
        -------
        FrozenSeries
            A new Column or Feature object.

        Examples
        --------

        Check whether the BrowserUserAgent column contains "x64":

        >>> view = catalog.get_table("GROCERYCUSTOMER").get_view()
        >>> view["BrowserUserAgent_x64"] = view["BrowserUserAgent"].str.contains("x64")
        >>> view.preview(5).filter(regex="BrowserUserAgent")
                                            BrowserUserAgent  BrowserUserAgent_x64
        0  Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKi...                 False
        1  Mozilla/5.0 (Windows NT 10.0; Win64; x64) Appl...                  True
        2  Mozilla/5.0 (Windows NT 10.0; Win64; x64) Appl...                  True
        3  Mozilla/5.0 (Windows NT 10.0; Win64; x64) Appl...                  True
        4  Mozilla/5.0 (Windows NT 10.0; Win64; x64) Appl...                  True
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
        Slice substring from each string element.

        Parameters
        ----------
        start: Optional[int]
            Starting position for slice operation.
        stop: Optional[int]
            Ending position for slice operation.
        step: Optional[int]
            Step size for slice operation (only size 1 is supported)

        Returns
        -------
        FrozenSeries
            A new Column or Feature object.

        Raises
        ------
        ValueError
            When the step size is neither None nor 1.

        Examples
        --------

        Slice the first 10 characters from the BrowserUserAgent column:

        >>> view = catalog.get_table("GROCERYCUSTOMER").get_view()
        >>> view["BrowserUserAgentSlice"] = view["BrowserUserAgent"].str.slice(0, 10)
        >>> view.preview(5).filter(regex="BrowserUserAgent")
                                            BrowserUserAgent BrowserUserAgentSlice
        0  Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKi...            Mozilla/5.
        1  Mozilla/5.0 (Windows NT 10.0; Win64; x64) Appl...            Mozilla/5.
        2  Mozilla/5.0 (Windows NT 10.0; Win64; x64) Appl...            Mozilla/5.
        3  Mozilla/5.0 (Windows NT 10.0; Win64; x64) Appl...            Mozilla/5.
        4  Mozilla/5.0 (Windows NT 10.0; Win64; x64) Appl...            Mozilla/5.
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
