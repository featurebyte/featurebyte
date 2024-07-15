"""
This module contains string accessor class
"""

from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar, Optional, TypeVar

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

    # class variables
    __fbautodoc__: ClassVar[FBAutoDoc] = FBAutoDoc(proxy_class="featurebyte.Series")

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
        Computes length of each string element.

        Returns
        -------
        FrozenSeries
            A new Column or Feature object.

        Examples
        --------

        Compute the length of each string element in the BrowserUserAgent column:

        >>> view = catalog.get_view("GROCERYCUSTOMER")
        >>> view["BrowserUserAgentLength"] = view["BrowserUserAgent"].str.len()
        >>> view.preview(5).filter(regex="BrowserUserAgent")
                                            BrowserUserAgent  BrowserUserAgentLength
        0  Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.3...                     102
        1  Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:6...                      78
        2  Mozilla/5.0 (Windows NT 10.0; Win64; x64) Appl...                     115
        3  Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_4...                     117
        4  Mozilla/5.0 (Windows NT 6.1; Win64; x64) Apple...                     113
        """
        return series_unary_operation(
            input_series=self._obj,
            node_type=NodeType.LENGTH,
            output_var_type=DBVarType.INT,
            node_params={},
        )

    def lower(self) -> FrozenSeries:
        """
        Converts each string element to lower case.

        Returns
        -------
        FrozenSeries
            A new Column or Feature object.

        Examples
        --------

        Convert the BrowserUserAgent column to lower case:

        >>> view = catalog.get_view("GROCERYCUSTOMER")
        >>> view["BrowserUserAgentLower"] = view["BrowserUserAgent"].str.lower()
        >>> view.preview(5).filter(regex="BrowserUserAgent")
                                            BrowserUserAgent                              BrowserUserAgentLower
        0  Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.3...  mozilla/5.0 (windows nt 6.1) applewebkit/537.3...
        1  Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:6...  mozilla/5.0 (windows nt 10.0; win64; x64; rv:6...
        2  Mozilla/5.0 (Windows NT 10.0; Win64; x64) Appl...  mozilla/5.0 (windows nt 10.0; win64; x64) appl...
        3  Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_4...  mozilla/5.0 (macintosh; intel mac os x 10_14_4...
        4  Mozilla/5.0 (Windows NT 6.1; Win64; x64) Apple...  mozilla/5.0 (windows nt 6.1; win64; x64) apple...
        """
        return series_unary_operation(
            input_series=self._obj,
            node_type=NodeType.STR_CASE,
            output_var_type=DBVarType.VARCHAR,
            node_params={"case": "lower"},
        )

    def upper(self) -> FrozenSeries:
        """
        Converts each string element to upper case.

        Returns
        -------
        FrozenSeries
            A new Column or Feature object.

        Examples
        --------

        Convert the BrowserUserAgent column to upper case:

        >>> view = catalog.get_view("GROCERYCUSTOMER")
        >>> view["BrowserUserAgentUpper"] = view["BrowserUserAgent"].str.upper()
        >>> view.preview(5).filter(regex="BrowserUserAgent")
                                            BrowserUserAgent                              BrowserUserAgentUpper
        0  Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.3...  MOZILLA/5.0 (WINDOWS NT 6.1) APPLEWEBKIT/537.3...
        1  Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:6...  MOZILLA/5.0 (WINDOWS NT 10.0; WIN64; X64; RV:6...
        2  Mozilla/5.0 (Windows NT 10.0; Win64; x64) Appl...  MOZILLA/5.0 (WINDOWS NT 10.0; WIN64; X64) APPL...
        3  Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_4...  MOZILLA/5.0 (MACINTOSH; INTEL MAC OS X 10_14_4...
        4  Mozilla/5.0 (Windows NT 6.1; Win64; x64) Apple...  MOZILLA/5.0 (WINDOWS NT 6.1; WIN64; X64) APPLE...
        """
        return series_unary_operation(
            input_series=self._obj,
            node_type=NodeType.STR_CASE,
            output_var_type=DBVarType.VARCHAR,
            node_params={"case": "upper"},
        )

    @typechecked
    def strip(self, to_strip: Optional[str] = None) -> FrozenSeries:
        """
        Removes leading and trailing characters (whitespaces by default) from each string element.

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

        >>> view = catalog.get_view("GROCERYCUSTOMER")
        >>> view["TitleStrip"] = view["Title"].str.strip("M")
        >>> view.preview(5).filter(regex="Title")
          Title TitleStrip
        0   Ms.         s.
        1   Ms.         s.
        2   Mr.         r.
        3  Mrs.        rs.
        4   Mr.         r.
        """
        return series_unary_operation(
            input_series=self._obj,
            node_type=NodeType.TRIM,
            output_var_type=DBVarType.VARCHAR,
            node_params={"character": to_strip, "side": "both"},
        )

    @typechecked
    def lstrip(self, to_strip: Optional[str] = None) -> FrozenSeries:
        """
        Removes leading characters (whitespaces by default) from each string element.

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

        >>> view = catalog.get_view("GROCERYCUSTOMER")
        >>> view["TitleStrip"] = view["Title"].str.lstrip("M")
        >>> view.preview(5).filter(regex="Title")
          Title TitleStrip
        0   Ms.         s.
        1   Ms.         s.
        2   Mr.         r.
        3  Mrs.        rs.
        4   Mr.         r.
        """
        return series_unary_operation(
            input_series=self._obj,
            node_type=NodeType.TRIM,
            output_var_type=DBVarType.VARCHAR,
            node_params={"character": to_strip, "side": "left"},
        )

    @typechecked
    def rstrip(self, to_strip: Optional[str] = None) -> FrozenSeries:
        """
        Removes trailing characters (whitespaces by default) from each string element.

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
        Remove trailing "M" characters from the Title column:

        >>> view = catalog.get_view("GROCERYCUSTOMER")
        >>> view["TitleStrip"] = view["Title"].str.rstrip(".")
        >>> view.preview(5).filter(regex="Title")
          Title TitleStrip
        0   Ms.         Ms
        1   Ms.         Ms
        2   Mr.         Mr
        3  Mrs.        Mrs
        4   Mr.         Mr
        """
        return series_unary_operation(
            input_series=self._obj,
            node_type=NodeType.TRIM,
            output_var_type=DBVarType.VARCHAR,
            node_params={"character": to_strip, "side": "right"},
        )

    @typechecked
    def replace(self, pat: str, repl: str) -> FrozenSeries:
        """
        Replaces the substring within each string element.

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

        >>> view = catalog.get_view("GROCERYCUSTOMER")
        >>> view["BrowserUserAgentNew"] = view["BrowserUserAgent"].str.replace("Windows", "Win")
        >>> view.preview(5).filter(regex="BrowserUserAgent")
                                            BrowserUserAgent                                BrowserUserAgentNew
        0  Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.3...  Mozilla/5.0 (Win NT 6.1) AppleWebKit/537.36 (K...
        1  Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:6...  Mozilla/5.0 (Win NT 10.0; Win64; x64; rv:66.0)...
        2  Mozilla/5.0 (Windows NT 10.0; Win64; x64) Appl...  Mozilla/5.0 (Win NT 10.0; Win64; x64) AppleWeb...
        3  Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_4...  Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_4...
        4  Mozilla/5.0 (Windows NT 6.1; Win64; x64) Apple...  Mozilla/5.0 (Win NT 6.1; Win64; x64) AppleWebK...
        """
        return series_unary_operation(
            input_series=self._obj,
            node_type=NodeType.REPLACE,
            output_var_type=DBVarType.VARCHAR,
            node_params={"pattern": pat, "replacement": repl},
        )

    @typechecked
    def pad(self, width: int, side: Side = "left", fillchar: str = " ") -> FrozenSeries:
        """
        Pads each string element up to the specified width.

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

        >>> view = catalog.get_view("GROCERYCUSTOMER")
        >>> view["PostalCodePadded"] = view["PostalCode"].str.pad(10, fillchar="0")
        >>> view.preview(5).filter(regex="PostalCode")
          PostalCode PostalCodePadded
        0      52000       0000052000
        1      68200       0000068200
        2      12100       0000012100
        3      97232       0000097232
        4      63000       0000063000
        """
        return series_unary_operation(
            input_series=self._obj,
            node_type=NodeType.PAD,
            output_var_type=DBVarType.VARCHAR,
            node_params={"side": side, "length": width, "pad": fillchar},
        )

    @typechecked
    def contains(self, pat: str, case: bool = True) -> FrozenSeries:
        """
        Checks whether each string element contains the provided substring pattern.

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

        >>> view = catalog.get_view("GROCERYCUSTOMER")
        >>> view["BrowserUserAgent_x64"] = view["BrowserUserAgent"].str.contains("x64")
        >>> view.preview(5).filter(regex="BrowserUserAgent")
                                            BrowserUserAgent  BrowserUserAgent_x64
        0  Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.3...                 False
        1  Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:6...                  True
        2  Mozilla/5.0 (Windows NT 10.0; Win64; x64) Appl...                  True
        3  Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_4...                 False
        4  Mozilla/5.0 (Windows NT 6.1; Win64; x64) Apple...                  True
        """
        return series_unary_operation(
            input_series=self._obj,
            node_type=NodeType.STR_CONTAINS,
            output_var_type=DBVarType.BOOL,
            node_params={"pattern": pat, "case": bool(case)},
        )

    @typechecked
    def slice(
        self, start: Optional[int] = None, stop: Optional[int] = None, step: Optional[int] = None
    ) -> FrozenSeries:
        """
        Slices substring from each string element.

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

        >>> view = catalog.get_view("GROCERYCUSTOMER")
        >>> view["BrowserUserAgentSlice"] = view["BrowserUserAgent"].str.slice(0, 10)
        >>> view.preview(5).filter(regex="BrowserUserAgent")
                                            BrowserUserAgent BrowserUserAgentSlice
        0  Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.3...            Mozilla/5.
        1  Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:6...            Mozilla/5.
        2  Mozilla/5.0 (Windows NT 10.0; Win64; x64) Appl...            Mozilla/5.
        3  Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_4...            Mozilla/5.
        4  Mozilla/5.0 (Windows NT 6.1; Win64; x64) Apple...            Mozilla/5.
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
        )
