"""
Target string accessor module.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar, Optional, TypeVar

from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.core.accessor.string import StringAccessor
from featurebyte.query_graph.node.string import Side

if TYPE_CHECKING:
    from featurebyte.api.target import Target
else:
    Target = TypeVar("Target")


class TargetStrAccessorMixin:
    """
    TargetStrAccessorMixin class
    """

    @property
    def str(self: Target) -> StringAccessor:  # type: ignore
        """
        str accessor object

        Returns
        -------
        StringAccessor
        """
        return TargetStringAccessor(self)


class TargetStringAccessor(StringAccessor):
    """
    TargetStringAccessor class used to manipulate string type Target objects
    """

    # class variables
    __fbautodoc__: ClassVar[FBAutoDoc] = FBAutoDoc()

    def len(self) -> Target:
        """
        Computes length of each string element.

        Returns
        -------
        Target
            A new Target object.

        Examples
        --------
        Compute the length of each string element in the ProductGroupLookup target:

        >>> target = catalog.get_target("ProductGroupLookup")  # doctest: +SKIP
        >>> target["ProductGroupLength"] = target.str.len()  # doctest: +SKIP
        """
        return super().len()  # type: ignore[return-value]

    def lower(self) -> Target:
        """
        Converts each string element to lower case.

        Returns
        -------
        Target
            A new Target object.

        Examples
        --------
        Convert the ProductGroupLookup target to lower case:

        >>> target = catalog.get_target("ProductGroupLookup")  # doctest: +SKIP
        >>> target["ProductGroupLower"] = target.str.lower()  # doctest: +SKIP
        """
        return super().lower()  # type: ignore[return-value]

    def upper(self) -> Target:
        """
        Converts each string element to upper case.

        Returns
        -------
        Target
            A new Target object.

        Examples
        --------
        Convert the ProductGroupLookup target to upper case:

        >>> target = catalog.get_target("ProductGroupLookup")  # doctest: +SKIP
        >>> target["ProductGroupUpper"] = target.str.upper()  # doctest: +SKIP
        """
        return super().upper()  # type: ignore[return-value]

    def strip(self, to_strip: Optional[str] = None) -> Target:
        """
        Removes leading and trailing characters (whitespaces by default) from each string element.

        Parameters
        ----------
        to_strip: Optional[str]
            Characters to remove, whitespace is used by default.

        Returns
        -------
        Target
            A new Target object.

        Examples
        --------
        Remove leading and trailing "M" characters from the ProductGroupLookup target:

        >>> target = catalog.get_target("ProductGroupLookup")  # doctest: +SKIP
        >>> target["ProductGroupStrip"] = target.str.strip("M")  # doctest: +SKIP
        """
        return super().strip(to_strip=to_strip)  # type: ignore[return-value]

    def lstrip(self, to_strip: Optional[str] = None) -> Target:
        """
        Removes leading characters (whitespaces by default) from each string element.

        Parameters
        ----------
        to_strip: Optional[str]
            Characters to remove, whitespace is used by default.

        Returns
        -------
        Target
            A new Target object.

        Examples
        --------
        Remove leading "M" characters from the ProductGroupLookup target:

        >>> target = catalog.get_target("ProductGroupLookup")  # doctest: +SKIP
        >>> target["ProductGroupStrip"] = target.str.lstrip("M")  # doctest: +SKIP
        """
        return super().lstrip(to_strip=to_strip)  # type: ignore[return-value]

    def rstrip(self, to_strip: Optional[str] = None) -> Target:
        """
        Removes leading characters (whitespaces by default) from each string element.

        Parameters
        ----------
        to_strip: Optional[str]
            Characters to remove, whitespace is used by default.

        Returns
        -------
        Target
            A new Target object.

        Examples
        --------
        Remove trailing "M" characters from the ProductGroupLookup target:

        >>> target = catalog.get_target("ProductGroupLookup")  # doctest: +SKIP
        >>> target["ProductGroupStrip"] = target.str.rstrip(".")  # doctest: +SKIP
        """
        return super().rstrip(to_strip=to_strip)  # type: ignore[return-value]

    def replace(self, pat: str, repl: str) -> Target:
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
        Target
            A new Target object.

        Examples
        --------
        Replace "Windows" with "Win" in the ProductGroupLookup target:

        >>> target = catalog.get_target("ProductGroupLookup")  # doctest: +SKIP
        >>> target["ProductGroupNew"] = target.str.replace("Windows", "Win")  # doctest: +SKIP
        """
        return super().replace(pat=pat, repl=repl)  # type: ignore[return-value]

    def pad(self, width: int, side: Side = "left", fillchar: str = " ") -> Target:
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
        Target
            A new Target object.

        Examples
        --------
        Pad the ProductGroupLookup target to 10 characters:

        >>> target = catalog.get_target("ProductGroupLookup")  # doctest: +SKIP
        >>> target["ProductGroupLookupPadded"] = target.str.pad(10, fillchar="0")  # doctest: +SKIP
        """
        return super().pad(width=width, side=side, fillchar=fillchar)  # type: ignore[return-value]

    def contains(self, pat: str, case: bool = True) -> Target:
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
        Target
            A new Target object.

        Examples
        --------

        Check whether the ProductGroupLookup target contains "x64":

        >>> target = catalog.get_target("ProductGroupLookup")  # doctest: +SKIP
        >>> target["ProductGroup_x64"] = target.str.contains("x64")  # doctest: +SKIP
        """
        return super().contains(pat=pat, case=case)  # type: ignore[return-value]

    def slice(
        self, start: Optional[int] = None, stop: Optional[int] = None, step: Optional[int] = None
    ) -> Target:
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
        Target
            A new Target object.

        Raises
        ------
        ValueError
            When the step size is neither None nor 1.

        Examples
        --------
        Slice the first 10 characters from the ProductGroupLookup target:

        >>> target = catalog.get_target("ProductGroupLookup")  # doctest: +SKIP
        >>> target["ProductGroupSlice"] = target.str.slice(0, 10)  # doctest: +SKIP

        # noqa: DAR402
        """
        return super().slice(start=start, stop=stop, step=step)  # type: ignore[return-value]
