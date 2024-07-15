"""
Feature string accessor module.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar, Optional, TypeVar

from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.core.accessor.string import StringAccessor
from featurebyte.query_graph.node.string import Side

if TYPE_CHECKING:
    from featurebyte.api.feature import Feature
else:
    Feature = TypeVar("Feature")


class FeatureStrAccessorMixin:
    """
    FeatureStrAccessorMixin class
    """

    @property
    def str(self: Feature) -> StringAccessor:  # type: ignore
        """
        str accessor object

        Returns
        -------
        StringAccessor
        """
        return FeatureStringAccessor(self)


class FeatureStringAccessor(StringAccessor):
    """
    FeatureStringAccessor class used to manipulate string type Feature objects
    """

    # class variables
    __fbautodoc__: ClassVar[FBAutoDoc] = FBAutoDoc()

    def len(self) -> Feature:
        """
        Computes length of each string element.

        Returns
        -------
        Feature
            A new Feature object.

        Examples
        --------
        Compute the length of each string element in the ProductGroupLookup feature:

        >>> feature = catalog.get_feature("ProductGroupLookup")
        >>> feature_group = fb.FeatureGroup([feature])
        >>> feature_group["ProductGroupLength"] = feature.str.len()
        """
        return super().len()  # type: ignore[return-value]

    def lower(self) -> Feature:
        """
        Converts each string element to lower case.

        Returns
        -------
        Feature
            A new Feature object.

        Examples
        --------
        Convert the ProductGroupLookup feature to lower case:

        >>> feature = catalog.get_feature("ProductGroupLookup")
        >>> feature_group = fb.FeatureGroup([feature])
        >>> feature_group["ProductGroupLower"] = feature.str.lower()
        """
        return super().lower()  # type: ignore[return-value]

    def upper(self) -> Feature:
        """
        Converts each string element to upper case.

        Returns
        -------
        Feature
            A new Feature object.

        Examples
        --------
        Convert the ProductGroupLookup feature to upper case:

        >>> feature = catalog.get_feature("ProductGroupLookup")
        >>> feature_group = fb.FeatureGroup([feature])
        >>> feature_group["ProductGroupUpper"] = feature.str.upper()
        """
        return super().upper()  # type: ignore[return-value]

    def strip(self, to_strip: Optional[str] = None) -> Feature:
        """
        Removes leading and trailing characters (whitespaces by default) from each string element.

        Parameters
        ----------
        to_strip: Optional[str]
            Characters to remove, whitespace is used by default.

        Returns
        -------
        Feature
            A new Feature object.

        Examples
        --------
        Remove leading and trailing "M" characters from the ProductGroupLookup feature:

        >>> feature = catalog.get_feature("ProductGroupLookup")
        >>> feature_group = fb.FeatureGroup([feature])
        >>> feature_group["ProductGroupStrip"] = feature.str.strip("M")
        """
        return super().strip(to_strip=to_strip)  # type: ignore[return-value]

    def lstrip(self, to_strip: Optional[str] = None) -> Feature:
        """
        Removes leading characters (whitespaces by default) from each string element.

        Parameters
        ----------
        to_strip: Optional[str]
            Characters to remove, whitespace is used by default.

        Returns
        -------
        Feature
            A new Feature object.

        Examples
        --------
        Remove leading "M" characters from the ProductGroupLookup feature:

        >>> feature = catalog.get_feature("ProductGroupLookup")
        >>> feature_group = fb.FeatureGroup([feature])
        >>> feature_group["ProductGroupStrip"] = feature.str.lstrip("M")
        """
        return super().lstrip(to_strip=to_strip)  # type: ignore[return-value]

    def rstrip(self, to_strip: Optional[str] = None) -> Feature:
        """
        Removes leading characters (whitespaces by default) from each string element.

        Parameters
        ----------
        to_strip: Optional[str]
            Characters to remove, whitespace is used by default.

        Returns
        -------
        Feature
            A new Feature object.

        Examples
        --------
        Remove trailing "M" characters from the ProductGroupLookup feature:

        >>> feature = catalog.get_feature("ProductGroupLookup")
        >>> feature_group = fb.FeatureGroup([feature])
        >>> feature_group["ProductGroupStrip"] = feature.str.rstrip(".")
        """
        return super().rstrip(to_strip=to_strip)  # type: ignore[return-value]

    def replace(self, pat: str, repl: str) -> Feature:
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
        Feature
            A new Feature object.

        Examples
        --------
        Replace "Windows" with "Win" in the ProductGroupLookup feature:

        >>> feature = catalog.get_feature("ProductGroupLookup")
        >>> feature_group = fb.FeatureGroup([feature])
        >>> feature_group["ProductGroupNew"] = feature.str.replace("Windows", "Win")
        """
        return super().replace(pat=pat, repl=repl)  # type: ignore[return-value]

    def pad(self, width: int, side: Side = "left", fillchar: str = " ") -> Feature:
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
        Feature
            A new Feature object.

        Examples
        --------
        Pad the ProductGroupLookup feature to 10 characters:

        >>> feature = catalog.get_feature("ProductGroupLookup")
        >>> feature_group = fb.FeatureGroup([feature])
        >>> feature_group["ProductGroupLookupPadded"] = feature.str.pad(10, fillchar="0")
        """
        return super().pad(width=width, side=side, fillchar=fillchar)  # type: ignore[return-value]

    def contains(self, pat: str, case: bool = True) -> Feature:
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
        Feature
            A new Feature object.

        Examples
        --------

        Check whether the ProductGroupLookup feature contains "x64":

        >>> feature = catalog.get_feature("ProductGroupLookup")
        >>> feature_group = fb.FeatureGroup([feature])
        >>> feature_group["ProductGroup_x64"] = feature.str.contains("x64")
        """
        return super().contains(pat=pat, case=case)  # type: ignore[return-value]

    def slice(
        self, start: Optional[int] = None, stop: Optional[int] = None, step: Optional[int] = None
    ) -> Feature:
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
        Feature
            A new Feature object.

        Raises
        ------
        ValueError
            When the step size is neither None nor 1.

        Examples
        --------
        Slice the first 10 characters from the ProductGroupLookup feature:

        >>> feature = catalog.get_feature("ProductGroupLookup")
        >>> feature_group = fb.FeatureGroup([feature])
        >>> feature_group["ProductGroupSlice"] = feature.str.slice(0, 10)

        # noqa: DAR402
        """
        return super().slice(start=start, stop=stop, step=step)  # type: ignore[return-value]
