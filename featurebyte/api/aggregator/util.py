"""
Aggregator utility functions
"""

from typing import Optional

import warnings

from featurebyte.typing import OptionalScalar


def conditional_set_skip_fill_na(skip_fill_na: Optional[bool], fill_value: OptionalScalar) -> bool:
    """
    Set skip_fill_na if not provided

    Parameters
    ----------
    skip_fill_na: Optional[bool]
        Whether to skip filling NaN values
    fill_value: OptionalScalar
        Value to fill if the value in the column is empty

    Returns
    -------
    bool
    """
    if skip_fill_na is not None or fill_value is not None:
        warnings.warn(
            "The parameters 'skip_fill_na' and 'fill_value' are deprecated and will be removed in a future version.",
            DeprecationWarning,
        )

    if skip_fill_na is None:
        return fill_value is None
    return skip_fill_na
