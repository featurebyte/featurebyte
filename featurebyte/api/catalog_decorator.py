"""
Catalog decorator.
"""
from typing import Any, Callable

from functools import wraps

import pandas as pd

from featurebyte.models.base import activate_catalog, get_active_catalog_id


def update_and_reset_catalog(func: Any) -> Any:
    """
    Decorator to update the catalog and reset it back to original state if needed.

    If the calling catalog object has the same ID as the global state, we will just call the function that is being
    decorated.
    If not, this decorator will temporarily update the global catalog state to the catalog_id of the calling catalog
    object, call the decorated function, and then reset the state back.

    This is useful as an intermediate state for us to support a catalog object oriented syntax, while still maintaining
    a global state for the catalog ID at the implementation level.

    Parameters
    ----------
    func: Any
        Function to decorate

    Returns
    -------
    Any
    """

    @wraps(func)
    def wrapper(self: Any, *args: Any, **kwargs: Any) -> Any:
        active_catalog_id = get_active_catalog_id()
        # If the catalog is already active, just call the function
        if self.id == active_catalog_id:
            return func(self, *args, **kwargs)
        # Activate catalog of object
        activate_catalog(self.id)
        try:
            return func(self, *args, **kwargs)
        finally:
            # Reset catalog back to original state
            activate_catalog(active_catalog_id)

    return wrapper


def _to_json_serializable_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    # convert dataframe to json and then back to dataframe to ensure that
    # the dataframe is JSON serializable
    output_json = df.to_json(default_handler=str)
    return pd.read_json(output_json)


def json_serializable_dataframe(func: Callable[..., pd.DataFrame]) -> Callable[..., pd.DataFrame]:
    """
    Decorator to convert the return dataframe of a function to a JSON serializable dataframe.

    Parameters
    ----------
    func: Callable[..., pd.DataFrame]
        Function to decorate

    Returns
    -------
    Callable[..., pd.DataFrame]
    """

    @wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> pd.DataFrame:
        output = func(*args, **kwargs)
        return _to_json_serializable_dataframe(output)

    return wrapper
