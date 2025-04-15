"""
Catalog decorator.
"""

from functools import wraps
from typing import Any

from featurebyte.common import activate_catalog, get_active_catalog_id
from featurebyte.logging import get_logger

logger = get_logger(__name__)


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
        # Log when the active catalog is not the same as the catalog of the object being queried from.
        # This is done to try to prevent common user error where users view objects from a catalog, thinking that
        # it is the current active catalog.
        # We check for None because we some of our setup scripts call a bunch of functions before a catalog is active
        # to do some cleanup. We don't want to log a warning in that case as it's noisy.
        if active_catalog_id is not None:
            logger.info(
                f"NOTE: the data you are looking at is _not_ from the current active catalog (id: {active_catalog_id})"
            )
        # Activate catalog of object
        activate_catalog(self.id)
        try:
            return func(self, *args, **kwargs)
        finally:
            # Reset catalog back to original state
            activate_catalog(active_catalog_id)

    return wrapper
