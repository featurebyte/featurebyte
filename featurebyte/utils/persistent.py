"""
Utility functions for persistent storage
"""
from __future__ import annotations

from featurebyte.config import Configurations
from featurebyte.persistent import GitDB, Persistent

PERSISTENT = None


def get_persistent() -> Persistent:
    """
    Return global Persistent object

    Returns
    -------
    Persistent
        Persistent object

    Raises
    ------
    ValueError
        Git configurations not available
    """
    global PERSISTENT  # pylint: disable=global-statement
    if not PERSISTENT:
        config = Configurations()
        if not config.git:
            raise ValueError("Git settings not available in configurations")
        git_db = GitDB(**config.git.dict())
        PERSISTENT = git_db
    return PERSISTENT


def cleanup_persistent(signum, frame):  # type: ignore
    """
    Clean up GitDB persistent

    Parameters
    ----------
    signum : int
        Signal number
    frame : frame
        Frame object
    """
    _ = signum, frame
    if PERSISTENT is not None and isinstance(PERSISTENT, GitDB):
        PERSISTENT.cleanup()
