from __future__ import annotations

from typing import Optional

from bson import ObjectId

DEFAULT_CATALOG_ID = ObjectId("23eda344d0313fb925f7883a")
ACTIVE_CATALOG_ID: Optional[ObjectId] = None


def get_active_catalog_id() -> Optional[ObjectId]:
    """
    Get active catalog id

    Returns
    -------
    Optional[ObjectId]
    """
    return ACTIVE_CATALOG_ID


def activate_catalog(catalog_id: Optional[ObjectId]) -> None:
    """
    Set active catalog

    Parameters
    ----------
    catalog_id: Optional[ObjectId]
        Catalog ID to set as active, or None to set no active catalog
    """
    global ACTIVE_CATALOG_ID
    ACTIVE_CATALOG_ID = catalog_id
