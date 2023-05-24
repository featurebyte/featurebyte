"""
Utility module
"""
import inspect
from http import HTTPStatus

import pandas as pd

from featurebyte.api.catalog import Catalog
from featurebyte.api.feature import Feature
from featurebyte.api.feature_group import BaseFeatureGroup
from featurebyte.config import Configurations


def list_unsaved_features() -> pd.DataFrame:
    """
    Lists all unsaved features in the current session.

    Returns
    -------
    pd.DataFrame
        List of unsaved features.

    Examples
    --------
    >>> customer_gender = catalog.get_view("GROCERYCUSTOMER")["Gender"].as_feature(
    ...     feature_name="Customer Gender"
    ... )
    >>> fb.list_unsaved_features()[["variable_name", "name", "catalog", "active_catalog"]]
         variable_name             name  catalog  active_catalog
    0  customer_gender  Customer Gender  grocery            True
    """
    processed_variables = set()
    unsaved_features = []
    client = Configurations().get_client()

    def _is_saved(feature: Feature) -> bool:
        """
        Check if a feature is saved.

        Parameters
        ----------
        feature: Feature
            Feature to check

        Returns
        -------
        bool
        """
        response = client.get(
            url=f"/feature/{feature.id}", headers={"active-catalog-id": str(feature.catalog_id)}
        )
        if response.status_code == HTTPStatus.OK:
            return True
        return False

    # get list of frame info from the current call stack
    call_stack = inspect.stack()
    # skip the first frame, which is the current function, to get the caller's frame
    caller_frame_info = call_stack[1]
    # check caller's local variables first, then global variables for unsaved features
    caller_variables = [caller_frame_info.frame.f_locals, caller_frame_info.frame.f_globals]
    for variables in caller_variables:
        for var_name, var_obj in variables.items():
            # global variable may be overriden by local variable
            if var_name in processed_variables:
                continue
            if isinstance(var_obj, Feature) and not _is_saved(var_obj):
                unsaved_features.append(
                    {
                        "object_id": str(var_obj.id),
                        "variable_name": var_name,
                        "name": var_obj.name,
                        "catalog_id": var_obj.catalog_id,
                    }
                )
            elif isinstance(var_obj, BaseFeatureGroup):
                for name, feature in var_obj.feature_objects.items():
                    if not _is_saved(feature):
                        unsaved_features.append(
                            {
                                "object_id": str(feature.id),
                                "variable_name": f'{var_name}["{name}"]',
                                "name": feature.name,
                                "catalog_id": feature.catalog_id,
                            }
                        )
            processed_variables.add(var_name)

    if unsaved_features:
        catalogs = Catalog.list(include_id=True)
        return (
            pd.DataFrame(unsaved_features)
            .merge(
                catalogs.rename({"name": "catalog", "active": "active_catalog"}, axis=1),
                left_on="catalog_id",
                right_on="id",
                how="left",
            )[["object_id", "variable_name", "name", "catalog", "active_catalog"]]
            .sort_values("object_id")
            .reset_index(drop=True)
        )
    return pd.DataFrame(columns=["object_id", "variable_name", "name", "catalog", "active_catalog"])
