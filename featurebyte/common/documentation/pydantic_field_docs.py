"""
Docstrings for pydantic field overrides for a class.
"""

from typing import Dict

CATALOG_ID = "catalog_id"
CREATED_AT = "created_at"
DTYPE = "dtype"
ENTITY_IDS = "entity_ids"
FEATURE_STORE = "feature_store"
ID = "id"
NAME = "name"
UPDATED_AT = "updated_at"


def _get_created_at_docstring_override(class_name: str) -> str:
    return f"Returns the timestamp indicating when the {class_name} object was created."


def _get_id_docstring_override(class_name: str) -> str:
    return f"Returns the unique identifier (ID) of the {class_name} object."


def _get_name_docstring_override(class_name: str) -> str:
    return f"Returns the name of the {class_name} object."


def _get_updated_at_docstring_override(class_name: str) -> str:
    return f"Returns the timestamp indicating when the {class_name} object was last updated."


def _get_catalog_id_docstring_override(class_name: str) -> str:
    return f"Returns the unique identifier (ID) of the Catalog that is associated with the {class_name} object."


def _get_dtype_docstring_override(class_name: str) -> str:
    return f"Returns the data type of the {class_name}."


def _get_doc_overrides(class_name: str) -> Dict[str, str]:
    """
    Get the docstring overrides for a class.

    Parameters
    ----------
    class_name : str
        The name of the class.

    Returns
    -------
    Dict[str, str]
        The docstring overrides for the class.
    """
    return {
        CREATED_AT: _get_created_at_docstring_override(class_name),
        ID: _get_id_docstring_override(class_name),
        NAME: _get_name_docstring_override(class_name),
        UPDATED_AT: _get_updated_at_docstring_override(class_name),
    }


def _get_target_or_feature_overrides(class_name: str) -> Dict[str, str]:
    """
    Get the docstring overrides for a Target or Feature class.

    Parameters
    ----------
    class_name : str
        The name of the class.

    Returns
    -------
    Dict[str, str]
        The docstring overrides for the class.
    """
    return {
        **_get_doc_overrides(class_name),
        CATALOG_ID: _get_catalog_id_docstring_override(class_name),
        DTYPE: _get_dtype_docstring_override(class_name),
        ENTITY_IDS: f"Returns a list of entity IDs that are linked to the {class_name}.",
    }


pydantic_field_doc_overrides = {
    "BatchFeatureTable": _get_doc_overrides("BatchFeatureTable"),
    "BatchRequestTable": _get_doc_overrides("BatchRequestTable"),
    "Catalog": _get_doc_overrides("Catalog"),
    "Context": _get_doc_overrides("Context"),
    "DisguisedValueImputation": {
        "imputed_value": "The value that will be used to replace any element that matches one of the values in the "
        "disguised_values list.",
    },
    "Entity": _get_doc_overrides("Entity"),
    "Target": _get_target_or_feature_overrides("Target"),
    "Feature": {
        **_get_target_or_feature_overrides("Feature"),
        "feature_list_ids": "Returns a list of IDs of feature lists that include the specified feature version.",
        "feature_namespace_id": "Returns the unique identifier (ID) of the feature namespace of the specified feature. "
        "All feature versions of a feature have the same feature namespace ID.",
        NAME: "The 'name' property of a Feature object serves to identify the feature namespace and can be used to "
        "retrieve or set its name. To save a Feature object derived from other Feature objects to a catalog, a "
        "unique name must be assigned to it using the property before saving it. The namespace of Lookup "
        "Features or Aggregate Features are defined during their declaration. The property can be used to "
        "modify it before saving it. Once the feature is saved, its name cannot be changed. Therefore, it is "
        "essential to give the Feature object a descriptive and meaningful name before saving it.",
    },
    "FeatureList": {
        **_get_doc_overrides("FeatureList"),
        CATALOG_ID: _get_catalog_id_docstring_override("FeatureList"),
    },
    "FeatureStore": _get_doc_overrides("FeatureStore"),
    "HistoricalFeatureTable": _get_doc_overrides("HistoricalFeatureTable"),
    "MissingValueImputation": {"imputed_value": "Value to fill missing values."},
    "ObservationTable": _get_doc_overrides("ObservationTable"),
    "Relationship": _get_doc_overrides("Relationship"),
    "StringValueImputation": {
        "imputed_value": "The value that will be used to replace any string values.",
    },
    "Table": {
        ID: _get_id_docstring_override("Table"),
        CREATED_AT: _get_created_at_docstring_override("Table"),
        UPDATED_AT: _get_updated_at_docstring_override("Table"),
    },
    "TableApiObject": {
        FEATURE_STORE: "Provides information about the feature store that the table is connected to.",
        NAME: _get_name_docstring_override("Table"),
        UPDATED_AT: _get_updated_at_docstring_override("Table"),
        "type": "Returns the Table object's type, which may be one of the following: Event Table, Item Table, Slowly "
        "Changing Dimension Table (SCD), or Dimension Table.",
    },
    "TableColumn": {
        NAME: _get_name_docstring_override("TableColumn"),
    },
    "View": {
        "columns_info": "Returns a list of columns specifications. This includes the id of the Entity objects tagged "
        "in the table and the default cleaning operations set for each column in the table.",
        "feature_store": "Provides information about the feature store that the view is connected to.",
    },
    "UseCase": _get_doc_overrides("UseCase"),
    "UnexpectedValueImputation": {
        "imputed_value": "The value that will be used to replace any value that does not match the expected values "
        "in the expected_values list.",
    },
    "ValueBeyondEndpointImputation": {
        "imputed_value": "The value that will be used to replace any value that falls outside a specified range.",
    },
    "ViewColumn": {
        DTYPE: _get_dtype_docstring_override("ViewColumn"),
        FEATURE_STORE: "Provides information about the feature store that the view column is connected to.",
        NAME: _get_name_docstring_override("ViewColumn"),
    },
    "UserDefinedFunction": _get_doc_overrides("UserDefinedFunction"),
}
