"""
This module contains utility functions related to documentation
"""
from typing import List, Optional

from pydantic import BaseModel, Field

COMMON_SKIPPED_ATTRIBUTES = [
    "Config",
    "Settings",
    "update_forward_refs",
    "unique_constraints",
    "parse_file",
    "parse_obj",
    "parse_raw",
    "post_async_task",
    "from_orm",
    "from_persistent_object_dict",
    "construct",
    "collection_name",
    "schema",
    "schema_json",
    "update",
    "copy",
    "validate",
    "validate_id",
    "validate_column_exists",
    "set_parent",
    "parent",
    "json",
    "json_dict",
    "dict",
    "binary_op_series_params",
    "extract_pruned_graph_and_node",
    "unary_op_series_params",
    "node_types_lineage",
    "pytype_dbtype_map",
    "node",
    "column_var_type_map",
    "inherited_columns",
    "protected_attributes",
    "protected_columns",
]


class FBAutoDoc(BaseModel):
    """
    FeatureByte Auto Documentation parameters
    """

    skipped_members: List[str] = Field(default=COMMON_SKIPPED_ATTRIBUTES)
    proxy_class: Optional[str] = Field(default=None)
    # Setting this to True will skip the rendering of the parameters in the documentation.
    # This is typically used for class level parameters that should not be initialized directly, compared to say a
    # dataclass object.
    skip_params_in_docs: bool = Field(default=False)
