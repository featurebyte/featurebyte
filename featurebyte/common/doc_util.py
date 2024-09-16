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
    "model_validate_json",
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
    "extract_pruned_graph_and_node",
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
    # Setting this to True will skip the rendering of the parameters, and the signature, in the documentation for the
    # class. This is typically used for class level parameters that should not be initialized directly, compared to say
    # a dataclass object.
    # For example, the Feature class should not be initialized directly. The documentation for the Feature class
    # should not show the constructor signature, and should have links in the See Also section to point users to the
    # factory methods.
    skip_params_and_signature_in_class_docs: bool = Field(default=False)

    # Setting this to True will skip the rendering of the keyword only params in the signature in the documentation
    # for the class.
    # For example, if a class has a constructor like
    #   def __init__(self, a, b, *, c, d)
    # then setting this to True will skip the rendering of c and d in the signature.
    hide_keyword_only_params_in_class_docs: bool = Field(default=False)
