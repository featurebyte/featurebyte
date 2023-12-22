"""
Code generation config is used to control the code generating style.
"""
from __future__ import annotations

from typing import Any, Dict, Optional

from bson import ObjectId
from pydantic import BaseModel, Field

from featurebyte.models.base import PydanticObjectId
from featurebyte.query_graph.node.schema import DatabaseDetails


class BaseCodeGenConfig(BaseModel):
    """
    Base class for code generation config

    max_expression_length: int
        Maximum expression length used to decide whether to assign the expression into a variable
        to reduce overall statement's line width.
    """

    max_expression_length: int = Field(default=40)


class OnDemandViewCodeGenConfig(BaseCodeGenConfig):
    """
    OnDemandViewCodeGenConfig is used to control the code generating style.

    input_df_name: str
        Input dataframe name
    """

    input_df_name: str
    output_df_name: str
    on_demand_function_name: str


class SDKCodeGenConfig(BaseCodeGenConfig):
    """
    SCKCodeGenConfig is used to control the code generating style like whether to introduce a new variable to
    store some intermediate results.

    feature_store_id: PydanticObjectId
        Feature store ID used to construct unsaved table object
    feature_store_name: str
        Feature store name used to construct unsaved table object
    table_id_to_info: Dict[PydanticObjectId, Any]
        Mapping from table ID to table info (name, record_creation_timestamp_column, etc)
    final_output_name: str
        Variable name which contains final output
    to_use_saved_data: str
        When enabled, load the table object from the persistent, otherwise construct the table from
        feature store explicitly
    """

    # values not controlled by the query graph (can be configured outside graph)
    # feature store ID & name
    feature_store_id: PydanticObjectId = Field(default_factory=ObjectId)
    feature_store_name: str = Field(default="feature_store")
    database_details: Optional[DatabaseDetails] = Field(default=None)

    # table ID to table info (name, record_creation_timestamp_column, etc)
    table_id_to_info: Dict[PydanticObjectId, Dict[str, Any]] = Field(default_factory=dict)

    # output variable name used to store the final output
    final_output_name: str = Field(default="output")

    # other configurations
    to_use_saved_data: bool = Field(default=False)
