"""
Code generation config is used to control the code generating style.
"""

from __future__ import annotations

from typing import Any, Dict, Optional

from bson import ObjectId
from pydantic import BaseModel, Field

from featurebyte.enum import DBVarType, SourceType
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
    OnDemandViewCodeGenConfig is used to control the code generating style for Feast on demand feature view.

    input_df_name: str
        Input dataframe name
    """

    input_df_name: str = Field(default="inputs")
    output_df_name: str = Field(default="df")
    on_demand_function_name: str = Field(default="on_demand_feature_view")
    source_type: SourceType


class OnDemandFunctionCodeGenConfig(BaseCodeGenConfig):
    """
    OnDemandFunctionCodeGenConfig is used to control the code generating style for DataBricks on demand function.

    input_var_prefix: str
        Input variable prefix for the function
    request_input_var_prefix: str
        Input variable prefix for the request column input
    output_dtype: DBVarType
        Output variable data type
    """

    source_type: SourceType = Field(default=SourceType.DATABRICKS)
    sql_function_name: str = Field(default="odf_func")
    sql_input_var_prefix: str = Field(default="x")
    sql_request_input_var_prefix: str = Field(default="r")
    sql_comment: str = Field(default="")
    function_name: str = Field(default="on_demand_feature_function")
    input_var_prefix: str = Field(default="col")
    request_input_var_prefix: str = Field(default="request_col")
    output_dtype: DBVarType
    to_generate_null_filling_function: bool = Field(default=False)

    @classmethod
    def to_py_type(cls, dtype: DBVarType) -> str:
        """
        Convert DBVarType to Python type

        Parameters
        ----------
        dtype: DBVarType
            Internal DBVarType

        Returns
        -------
        str
            Python type

        Raises
        ------
        ValueError
            If the input dtype is not supported
        """
        output = dtype.to_type_str()
        if output:
            return output
        if dtype in DBVarType.supported_timestamp_types():
            return "pd.Timestamp"
        if dtype == DBVarType.DATE:
            return "datetime.date"
        if dtype in DBVarType.dictionary_types():
            return "dict[str, float]"
        if dtype in DBVarType.array_types():
            return "list[float]"
        if dtype == DBVarType.TIMESTAMP_TZ_TUPLE:
            return "str"
        raise ValueError(f"Unsupported dtype: {dtype}")

    @classmethod
    def to_sql_type(cls, py_type: str) -> str:
        """
        Convert DBVarType to SQL type

        Parameters
        ----------
        py_type: str
            Python type

        Returns
        -------
        str
            SQL type

        Raises
        ------
        ValueError
            If the input py_type is not supported
        """
        mapping = {
            "bool": "BOOLEAN",
            "str": "STRING",
            "float": "DOUBLE",
            "int": "BIGINT",
            "pd.Timestamp": "TIMESTAMP",
            "datetime.date": "DATE",
            "dict[str, float]": "MAP<STRING, DOUBLE>",
            "list[float]": "ARRAY<DOUBLE>",
        }
        output = mapping.get(py_type)
        if output:
            return output
        raise ValueError(f"Unsupported py_type: {py_type}")

    @property
    def return_type(self) -> str:
        """
        Return type of the function

        Returns
        -------
        str
            Return type of the function
        """
        return self.to_py_type(self.output_dtype)

    @property
    def sql_return_type(self) -> str:
        """
        Return type of the function

        Returns
        -------
        str
            Return type of the function
        """
        return self.to_sql_type(self.return_type)

    def get_sql_param_prefix(self, py_param_name: str) -> str:
        """
        Get SQL parameter prefix based on the python parameter name

        Parameters
        ----------
        py_param_name: str
            Python parameter name

        Returns
        -------
        str
            SQL parameter prefix

        Raises
        ------
        ValueError
            If the python parameter name is not expected
        """
        if py_param_name.startswith(self.input_var_prefix):
            return self.sql_input_var_prefix
        if py_param_name.startswith(self.request_input_var_prefix):
            return self.sql_request_input_var_prefix
        raise ValueError(f"Unexpected py_param_name: {py_param_name}")


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
