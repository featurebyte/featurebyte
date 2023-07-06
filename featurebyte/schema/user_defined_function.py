"""
UserDefinedFunction API payload schema
"""
from typing import List, Optional

from bson import ObjectId
from pydantic import Field, StrictStr, validator

from featurebyte.enum import DBVarType
from featurebyte.models.base import FeatureByteBaseModel, PydanticObjectId
from featurebyte.models.user_defined_function import FunctionParameter, UserDefinedFunctionModel
from featurebyte.query_graph.node.validator import construct_unique_name_validator
from featurebyte.schema.common.base import BaseDocumentServiceUpdateSchema, PaginationMixin


class UserDefinedFunctionCreate(FeatureByteBaseModel):
    """
    UserDefinedFunction creation schema
    """

    id: Optional[PydanticObjectId] = Field(default_factory=ObjectId, alias="_id")
    name: StrictStr
    sql_function_name: StrictStr
    function_parameters: List[FunctionParameter]
    output_dtype: DBVarType
    catalog_id: Optional[PydanticObjectId]
    feature_store_id: PydanticObjectId

    # pydanctic validator
    _validate_unique_function_parameter_name = validator("function_parameters", allow_reuse=True)(
        construct_unique_name_validator(field="name")
    )


class UserDefinedFunctionUpdate(FeatureByteBaseModel):
    """
    UserDefinedFunction update schema
    """

    sql_function_name: Optional[StrictStr]
    function_parameters: Optional[List[FunctionParameter]]
    output_dtype: Optional[DBVarType]

    # pydanctic validator
    _validate_unique_function_parameter_name = validator("function_parameters", allow_reuse=True)(
        construct_unique_name_validator(field="name")
    )


class UserDefinedFunctionServiceUpdate(UserDefinedFunctionUpdate, BaseDocumentServiceUpdateSchema):
    """
    UserDefinedFunction service update schema
    """

    signature: StrictStr


class UserDefinedFunctionList(PaginationMixin):
    """
    Paginated list of UserDefinedFunction
    """

    data: List[UserDefinedFunctionModel]
