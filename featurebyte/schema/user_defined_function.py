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
    function_name: StrictStr
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

    function_parameters: List[FunctionParameter]

    # pydanctic validator
    _validate_unique_function_parameter_name = validator("function_parameters", allow_reuse=True)(
        construct_unique_name_validator(field="name")
    )


class UserDefinedFunctionServiceUpdate(BaseDocumentServiceUpdateSchema):
    """
    UserDefinedFunction service update schema
    """

    function_parameters: List[FunctionParameter]


class UserDefinedFunctionList(PaginationMixin):
    """
    Paginated list of UserDefinedFunction
    """

    data: List[UserDefinedFunctionModel]
