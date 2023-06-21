"""
UserDefinedFunction API payload schema
"""
from typing import List, Optional

from bson import ObjectId
from pydantic import Field, StrictStr

from featurebyte.enum import DBVarType
from featurebyte.models.base import FeatureByteBaseModel, PydanticObjectId
from featurebyte.models.user_defined_function import FunctionParameter, UserDefinedFunctionModel
from featurebyte.schema.common.base import PaginationMixin


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


class UserDefinedFunctionUpdate(FeatureByteBaseModel):
    """
    UserDefinedFunction update schema
    """


class UserDefinedFunctionList(PaginationMixin):
    """
    Paginated list of UserDefinedFunction
    """

    data: List[UserDefinedFunctionModel]
