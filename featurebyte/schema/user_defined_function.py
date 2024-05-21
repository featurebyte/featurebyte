"""
UserDefinedFunction API payload schema
"""

from typing import Any, Dict, List, Optional

from bson import ObjectId
from pydantic import Field, StrictStr, root_validator, validator

from featurebyte.enum import DBVarType
from featurebyte.models.base import FeatureByteBaseModel, NameStr, PydanticObjectId
from featurebyte.models.user_defined_function import FunctionParameter, UserDefinedFunctionModel
from featurebyte.query_graph.node.validator import construct_unique_name_validator
from featurebyte.schema.common.base import BaseDocumentServiceUpdateSchema, PaginationMixin


class UserDefinedFunctionCreateBase(FeatureByteBaseModel):
    """
    UserDefinedFunction creation schema
    """

    id: Optional[PydanticObjectId] = Field(default_factory=ObjectId, alias="_id")
    name: NameStr
    description: Optional[StrictStr] = Field(default=None)
    sql_function_name: NameStr
    function_parameters: List[FunctionParameter]
    output_dtype: DBVarType

    # pydantic validator
    _validate_unique_function_parameter_name = validator("function_parameters", allow_reuse=True)(
        construct_unique_name_validator(field="name")
    )


class UserDefinedFunctionCreate(UserDefinedFunctionCreateBase):
    """
    UserDefinedFunction creation schema
    """

    is_global: bool = Field(default=False)


class UserDefinedFunctionServiceCreate(UserDefinedFunctionCreateBase):
    """
    UserDefinedFunction service creation schema
    """

    catalog_id: Optional[PydanticObjectId]
    feature_store_id: PydanticObjectId


class UserDefinedFunctionUpdate(FeatureByteBaseModel):
    """
    UserDefinedFunction update schema
    """

    sql_function_name: Optional[NameStr]
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


class UserDefinedFunctionResponse(UserDefinedFunctionModel):
    """
    UserDefinedFunction response schema
    """

    is_global: bool = Field(default=False)

    @root_validator(pre=True)
    @classmethod
    def _derive_is_global(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        values["is_global"] = values.get("catalog_id") is None
        return values


class UserDefinedFunctionList(PaginationMixin):
    """
    Paginated list of UserDefinedFunction
    """

    data: List[UserDefinedFunctionResponse]
