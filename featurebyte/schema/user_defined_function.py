"""
UserDefinedFunction API payload schema
"""

from typing import List, Optional

from bson import ObjectId
from pydantic import Field, StrictStr, field_validator, model_validator

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
    _validate_unique_function_parameter_name = field_validator("function_parameters")(
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

    catalog_id: Optional[PydanticObjectId] = Field(default=None)
    feature_store_id: PydanticObjectId


class UserDefinedFunctionUpdate(FeatureByteBaseModel):
    """
    UserDefinedFunction update schema
    """

    sql_function_name: Optional[NameStr] = Field(default=None)
    function_parameters: Optional[List[FunctionParameter]] = Field(default=None)
    output_dtype: Optional[DBVarType] = Field(default=None)

    # pydanctic validator
    _validate_unique_function_parameter_name = field_validator("function_parameters")(
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

    @model_validator(mode="after")
    def _derive_is_global(self) -> "UserDefinedFunctionResponse":
        # assign to __dict__ to avoid infinite recursion due to model_validator(mode="after") call with
        # validate_assign=True in model_config.
        self.__dict__["is_global"] = self.catalog_id is None
        return self


class UserDefinedFunctionList(PaginationMixin):
    """
    Paginated list of UserDefinedFunction
    """

    data: List[UserDefinedFunctionResponse]
