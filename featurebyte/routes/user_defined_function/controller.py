"""
UserDefinedFunction API route controller
"""
from __future__ import annotations

from featurebyte.models.user_defined_function import UserDefinedFunctionModel
from featurebyte.routes.common.base import BaseDocumentController
from featurebyte.schema.user_defined_function import (
    UserDefinedFunctionCreate,
    UserDefinedFunctionList,
)
from featurebyte.service.user_defined_function import UserDefinedFunctionService


class UserDefinedFunctionController(
    BaseDocumentController[
        UserDefinedFunctionModel, UserDefinedFunctionService, UserDefinedFunctionList
    ]
):
    """
    UserDefinedFunctionController class
    """

    paginated_document_class = UserDefinedFunctionList

    async def create_user_defined_function(
        self,
        data: UserDefinedFunctionCreate,
    ) -> UserDefinedFunctionModel:
        """
        Create UserDefinedFunction at persistent

        Parameters
        ----------
        data: UserDefinedFunctionCreate
            UserDefinedFunction creation payload

        Returns
        -------
        UserDefinedFunctionModel
            Newly created user_defined_function object
        """
        return await self.service.create_document(data)
