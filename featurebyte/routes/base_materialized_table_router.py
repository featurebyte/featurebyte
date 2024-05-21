"""
Base materialized table router
"""

from typing import Generic, Type, TypeVar

from fastapi import APIRouter, Request

from featurebyte.models.base import PydanticObjectId
from featurebyte.models.materialized_table import MaterializedTableModel
from featurebyte.routes.base_router import BaseRouter

MaterializedTableModelT = TypeVar("MaterializedTableModelT", bound=MaterializedTableModel)


class BaseMaterializedTableRouter(Generic[MaterializedTableModelT], BaseRouter):
    """
    Base materialized table router.

    This class contains all the routes that are common between base materialized tables.
    """

    table_model: Type[MaterializedTableModelT]
    controller: str

    def __init__(self, prefix: str):
        super().__init__(router=APIRouter(prefix=prefix))
        base_name = prefix.lstrip("/")
        api_id = f"{{{base_name}_id}}"
        self.router.add_api_route(
            f"/{api_id}",
            self.get_table,
            methods=["GET"],
            response_model=self.table_model,
        )

    async def get_table(
        self, request: Request, table_id: PydanticObjectId
    ) -> MaterializedTableModelT:
        """
        Get table
        """
        controller = request.state.app_container.get(self.controller)
        table: MaterializedTableModelT = await controller.get(document_id=table_id)
        return table
