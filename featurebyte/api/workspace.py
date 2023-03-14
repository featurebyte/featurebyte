"""
Workspace class
"""
from __future__ import annotations

from typing import Any

import pandas as pd
from typeguard import typechecked

from featurebyte.api.api_object import SavableApiObject
from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.config import activate_workspace, get_active_workspace_id
from featurebyte.logger import logger
from featurebyte.models.workspace import WorkspaceModel
from featurebyte.schema.workspace import WorkspaceCreate, WorkspaceUpdate


class Workspace(WorkspaceModel, SavableApiObject):
    """
    Workspace class
    """

    # documentation metadata
    __fbautodoc__ = FBAutoDoc(section=["Workspace"], proxy_class="featurebyte.Workspace")

    # class variables
    _route = "/workspace"
    _update_schema_class = WorkspaceUpdate
    _list_schema = WorkspaceModel
    _get_schema = WorkspaceModel
    _list_fields = ["name", "created_at", "active"]

    def _get_create_payload(self) -> dict[str, Any]:
        data = WorkspaceCreate(**self.json_dict())
        return data.json_dict()

    @classmethod
    def activate_workspace(cls, name: str) -> None:
        """
        Activate workspace by name

        Parameters
        ----------
        name: str
            Name of workspace to activate
        """
        cls.get(name).activate()

    @classmethod
    def create(
        cls,
        name: str,
    ) -> Workspace:
        """
        Create and activate workspace

        Parameters
        ----------
        name: str
            feature store name

        Returns
        -------
        Workspace

        Examples
        --------
        Create a new workspace

        >>> from featurebyte import Workspace
        >>> Workspace.create(  # doctest: +SKIP
        ...     name="My Workspace"
        ... )

        List workspaces
        >>> Workspace.list()  # doctest: +SKIP
                                  id	             name	             created_at	active
        0	63ef2ca50523266031b728dd	     My Workspace	2023-02-17 07:28:37.368   True
        1	63eda344d0313fb925f7883a	          default	2023-02-17 07:03:26.267	 False
        """
        workspace = Workspace(name=name)
        workspace.save()
        workspace.activate()
        return workspace

    @classmethod
    def get_active(cls) -> Workspace:
        """
        Get active workspace

        Returns
        -------
        Workspace
        """
        return cls.get_by_id(get_active_workspace_id())

    @classmethod
    def _post_process_list(cls, item_list: pd.DataFrame) -> pd.DataFrame:
        item_list = super()._post_process_list(item_list)

        # add column to indicate whether workspace is active
        item_list["active"] = item_list.id == get_active_workspace_id()

        return item_list

    def activate(self) -> None:
        """
        Activate workspace
        """
        activate_workspace(self.id)
        logger.debug(f"Current workspace is now: {self.name}")

    @typechecked
    def update_name(self, name: str) -> None:
        """
        Change entity name

        Parameters
        ----------
        name: str
            New entity name
        """
        self.update(update_payload={"name": name}, allow_update_local=True)

    @property
    def name_history(self) -> list[dict[str, Any]]:
        """
        List of name history entries

        Returns
        -------
        list[dict[str, Any]]
        """
        return self._get_audit_history(field_name="name")
