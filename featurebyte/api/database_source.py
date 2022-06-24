"""
DatabaseSource class
"""
from __future__ import annotations

from featurebyte.api.database_table import DatabaseTable
from featurebyte.config import Credentials
from featurebyte.core.generic import ExtendedDatabaseSourceModel
from featurebyte.models.event_data import DatabaseSourceModel


class DatabaseSource(ExtendedDatabaseSourceModel):
    """
    DatabaseSource class
    """

    def list_tables(self, credentials: Credentials | None = None) -> list[str]:
        """
        List tables of the data source

        Parameters
        ----------
        credentials: Credentials
            configuration contains data source settings & credentials

        Returns
        -------
        list tables
        """
        return self.get_session(credentials=credentials).list_tables()

    def __getitem__(self, item: str | tuple[str, Credentials]) -> DatabaseTable:
        """
        Get table from the data source

        Parameters
        ----------
        item: str | tuple[str, Credentials]
            Table name or (table name, credentials) pair

        Returns
        -------
        DatabaseTable

        Raises
        ------
        TypeError
            When the item type does not support

        """
        table_name, credentials = "", None
        if isinstance(item, str):
            table_name = item
        elif isinstance(item, tuple):
            table_name, credentials = item
        else:
            raise TypeError(f"DatabaseSource indexing with value '{item}' not supported!")

        return DatabaseTable(
            tabular_source=(DatabaseSourceModel(**self.dict()), table_name),
            credentials=credentials,
        )
