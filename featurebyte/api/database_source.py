"""
DatabaseSource class
"""
from __future__ import annotations

from featurebyte.api.database_table import DatabaseTable
from featurebyte.config import Credentials
from featurebyte.core.generic import ExtendedDatabaseSourceModel
from featurebyte.models.database_source import DatabaseSourceModel, TableDetails


class DatabaseSource(ExtendedDatabaseSourceModel):
    """
    DatabaseSource class
    """

    def list_databases(self, credentials: Credentials | None = None) -> list[str]:
        """
        List databases of the data source

        Parameters
        ----------
        credentials: Credentials
            configuration contains data source settings & credentials

        Returns
        -------
        list databases
        """
        return self.get_session(credentials=credentials).list_databases()

    def list_schemas(
        self, database_name: str | None = None, credentials: Credentials | None = None
    ) -> list[str]:
        """
        List schemas of the data source

        Parameters
        ----------
        database_name: str | None
            Database name
        credentials: Credentials
            configuration contains data source settings & credentials


        Returns
        -------
        list schemas
        """
        return self.get_session(credentials=credentials).list_schemas(database_name=database_name)

    def list_tables(
        self,
        database_name: str | None = None,
        schema_name: str | None = None,
        credentials: Credentials | None = None,
    ) -> list[str]:
        """
        List tables of the data source

        Parameters
        ----------
        database_name: str | None
            Database name
        schema_name: str | None
            Schema name
        credentials: Credentials
            configuration contains data source settings & credentials

        Returns
        -------
        list tables
        """
        return self.get_session(credentials=credentials).list_tables(
            database_name=database_name, schema_name=schema_name
        )

    def get_table(
        self,
        database_name: str | None,
        schema_name: str | None,
        table_name: str,
        credentials: Credentials | None = None,
    ) -> DatabaseTable:
        """
        Get table from the data source

        Parameters
        ----------
        database_name: str | None
            Database name
        schema_name: str | None
            Schema name
        table_name: str
            Table name
        credentials: Credentials
            configuration contains data source settings & credentials

        Returns
        -------
        DatabaseTable
        """
        return DatabaseTable(
            tabular_source=(
                DatabaseSourceModel(**self.dict()),
                TableDetails(
                    database_name=database_name,
                    schema_name=schema_name,
                    table_name=table_name,
                ),
            ),
            credentials=credentials,
        )
