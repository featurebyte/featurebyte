"""
Session class
"""
from __future__ import annotations

from typing import Any, Dict

from dataclasses import dataclass, field

import pandas as pd

from featurebyte.enum import DBVarType
from featurebyte.session.enum import SourceType

TableName = str
TableSchema = Dict[str, DBVarType]


@dataclass
class BaseSession:
    """
    Abstract session class to extract data warehouse table metadata & execute query
    """

    source_type: SourceType = field(init=False)
    database_metadata: dict[TableName, TableSchema] = field(init=False)
    _connection: Any = field(default=None, init=False)

    def __post_init__(self) -> None:
        if self._connection is None:
            raise ConnectionError("Failed to established a database connection.")
        self.database_metadata = self.populate_database_metadata()

    def populate_database_metadata(self) -> dict[TableName, TableSchema]:
        """
        Extract database table schema info and store it to the database metadata

        Raises
        ------
        NotImplementedError
            if the child class not implement this method
        """
        raise NotImplementedError

    def execute_query(self, query: str) -> pd.DataFrame | None:
        """
        Execute SQL query

        Parameters
        ----------
        query: str
            sql query to execute

        Returns
        -------
        pd.DataFrame
            return pandas DataFrame if the query expect output
        """
        cursor = self._connection.cursor()
        try:
            cursor.execute(query)
            if cursor.description:
                all_rows = cursor.fetchall()
                columns = [row[0] for row in cursor.description]
                return pd.DataFrame(all_rows, columns=columns)
        finally:
            cursor.close()
