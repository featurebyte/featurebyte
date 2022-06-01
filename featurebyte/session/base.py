"""
Session class
"""
from __future__ import annotations

from typing import Dict

from abc import ABC, abstractmethod
from dataclasses import dataclass, field

import pandas as pd

from featurebyte.enum import DBVarType
from featurebyte.session.enum import SourceType

TableName = str
TableSchema = Dict[str, DBVarType]


class _AbstractSession(ABC):
    """
    Abstract session class to extract data warehouse table metadata & execute query
    """

    @abstractmethod
    def populate_database_metadata(self) -> dict[TableName, TableSchema]:
        """
        Extract database table schema info and store it to the database metadata

        Returns
        -------
        dict[TableName, TableSchema]
            database metadata dictionary which all table schema info
        """

    @abstractmethod
    def execute_query(self, query: str) -> pd.DataFrame:
        """
        Execute SQL query

        Returns
        -------
        pd.DataFrame
            return pandas DataFrame if the query expect output
        """


@dataclass
class _SessionDataclassMixin:
    """
    Data class mixin
    """

    source_type: SourceType = field(init=False)
    database_metadata: dict[TableName, TableSchema] = field(init=False)

    def __post_init__(self) -> None:
        self.database_metadata = self.populate_database_metadata()

    def populate_database_metadata(self) -> dict[TableName, TableSchema]:
        """
        Extract database table schema info and store it to the database metadata

        Returns
        -------
        dict[TableName, TableSchema]
            database metadata dictionary which all table schema info
        """
        raise NotImplementedError


class AbstractSession(_SessionDataclassMixin, _AbstractSession):
    """
    Abstract session class to extract data warehouse table metadata & execute query
    """
