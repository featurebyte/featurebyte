"""
DatabaseTable class
"""
from __future__ import annotations

from typing import Dict

import pandas as pd

from featurebyte.core.generic import QueryObject
from featurebyte.enum import DBVarType


class DatabaseTable(QueryObject):
    """
    DatabaseTable class to preview table
    """

    column_var_type_map: Dict[str, DBVarType]

    @property
    def dtypes(self) -> pd.Series:
        return pd.Series(self.column_var_type_map)
