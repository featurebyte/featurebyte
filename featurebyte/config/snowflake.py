"""
This Module acts as an interface between Snowflake and local environment.
"""
from typing import Any, Tuple

import pandas as pd
from snowflake import connector


class SnowflakeConfig:
    """
    This class implmenents both Retrieval From and Updating to the specified Snowflake database.
    """

    def __init__(
        self,
        url: str,
        user: str,
        password: str,
        database: str,
        schema: str,
        warehouse: str,
        table: str,
    ) -> None:
        """
        Instantiate a Snowflake instance.

        Parameters
        ----------
        url: str
            url of Snakeflake
        user: str
            username
        password: str
            password
        database: str
            database name
        schema: str
            schema nane
        warehouse: str
            warehouse name
        table: str
            table name
        """

        self.table = table
        self.database = database
        self.schema = schema
        self.conn = connector.connect(
            user=user,
            password=password,
            account=url,
            database=database,
            schema=schema,
            warehouse=warehouse,
        )

        self.json = {
            "database": database,
            "schema": schema,
            "table": table,
            "url": url,
            "warehouse": warehouse,
        }

    def get_table_sample_data(self, limit: int = 10) -> pd.DataFrame:
        """
        Get sample data as a dataframe from the configured table.

        Parameters
        ----------
        limit: int
            number of sample records to retrieve from the configured table

        Returns
        -------
            dataframe: Dataframe
                pandas Dataframe for sample records

        """
        sql = "select * from table(%s) limit %s"
        params = (self.table, limit)
        dataframe = self._execute_df(sql, params)
        dataframe.columns = map(str.lower, dataframe.columns)
        return dataframe

    def get_table_metadata(self) -> pd.DataFrame:
        """
        Get the metadata as a dataframe of Column name & data_type from the specified table

        Returns
        -------
            dataframe: Dataframe
                pandas Dataframe for metadata
        """

        sql = (
            " select COLUMN_NAME, DATA_TYPE from table(%s) "
            " where table_name = %s and table_schema = %s "
        )

        params = (
            f"{self.database}.information_schema.columns",
            self.table.upper(),
            self.schema.upper(),
        )
        dataframe = self._execute_df(sql, params)
        d_map = {
            "DATE": "date",
            "TIMESTAMP_NTZ": "timestamp",
            "TIMESTAMP_TZ": "timestamp",
            "TIMESTAMP_LTZ": "timestamp",
            "TIMESTAMP": "timestamp",
            "TEXT": "string",
            "NUMBER": "numeric",
        }
        dataframe["VARIABLE_TYPE"] = dataframe["DATA_TYPE"].apply(lambda x: d_map[x])
        return dataframe

    def _execute_df(self, sql: str, params: Tuple[Any, ...]) -> pd.DataFrame:
        """
        Execute input sql and return pandas Dataframe as result

        Parameters
        ----------
        sql: str
            parameterized sql from caller function
        params: Tuple[Any, ...]
            parameters for the sql query.

        Returns
        -------
            dataframe: Dataframe
                result dataframe from input sql
        """
        cursor = self.conn.cursor()
        try:
            cursor.execute(sql, params)
            return cursor.fetch_pandas_all()
        finally:
            cursor.close()
