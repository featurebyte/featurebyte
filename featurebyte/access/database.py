"""
This module implements the Insert, Update and Search of EventSource JSON config files from Local database
"""

from typing import Any, Dict

import os
import re
from datetime import datetime

from logzero import logger
from tinydb import Query, TinyDB

query_path_map = {
    "name": "name",
    "author": "author",
    "origin_wh": "source.type",
    "origin_table": "source.connection.table",
    "exact": False,
}


class LocalSourceDBManager:
    """
    Wrapper instance to search and update the local TinyDB database
    """

    def __init__(self) -> None:
        """
        Instantiate LocalSourceDBManager instance with default TinyDB output json to ~/.featurebyte_db.json
        """
        fb_path = os.path.join(os.path.expanduser("~"), ".featurebyte_db.json")
        self._tinydb = TinyDB(fb_path)

    def search_sources(self, **kwargs: Any) -> Any:
        """
        Search document by the attributes specified in query_path_map

        Parameters
        ----------
        kwargs
            Arbitrary keyword arguments for search query

        Returns
        -------
        docs:
            list of json documents

        Raises
        ------
        AttributeError
            if search parameters is empty
        ValueError
            if the search parameter is not supported
        """
        if len(kwargs) == 0:
            raise AttributeError("Search parameter cannot be empty")

        keys = kwargs.keys()
        q_str = ""
        exact_match = kwargs.get("exact", False)

        for key in keys:
            if key == "exact":
                continue

            if key not in query_path_map:
                raise ValueError(f"search parameter {key} is not supported")

            value = kwargs[key]
            q_key = query_path_map[key]

            if isinstance(value, str):
                if exact_match:
                    q_str += f"(query.{q_key} == '{value}')"
                else:
                    q_str += f"(query.{q_key}.matches('{value}', flags=re.IGNORECASE))"
            else:
                q_str += f"(query.{q_key} == {value})"

            q_str += "&"

        if q_str.endswith("&"):
            q_str = q_str[:-1]

        docs = self._tinydb_op("query", None, query_str=q_str)

        return docs

    def insert_source(self, doc: Dict[Any, Any]) -> None:
        """
        Insert the JSON Document doc if it doesn't already exist, otherwise update the existing Document

        Parameters
        ----------
        doc: dict
            Json config document for the event source to be inserted into local database
        """
        r_docs = self.search_sources(name=doc["name"], exact=True)
        if len(r_docs) > 0:
            logger.info(f"{doc['name']} already existed in local db. Doing update")
            doc["updated"] = str(datetime.now())
            # self._tinydb.update(doc, Query().name == doc["name"])
            self._tinydb_op("update", doc)
        else:
            logger.info(f"Creating {doc['name']} json document in local db")
            doc["created"] = str(datetime.now())
            # self._tinydb.insert(doc)
            self._tinydb_op("insert", doc)

    def _tinydb_op(self, operation: str, doc: Any, query_str: str = "") -> Any:
        if operation == "query":
            query = Query()
            logger.debug(f"re.IGNORECASE: {re.IGNORECASE} and query: {query}")
            # pylint: disable=W0123
            return eval(f"self._tinydb.search({query_str})")  # nosec B307

        if operation == "insert":
            self._tinydb.insert(doc)

        if operation == "update":
            self._tinydb.update(doc, Query().name == doc["name"])

        return None
