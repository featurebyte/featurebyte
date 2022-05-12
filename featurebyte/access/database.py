"""
This module implements the Insert, Update and Search of EventSource JSON config files from Local database
"""

from typing import Any, Dict

import copy
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

    def search_sources(self, exact_match: bool, **kwargs: Any) -> Any:
        """
        Search document by the attributes specified in query_path_map

        Parameters
        ----------
        exact_match: bool
            whether the query parameters should be exactly matched or not
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
        """
        if len(kwargs) == 0:
            raise AttributeError("Search parameter cannot be empty")

        requirements = {}
        for key, value in kwargs.items():
            requirements[query_path_map[key]] = value

        requirements["exact"] = exact_match
        docs = self._tinydb_op("query", None, query_params=requirements)

        return docs

    @staticmethod
    def predicate(input_object: Any, requirements: Dict[str, str], exact_match: bool) -> bool:
        """
        Predicate to retrieve data from local TinyDB

        Parameters
        ----------
        input_object: Any
            existing json object from tinydb
        requirements: dict
            dict of query parameters
        exact_match: bool
            whether exact_match is required or not

        Returns
        -------
        bool
        """
        condition = True

        for r_key, r_value in requirements.items():
            exist_value = copy.deepcopy(input_object)
            for key in r_key.split("."):
                exist_value = exist_value[key]

            if exact_match:
                condition &= exist_value == r_value
            else:
                condition &= re.match(r_value, exist_value, re.IGNORECASE) is not None

        return condition is True

    def insert_source(self, doc: Dict[Any, Any]) -> None:
        """
        Insert the JSON Document doc if it doesn't already exist, otherwise update the existing Document

        Parameters
        ----------
        doc: dict
            Json config document for the event source to be inserted into local database
        """
        r_docs = self.search_sources(exact_match=True, name=doc["name"])
        if len(r_docs) > 0:
            logger.info(f"{doc['name']} already existed in local db. Doing update")
            doc["updated"] = str(datetime.now())
            self._tinydb_op("update", doc)
        else:
            logger.info(f"Creating {doc['name']} json document in local db")
            doc["created"] = str(datetime.now())
            self._tinydb_op("insert", doc)

    def _tinydb_op(self, operation: str, doc: Any, query_params: Any = None) -> Any:
        if operation == "query":
            exact_match = query_params.pop("exact")
            return self._tinydb.search(lambda obj: self.predicate(obj, query_params, exact_match))

        if operation == "insert":
            self._tinydb.insert(doc)

        if operation == "update":
            self._tinydb.update(doc, Query().name == doc["name"])

        return None
