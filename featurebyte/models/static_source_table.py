"""
StaticSourceTableModel models
"""

from __future__ import annotations

from typing import Union

import pymongo
from pydantic import Field
from typing_extensions import Annotated

from featurebyte.models.materialized_table import MaterializedTableModel
from featurebyte.models.request_input import SourceTableRequestInput, ViewRequestInput


class ViewStaticSourceInput(ViewRequestInput):
    """
    ViewStaticSourceInput is a ViewRequestInput that is used to create an StaticSourceTableModel
    """


class SourceTableStaticSourceInput(SourceTableRequestInput):
    """
    SourceTableStaticSourceInput is a SourceTableRequestInput that is used to create an StaticSourceTableModel
    """


StaticSourceInput = Annotated[
    Union[ViewStaticSourceInput, SourceTableStaticSourceInput], Field(discriminator="type")
]


class StaticSourceTableModel(MaterializedTableModel):
    """
    StaticSourceTableModel is a table that can be used to as a static source table

    request_input: StaticSourceInput
        The input that defines how the static source table table is created
    """

    request_input: StaticSourceInput

    class Settings(MaterializedTableModel.Settings):
        """
        MongoDB settings
        """

        collection_name: str = "static_source_table"

        indexes = MaterializedTableModel.Settings.indexes + [
            [
                ("name", pymongo.TEXT),
                ("description", pymongo.TEXT),
            ],
        ]
