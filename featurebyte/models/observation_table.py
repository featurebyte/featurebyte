"""
ObservationTableModel models
"""
from __future__ import annotations

from typing import List, Literal, Optional, Union
from typing_extensions import Annotated

from abc import abstractmethod  # pylint: disable=wrong-import-order
from datetime import datetime  # pylint: disable=wrong-import-order

import pymongo
from pydantic import Field, StrictStr, validator
from sqlglot.expressions import Select

from featurebyte.enum import SourceType, StrEnum
from featurebyte.models.base import FeatureByteBaseModel, PydanticObjectId
from featurebyte.models.materialized_table import MaterializedTable
from featurebyte.query_graph.model.common_table import TabularSource
from featurebyte.query_graph.model.graph import QueryGraphModel
from featurebyte.query_graph.node.schema import TableDetails
from featurebyte.query_graph.sql.adapter import get_sql_adapter
from featurebyte.query_graph.sql.common import sql_to_string
from featurebyte.query_graph.sql.materialisation import (
    create_table_as,
    get_row_count_sql,
    get_source_expr,
    get_view_expr,
)
from featurebyte.session.base import BaseSession


class ObservationInputType(StrEnum):
    """
    Input type refers to how an ObservationTableModel is created
    """

    VIEW = "view"
    SOURCE_TABLE = "source_table"


class BaseObservationInput(FeatureByteBaseModel):
    """
    BaseObservationInput is the base class for all ObservationInput types
    """

    @abstractmethod
    def get_query_expr(self, source_type: SourceType) -> Select:
        """
        Get the SQL expression for the underlying data (can be either a table or a view)

        Parameters
        ----------
        source_type: SourceType
            The source type of the destination table

        Returns
        -------
        Select
        """

    async def get_row_count(self, session: BaseSession) -> int:
        """
        Get the number of rows in the observation table

        Parameters
        ----------
        session: BaseSession
            The session to use to get the row count

        Returns
        -------
        int
        """
        query = get_row_count_sql(
            table_expr=self.get_query_expr(source_type=session.source_type),
            source_type=session.source_type,
        )
        result = await session.execute_query(query)
        return int(result.iloc[0]["row_count"])  # type: ignore[union-attr]

    async def materialize(
        self, session: BaseSession, destination: TableDetails, sample_rows: Optional[int]
    ) -> None:
        """
        Materialize the observation table

        Parameters
        ----------
        session: BaseSession
            The session to use to materialize the table
        destination: TableDetails
            The destination table details
        sample_rows: Optional[int]
            The number of rows to sample. If None, no sampling is performed
        """

        query_expr = self.get_query_expr(source_type=session.source_type)

        if sample_rows is not None:
            num_rows = await self.get_row_count(session=session)
            if num_rows > sample_rows:
                # Sample a bit above the theoretical sample percentage since bernoulli sampling
                # doesn't guarantee an exact number of rows.
                # buffer_rows = 100
                # num_percent = 100 * float((sample_rows + buffer_rows) / num_rows)
                num_percent = 100 * float(sample_rows / num_rows) * 1.1
                adapter = get_sql_adapter(source_type=session.source_type)
                query_expr = adapter.tablesample(query_expr, num_percent).limit(sample_rows)

        query = sql_to_string(
            create_table_as(table_details=destination, select_expr=query_expr),
            source_type=session.source_type,
        )

        await session.execute_query(query)


class ViewObservationInput(BaseObservationInput):
    """
    ViewObservationInput is the input for creating an ObservationTableModel from a view

    graph: QueryGraphModel
        The query graph that defines the view
    node_name: str
        The name of the node in the query graph that defines the view
    type: Literal[ObservationInputType.VIEW]
        The type of the input. Must be VIEW for this class
    """

    graph: QueryGraphModel
    node_name: StrictStr
    type: Literal[ObservationInputType.VIEW] = Field(ObservationInputType.VIEW, const=True)

    def get_query_expr(self, source_type: SourceType) -> Select:
        return get_view_expr(graph=self.graph, node_name=self.node_name, source_type=source_type)


class SourceTableObservationInput(BaseObservationInput):
    """
    SourceTableObservationInput is the input for creating an ObservationTableModel from a source table

    source: TabularSource
        The source table
    type: Literal[ObservationInputType.SOURCE_TABLE]
        The type of the input. Must be SOURCE_TABLE for this class
    """

    source: TabularSource
    type: Literal[ObservationInputType.SOURCE_TABLE] = Field(
        ObservationInputType.SOURCE_TABLE, const=True
    )

    def get_query_expr(self, source_type: SourceType) -> Select:
        _ = source_type
        return get_source_expr(source=self.source.table_details)


ObservationInput = Annotated[
    Union[ViewObservationInput, SourceTableObservationInput], Field(discriminator="type")
]


class ObservationTableModel(MaterializedTable):
    """
    ObservationTableModel is a table that can be used to request historical features

    observation_input: ObservationInput
        The input that defines how the observation table is created
    context_id: Optional[PydanticObjectId]
        The id of the context that the observation table is associated with
    """

    observation_input: ObservationInput
    column_names: List[StrictStr]
    most_recent_point_in_time: StrictStr
    context_id: Optional[PydanticObjectId] = Field(default=None)

    @validator("most_recent_point_in_time")
    @classmethod
    def _validate_most_recent_point_in_time(cls, value: str) -> str:
        # Check that most_recent_point_in_time is a valid ISO 8601 datetime
        _ = datetime.fromisoformat(value)
        return value

    class Settings(MaterializedTable.Settings):
        """
        MongoDB settings
        """

        collection_name: str = "observation_table"

        indexes = MaterializedTable.Settings.indexes + [
            pymongo.operations.IndexModel("context_id"),
            [
                ("name", pymongo.TEXT),
            ],
        ]
