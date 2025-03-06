"""
View class
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime
from functools import cached_property
from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
    Dict,
    List,
    Optional,
    Tuple,
    Type,
    TypeVar,
    Union,
    cast,
)

import pandas as pd
from pydantic import PrivateAttr
from typeguard import typechecked
from typing_extensions import Literal

from featurebyte.api.batch_request_table import BatchRequestTable
from featurebyte.api.entity import Entity
from featurebyte.api.feature import Feature
from featurebyte.api.feature_group import FeatureGroup
from featurebyte.api.mixin import SampleMixin
from featurebyte.api.obs_table.utils import get_definition_for_obs_table_creation_from_view
from featurebyte.api.observation_table import ObservationTable
from featurebyte.api.static_source_table import StaticSourceTable
from featurebyte.api.target import Target
from featurebyte.api.window_validator import validate_offset
from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.common.join_utils import (
    apply_column_name_modifiers,
    apply_column_name_modifiers_columns_info,
    combine_column_info_of_views,
    filter_columns,
    filter_columns_info,
    is_column_name_in_columns,
)
from featurebyte.common.utils import validate_datetime_input
from featurebyte.common.validator import validate_target_type
from featurebyte.core.frame import Frame, FrozenFrame
from featurebyte.core.generic import ProtectedColumnsQueryObject, QueryObject
from featurebyte.core.series import FrozenSeries, FrozenSeriesT, Series
from featurebyte.core.util import series_binary_operation
from featurebyte.enum import DBVarType, TargetType
from featurebyte.exception import (
    NoJoinKeyFoundError,
    RepeatedColumnNamesError,
    TargetFillValueNotProvidedError,
)
from featurebyte.logging import get_logger
from featurebyte.models.batch_request_table import BatchRequestInput, ViewBatchRequestInput
from featurebyte.models.observation_table import ViewObservationInput
from featurebyte.models.static_source_table import ViewStaticSourceInput
from featurebyte.query_graph.enum import GraphNodeType, NodeOutputType, NodeType
from featurebyte.query_graph.model.column_info import ColumnInfo
from featurebyte.query_graph.model.timestamp_schema import TimeZoneColumn
from featurebyte.query_graph.node import Node
from featurebyte.query_graph.node.cleaning_operation import (
    CleaningOperation,
    ColumnCleaningOperation,
)
from featurebyte.query_graph.node.generic import JoinMetadata, ProjectNode
from featurebyte.query_graph.node.input import InputNode
from featurebyte.query_graph.node.nested import BaseGraphNode
from featurebyte.schema.batch_request_table import BatchRequestTableCreate
from featurebyte.schema.observation_table import ObservationTableCreate
from featurebyte.schema.static_source_table import StaticSourceTableCreate
from featurebyte.typing import UNSET, OptionalScalar, ScalarSequence, Unset

if TYPE_CHECKING:
    from featurebyte.api.groupby import GroupBy
else:
    GroupBy = TypeVar("GroupBy")

ViewT = TypeVar("ViewT", bound="View")


logger = get_logger(__name__)


class ViewColumn(Series, SampleMixin):
    """
    ViewColumn class that is the base class of columns returned from any View. It is a series-like object that
    can be used to create features, or perform operations with other series-like objects.
    """

    # class variables
    __fbautodoc__: ClassVar[FBAutoDoc] = FBAutoDoc(proxy_class="featurebyte.ViewColumn")

    # instance variables
    _parent: Optional[View] = PrivateAttr(default=None)

    @property
    def timestamp_column(self) -> Optional[str]:
        if not self._parent:
            return None
        return self._parent.timestamp_column

    @property
    def cleaning_operations(self) -> Optional[List[CleaningOperation]]:
        """
        List of cleaning operations that were applied to this column during the view's creation.

        Returns
        -------
        Optional[List[CleaningOperation]]
            Returns None if the view column does not have parent. Otherwise, returns the list of cleaning operations.

        Examples
        --------
        Show the list of cleaning operations of the event view amount column after updating the critical
        data info of the event table.

        >>> event_table = catalog.get_table("GROCERYINVOICE")
        >>> event_table["Amount"].update_critical_data_info(
        ...     cleaning_operations=[
        ...         fb.MissingValueImputation(imputed_value=0),
        ...     ]
        ... )
        >>> view = catalog.get_view("GROCERYINVOICE")
        >>> view["Amount"].cleaning_operations
        [MissingValueImputation(imputed_value=0.0)]

        Empty list of column cleaning operations of the event table amount column.

        >>> event_table["Amount"].update_critical_data_info(cleaning_operations=[])
        >>> event_table["Amount"].cleaning_operations
        []
        """
        if not self.parent:
            return None

        for column_cleaning_operations in self.parent.column_cleaning_operations:
            if column_cleaning_operations.column_name == self.name:
                return cast(List[CleaningOperation], column_cleaning_operations.cleaning_operations)
        return []

    def sample(
        self,
        size: int = 10,
        seed: int = 1234,
        from_timestamp: Optional[Union[datetime, str]] = None,
        to_timestamp: Optional[Union[datetime, str]] = None,
        **kwargs: Any,
    ) -> pd.DataFrame:
        """
        Returns a Series that contains a random selection of rows of the view column based on a specified time range,
        size, and seed for sampling control. The materialization process occurs after any cleaning operations that
        were defined either at the table level or during the view's creation.

        Parameters
        ----------
        size: int
            Maximum number of rows to sample, with an upper bound of 10,000 rows.
        seed: int
            Seed to use for random sampling.
        from_timestamp: Optional[datetime]
            Start of date range to sample from.
        to_timestamp: Optional[datetime]
            End of date range to sample from.
        **kwargs: Any
            Additional keyword parameters.

        Returns
        -------
        pd.DataFrame
            Sampled rows of the data.

        Examples
        --------
        Sample 3 rows of a column.
        >>> catalog.get_view("GROCERYPRODUCT")["ProductGroup"].sample(3)
          ProductGroup
        0       Épices
        1         Chat
        2        Pains


        Sample 3 rows of a column with timestamp.
        >>> catalog.get_view("GROCERYINVOICE")["Amount"].sample(  # doctest: +SKIP
        ...     size=3, seed=123, from_timestamp="2020-01-01", to_timestamp="2023-01-31"
        ... )

        See Also
        --------
        - [ViewColumn.preview](/reference/featurebyte.api.view.ViewColumn.preview/):
          Retrieve a preview of a ViewColumn.
        - [ViewColumn.sample](/reference/featurebyte.api.view.ViewColumn.sample/):
          Retrieve a sample of a ViewColumn.
        """
        return super().sample(size, seed, from_timestamp, to_timestamp, **kwargs)

    @typechecked
    def preview(self, limit: int = 10, **kwargs: Any) -> pd.DataFrame:
        """
        Returns a DataFrame that contains a selection of rows of the view column. The materialization process occurs
        after any cleaning operations that were defined either at the table level or during the view's creation.

        Parameters
        ----------
        limit: int
            Maximum number of return rows.
        **kwargs: Any
            Additional keyword parameters.

        Returns
        -------
        pd.DataFrame
            Preview rows of the data.

        Examples
        --------
        Preview 3 rows of a column.
        >>> catalog.get_view("GROCERYPRODUCT")["GroceryProductGuid"].preview(3)
                             GroceryProductGuid
        0  10355516-5582-4358-b5f9-6e1ea7d5dc9f
        1  116c9284-2c41-446e-8eee-33901e0acdef
        2  3a45a5e8-1b71-42e8-b84e-43ddaf692375
        """
        return super().preview(limit=limit, **kwargs)

    @typechecked
    def describe(
        self,
        size: int = 0,
        seed: int = 1234,
        from_timestamp: Optional[Union[datetime, str]] = None,
        to_timestamp: Optional[Union[datetime, str]] = None,
        **kwargs: Any,
    ) -> pd.DataFrame:
        """
        Returns descriptive statistics of the view column. The statistics are computed after any cleaning operations
        that were defined either at the table level or during the view's creation have been applied.

        Parameters
        ----------
        size: int
            Maximum number of rows to sample.
        seed: int
            Seed to use for random sampling.
        from_timestamp: Optional[datetime]
            Start of date range to sample from.
        to_timestamp: Optional[datetime]
            End of date range to sample from.
        **kwargs: Any
            Additional keyword parameters.

        Returns
        -------
        pd.DataFrame
            Summary of the view.

        Examples
        --------
        Get summary of a column.
        >>> catalog.get_view("GROCERYPRODUCT")["ProductGroup"].describe()
                        ProductGroup
        dtype                VARCHAR
        unique                    87
        %missing                 0.0
        %empty                     0
        entropy              4.13031
        top       Chips et Tortillas
        freq                  1319.0


        Get summary of a column with timestamp.
        >>> catalog.get_view("GROCERYINVOICE")["Amount"].describe(  # doctest: +SKIP
        ...     from_timestamp="2020-01-01", to_timestamp="2023-01-31"
        ... )
        """
        return super().describe(size, seed, from_timestamp, to_timestamp, **kwargs)

    def _get_view_and_input_col_for_lookup(self, function_name: str) -> Tuple[View, str]:
        """
        Returns the View object that is used for lookup targets or features.

        Parameters
        ----------
        function_name: str
            The name of the function that is being called.

        Returns
        -------
        Tuple[View, str]
            The View object and the input column name.

        Raises
        ------
        ValueError
            If the View object is not assigned to the ViewColumn.
        """
        view = self._parent
        if view is None:
            raise ValueError(
                f"{function_name} is only supported for named columns in the View object. Consider"
                f" assigning to the View before calling {function_name}()."
            )
        input_column_name = cast(ProjectNode.Parameters, self.node.parameters).columns[0]
        return cast(View, view[[input_column_name]]), input_column_name

    @typechecked
    def as_target(
        self,
        target_name: str,
        offset: Optional[str] = None,
        target_type: Optional[TargetType] = None,
        fill_value: Union[OptionalScalar, Unset] = UNSET,
    ) -> Target:
        """
        Create a lookup target directly from the column in the View.

        For SCD views, lookup targets are materialized through point-in-time joins, and the resulting value represents
        the active row for the natural key at the point-in-time indicated in the target request.

        To obtain a target value at a specific time after the request's point-in-time, an offset can be specified.

        Parameters
        ----------
        target_name: str
            Name of the target to create.
        offset: str
            When specified, retrieve target value as of this offset after the point-in-time.
        target_type: Optional[TargetType]
            Type of the target
        fill_value: Union[OptionalScalar, Unset]
            Value to fill if the value in the column is empty

        Returns
        -------
        Target

        Raises
        ------
        TargetFillValueNotProvidedError
            If fill_value is not provided.

        Examples
        --------
        >>> customer_view = catalog.get_view("GROCERYCUSTOMER")
        >>> # Extract operating system from BrowserUserAgent column
        >>> customer_view["OperatingSystemIsWindows"] = customer_view.BrowserUserAgent.str.contains(
        ...     "Windows"
        ... )
        >>> # Create a target from the OperatingSystemIsWindows column
        >>> uses_windows = customer_view.OperatingSystemIsWindows.as_target(
        ...     "UsesWindows", fill_value=None
        ... )


        If the view is a Slowly Changing Dimension View, you may also consider creating a target that retrieves the
        entity's attribute at a specific time after the point-in-time specified in the target request by specifying
        an offset.

        >>> uses_windows_next_12w = customer_view.OperatingSystemIsWindows.as_target(
        ...     "UsesWindows_next_12w", offset="12w", fill_value=None
        ... )
        """
        view, input_column_name = self._get_view_and_input_col_for_lookup("as_target")

        # Perform validation
        validate_offset(offset)

        # Add lookup node to graph, and return Target
        lookup_node_params = view.get_lookup_node_params([input_column_name], [target_name], offset)
        input_node = view.get_input_node_for_lookup_node()
        lookup_node = self.graph.add_operation(
            node_type=NodeType.LOOKUP_TARGET,
            node_params=lookup_node_params,
            node_output_type=NodeOutputType.FRAME,
            input_nodes=[input_node],
        )
        target = view.project_target_from_node(
            input_node=lookup_node,
            target_name=target_name,
            target_dtype=view.column_var_type_map[input_column_name],
        )

        if fill_value is UNSET:
            raise TargetFillValueNotProvidedError("fill_value must be provided")
        elif fill_value is not None:
            target.fillna(fill_value)  # type: ignore
        if target_type:
            validate_target_type(target_type, target.dtype)
            target.update_target_type(target_type)
        return target

    @typechecked
    def as_feature(
        self,
        feature_name: str,
        offset: Optional[str] = None,
        fill_value: OptionalScalar = None,
    ) -> Feature:
        """
        Creates a lookup feature directly from the column in the View.

        For SCD views, lookup features are materialized through point-in-time joins, and the resulting value represents
        the active row for the natural key at the point-in-time indicated in the feature request.

        To obtain a feature value at a specific time before the request's point-in-time, an offset can be specified.

        Parameters
        ----------
        feature_name: str
            Name of the feature to create.
        offset: str
            When specified, retrieve feature value as of this offset prior to the point-in-time.
        fill_value: OptionalScalar
            Value to fill if the value in the column is empty

        Returns
        -------
        Feature

        Examples
        --------
        >>> customer_view = catalog.get_view("GROCERYCUSTOMER")
        >>> # Extract operating system from BrowserUserAgent column
        >>> customer_view["OperatingSystemIsWindows"] = customer_view.BrowserUserAgent.str.contains(
        ...     "Windows"
        ... )
        >>> # Create a feature from the OperatingSystemIsWindows column
        >>> uses_windows = customer_view.OperatingSystemIsWindows.as_feature("UsesWindows")


        If the view is a Slowly Changing Dimension View, you may also consider to create a feature that retrieves the
        entity's attribute at a point-in-time prior to the point-in-time specified in the feature request by specifying
        an offset.

        >>> uses_windows_12w_ago = customer_view.OperatingSystemIsWindows.as_feature(
        ...     "UsesWindows_12w_ago", offset="12w"
        ... )
        """
        view, input_column_name = self._get_view_and_input_col_for_lookup("as_feature")
        feature = view.as_features(
            [input_column_name],
            [feature_name],
            offset=offset,
        )[feature_name]

        if fill_value is not None:
            feature.fillna(fill_value)  # type: ignore
        return cast(Feature, feature)

    @property
    def is_datetime(self) -> bool:
        """
        Returns whether the view column has a datetime data type.

        Returns
        -------
        bool

        Examples
        --------
        >>> view = fb.Table.get("GROCERYINVOICE").get_view()

        >>> print(view["Timestamp"].is_datetime)
        True
        >>> print(view["Amount"].is_datetime)
        False
        """
        return super().is_datetime

    @property
    def is_numeric(self) -> bool:
        """
        Returns whether the view column has a numeric data type.

        Returns
        -------
        bool

        Examples
        --------
        >>> view = fb.Table.get("GROCERYINVOICE").get_view()

        >>> print(view["Amount"].is_numeric)
        True
        >>> print(view["Timestamp"].is_numeric)
        False
        """
        return super().is_numeric

    @typechecked
    def preview_sql(self, limit: int = 10, **kwargs: Any) -> str:
        """
        Returns an SQL query for previewing the column data after applying the set of cleaning operations defined
        at the column level.

        Parameters
        ----------
        limit: int
            maximum number of return rows
        **kwargs: Any
            Additional keyword parameters

        Returns
        -------
        str
        """
        return super().preview_sql(limit=limit, **kwargs)

    @typechecked
    def astype(
        self: FrozenSeriesT,
        new_type: Union[Type[int], Type[float], Type[str], Literal["int", "float", "str"]],
    ) -> FrozenSeriesT:
        """
        Converts the data type of a column. It is useful when you need to convert column values between numerical and
        string formats, or the other way around.

        Parameters
        ----------
        new_type : Union[Type[int], Type[float], Type[str], Literal["int", "float", "str"]])
            Desired type after conversion. Type can be provided directly, or as a string.

        Returns
        -------
        FrozenSeriesT
            A new Series with converted variable type.

        Examples
        --------
        Convert a numerical series to a string series, and back to an int series.

        >>> event_view = catalog.get_view("GROCERYINVOICE")
        >>> event_view["Amount"] = event_view["Amount"].astype(str)
        >>> event_view["Amount"] = event_view["Amount"].astype(int)
        """
        return super().astype(new_type=new_type)

    @typechecked
    def isin(self: FrozenSeriesT, other: Union[FrozenSeries, ScalarSequence]) -> FrozenSeriesT:
        """
        Identifies if each element is contained in a sequence of values represented by the `other` parameter.

        Parameters
        ----------
        other: Union[FrozenSeries, ScalarSequence]
            The sequence of values to check for membership. `other` can be a predefined list of values.

        Returns
        -------
        FrozenSeriesT
            Column with boolean values

        Examples
        --------
        Check to see if the values in a series are in a list of values, and use the result to filter
        the original view:

        >>> view = catalog.get_table("GROCERYPRODUCT").get_view()
        >>> condition = view["ProductGroup"].isin(["Sauces", "Fromages", "Fruits"])
        >>> view[condition].sample(5, seed=0)
                             GroceryProductGuid ProductGroup
        0  45cd58ba-efec-463a-9107-0633168a215e     Fromages
        1  97e6afc9-1033-4fb3-b2a2-3d62261e1d17     Fromages
        2  fb26ed22-524e-4c9e-9ea2-03c266e7f9b9     Fromages
        3  a817d904-bc58-4048-978d-c13857969a69       Fruits
        4  00abe6d0-e3f7-4f29-b0ab-69ea5581ab02       Sauces
        """
        return super().isin(other=other)

    def zip_timestamp_timezone_columns(self) -> ViewColumn:
        """
        Zips the timestamp and timezone columns into a single timestamp with timezone tuple.

        Returns
        -------
        ViewColumn
            A new column with the zipped timestamp and timezone columns.

        Raises
        ------
        ValueError
            If the column does not have an associated timezone column.
        """
        timezone_column_name = self.associated_timezone_column_name
        if timezone_column_name is None:
            raise ValueError(
                "Column must have a timezone column associated with it to zip the columns."
            )

        # Must have a timestamp schema because the column has an associated timezone column
        timestamp_schema = self.dtype_info.timestamp_schema
        assert timestamp_schema is not None

        return series_binary_operation(
            input_series=self,
            other=self._parent[timezone_column_name],  # type: ignore
            node_type=NodeType.ZIP_TIMESTAMP_TZ_TUPLE,
            output_var_type=DBVarType.TIMESTAMP_TZ_TUPLE,
            additional_node_params={"timestamp_schema": timestamp_schema},
        )


class GroupByMixin:
    """
    Mixin that provides groupby functionality to a View object
    """

    # class variables
    __fbautodoc__: ClassVar[FBAutoDoc] = FBAutoDoc()

    @typechecked
    def groupby(self, by_keys: Union[str, List[str]], category: Optional[str] = None) -> GroupBy:
        """
        The groupby method of a view returns a GroupBy class that can be used to group data based on one or more
        columns representing entities (specified in the key parameter). Within each entity or group of entities,
        the GroupBy class applies aggregation function(s) to the data.

        The grouping keys determine the primary entity for the declared features in the aggregation function.

        Moreover, the groupby method's category parameter allows you to define a categorical column, which can be
        used to generate Cross Aggregate Features. These features involve aggregating data across categories of the
        categorical column, enabling the extraction of patterns in an entity across these categories. For instance,
        you can calculate the amount spent by a customer on each product category during a specific time period using
        this approach.

        Parameters
        ----------
        by_keys: Union[str, List[str]]
            Specifies the column or list of columns by which the data should be grouped. These columns must correspond
            to entities registered in the catalog. If this parameter is set to an empty list, the data will not be
            grouped.
        category : Optional[str]
            Optional category parameter to enable aggregation across categories. To use this parameter, provide the
            name of a column in the View that represents a categorical column.

        Returns
        -------
        GroupBy
            a groupby object that contains information about the groups

        See Also
        --------
        - [GroupBy](/reference/featurebyte.api.groupby.GroupBy/): GroupBy object
        - [GroupBy.aggregate](/reference/featurebyte.api.groupby.GroupBy.aggregate/):
        Create feature from grouped aggregates
        - [GroupBy.aggregate_over](/reference/featurebyte.api.groupby.GroupBy.aggregate_over/):
        Create features from grouped aggregates over different time windows

        Examples
        --------
        Groupby for Aggregate features.

        >>> items_view = catalog.get_view("INVOICEITEMS")
        >>> # Group items by the column GroceryCustomerGuid that references the customer entity
        >>> items_by_customer = items_view.groupby("GroceryCustomerGuid")  # doctest: +SKIP
        >>> # Declare features that measure the discount received by customer
        >>> customer_discounts = items_by_customer.aggregate_over(  # doctest: +SKIP
        ...     "Discount",
        ...     method=fb.AggFunc.SUM,
        ...     feature_names=["CustomerDiscounts_7d", "CustomerDiscounts_28d"],
        ...     fill_value=0,
        ...     windows=["7d", "28d"],
        ... )


        Groupby for Cross Aggregate features.

        >>> # Join product view to items view
        >>> product_view = catalog.get_view("GROCERYPRODUCT")
        >>> items_view = items_view.join(product_view)  # doctest: +SKIP
        >>> # Group items by the column GroceryCustomerGuid that references the customer entity
        >>> # And use ProductGroup as the column to perform operations across
        >>> items_by_customer_across_product_group = items_view.groupby(  # doctest: +SKIP
        ...     by_keys="GroceryCustomerGuid", category="ProductGroup"
        ... )
        >>> # Cross Aggregate feature of the customer purchases across product group over the past 4 weeks
        >>> customer_inventory_28d = (
        ...     items_by_customer_across_product_group.aggregate_over(  # doctest: +SKIP
        ...         "TotalCost",
        ...         method=fb.AggFunc.SUM,
        ...         feature_names=["CustomerInventory_28d"],
        ...         windows=["28d"],
        ...     )
        ... )
        """

        from featurebyte.api.groupby import GroupBy

        return GroupBy(obj=self, keys=by_keys, category=category)  # type: ignore

    def validate_aggregate_over_parameters(
        self, keys: list[str], value_column: Optional[str]
    ) -> None:
        """
        Perform View specific validation on the parameters provided for aggregate_over groupby's.

        Parameters
        ----------
        keys: list[str]
            keys
        value_column: Optional[str]
            Column to be aggregated
        """

    def validate_simple_aggregate_parameters(
        self, keys: list[str], value_column: Optional[str]
    ) -> None:
        """
        Perform View specific validation on the parameters provided for simple aggregation functions.

        Parameters
        ----------
        keys: list[str]
            keys
        value_column: Optional[str]
            Column to be aggregated
        """


class RawMixin(QueryObject, ABC):
    """
    Mixin that provides functionality to access raw view
    """

    # class variables
    __fbautodoc__: ClassVar[FBAutoDoc] = FBAutoDoc()
    _view_graph_node_type: ClassVar[GraphNodeType]

    @property
    def raw(self) -> FrozenFrame:
        """
        Return the raw input table view (without any cleaning operations applied).

        Examples
        --------
        >>> items_view = catalog.get_view("INVOICEITEMS")
        >>> items_view["Discount_missing"] = items_view.raw["Discount"].isnull()

        Returns
        -------
        FrozenFrame
        """
        view_input_node_names = []
        for graph_node in self.graph.iterate_nodes(target_node=self.node, node_type=NodeType.GRAPH):
            assert isinstance(graph_node, BaseGraphNode)
            if graph_node.parameters.type == self._view_graph_node_type:
                view_input_node_names = self.graph.get_input_node_names(graph_node)

        # first input node names must be the input node used to create the view
        assert len(view_input_node_names) >= 1, "View should have at least one input"
        input_node = self.graph.get_node_by_name(view_input_node_names[0])
        assert isinstance(input_node, InputNode)
        return FrozenFrame(
            node_name=input_node.name,
            tabular_source=self.tabular_source,
            feature_store=self.feature_store,
            columns_info=[
                ColumnInfo(name=col.name, dtype=col.dtype, entity_id=None, semantic_id=None)
                for col in input_node.parameters.columns
            ],
        )


class View(ProtectedColumnsQueryObject, Frame, SampleMixin, ABC):
    """
    Views are cleaned versions of Catalog tables and offer a flexible and efficient way to work with Catalog tables.
    They allow operations like creating new columns, filtering records, conditionally editing columns, extracting lags,
    capturing attribute changes, and joining views, similar to Pandas. Unlike Pandas DataFrames, which require loading
    all data into memory, views are materialized only when needed during previews or feature materialization.

    When a view is created, it inherits the metadata of the Catalog Table it originated from. There are currently five
    types of views supported:

    - Event Views
    - Item Views
    - Dimension Views
    - Slowly Changing Dimension (SCD) Views
    - Change Views

    The syntax used to manipulate data in a FeatureByte view is similar to a Pandas DataFrame.

    By default, data accessed through views is cleaned according to the default cleaning operations specified in the
    Catalog Tables. However, it is still possible to perform data manipulation based on the raw data present in the
    source table using the raw view attribute or to overwrite the cleaning operations when the view is created.

    View operations are only materialized for purposes such as samples, exploratory data analysis, or feature
    materialization. FeatureByte follows a lazy execution strategy where view operations are translated into a
    graphical representation of intended operations.
    """

    # class variables
    __fbautodoc__: ClassVar[FBAutoDoc] = FBAutoDoc(proxy_class="featurebyte.View")
    _view_graph_node_type: ClassVar[GraphNodeType]

    def __repr__(self) -> str:
        return f"{type(self).__name__}(node.name={self.node.name})"

    def __str__(self) -> str:
        return repr(self)

    @property
    def columns(self) -> list[str]:
        """
        List the names of the columns in the view.

        Returns
        -------
        list[str]
        """
        return super().columns

    @property
    def column_cleaning_operations(self) -> List[ColumnCleaningOperation]:
        """
        List the cleaning operations associated with each column in the view during view creation.

        Returns
        -------
        List[ColumnCleaningOperation]
            List of column cleaning operations

        See Also
        --------
        - [Table.columns_info](/reference/featurebyte.api.base_table.TableApiObject.columns_info)
        - [TableColumn.update_critical_data_info](/reference/featurebyte.api.base_table.TableColumn.update_critical_data_info)
        """
        return [
            ColumnCleaningOperation(
                column_name=col.name, cleaning_operations=col.critical_data_info.cleaning_operations
            )
            for col in self.columns_info
            if col.critical_data_info is not None and col.critical_data_info.cleaning_operations
        ]

    @typechecked
    def preview_sql(self, limit: int = 10, **kwargs: Any) -> str:
        """
        Returns an SQL query for previewing the view raw data after applying the set of cleaning operations defined
        at the column level.

        Parameters
        ----------
        limit: int
            maximum number of return rows
        **kwargs: Any
            Additional keyword parameters

        Returns
        -------
        str
        """
        return super().preview_sql(limit=limit, **kwargs)

    @typechecked
    def describe(
        self,
        size: int = 0,
        seed: int = 1234,
        from_timestamp: Optional[Union[datetime, str]] = None,
        to_timestamp: Optional[Union[datetime, str]] = None,
        **kwargs: Any,
    ) -> pd.DataFrame:
        """
        Returns descriptive statistics of the view. The statistics are computed after any cleaning operations
        that were defined either at the table level or during the view's creation have been applied.

        Parameters
        ----------
        size: int
            Maximum number of rows to sample.
        seed: int
            Seed to use for random sampling.
        from_timestamp: Optional[datetime]
            Start of date range to sample from.
        to_timestamp: Optional[datetime]
            End of date range to sample from.
        **kwargs: Any
            Additional keyword parameters.

        Returns
        -------
        pd.DataFrame
            Summary of the view.

        Examples
        --------
        Get summary of a view.
        >>> catalog.get_view("GROCERYPRODUCT").describe()
                                    GroceryProductGuid        ProductGroup
        dtype                                  VARCHAR             VARCHAR
        unique                                   29099                  87
        %missing                                   0.0                 0.0
        %empty                                       0                   0
        entropy                               6.214608             4.13031
        top       017fe5ed-80a2-4e70-ae48-78aabfdee856  Chips et Tortillas
        freq                                       1.0              1319.0


        Get summary of a view with timestamp.
        >>> catalog.get_view("GROCERYINVOICE").describe(  # doctest: +SKIP
        ...     from_timestamp=datetime(2019, 1, 1),
        ...     to_timestamp=datetime(2019, 1, 31),
        ... )
        """
        return super().describe(size, seed, from_timestamp, to_timestamp, **kwargs)

    @typechecked
    def preview(self, limit: int = 10, **kwargs: Any) -> pd.DataFrame:
        """
        Returns a DataFrame that contains a selection of rows of the view. The materialization process occurs after
        any cleaning operations that were defined either at the table level or during the view's creation.

        Parameters
        ----------
        limit: int
            Maximum number of return rows.
        **kwargs: Any
            Additional keyword parameters.

        Returns
        -------
        pd.DataFrame
            Preview rows of the data.

        Examples
        --------
        Preview 3 rows of a view.
        >>> catalog.get_view("GROCERYPRODUCT").preview(3)
                             GroceryProductGuid ProductGroup
        0  10355516-5582-4358-b5f9-6e1ea7d5dc9f      Glaçons
        1  116c9284-2c41-446e-8eee-33901e0acdef      Glaçons
        2  3a45a5e8-1b71-42e8-b84e-43ddaf692375      Glaçons
        """
        return super().preview(limit=limit, **kwargs)

    def sample(
        self,
        size: int = 10,
        seed: int = 1234,
        from_timestamp: Optional[Union[datetime, str]] = None,
        to_timestamp: Optional[Union[datetime, str]] = None,
        **kwargs: Any,
    ) -> pd.DataFrame:
        """
        Returns a DataFrame that contains a random selection of rows of the view based on a specified time range,
        size, and seed for sampling control. The materialization process occurs after any cleaning operations that
        were defined either at the table level or during the view's creation.

        Parameters
        ----------
        size: int
            Maximum number of rows to sample, with an upper bound of 10,000 rows.
        seed: int
            Seed to use for random sampling.
        from_timestamp: Optional[datetime]
            Start of date range to sample from.
        to_timestamp: Optional[datetime]
            End of date range to sample from.
        **kwargs: Any
            Additional keyword parameters.

        Returns
        -------
        pd.DataFrame
            Sampled rows of the data.

        Examples
        --------
        Sample rows of a view.
        >>> catalog.get_view("GROCERYPRODUCT").sample(size=3)
                             GroceryProductGuid ProductGroup
        0  e890c5cb-689b-4caf-8e49-6b97bb9420c0       Épices
        1  5720e4df-2996-4443-a1bc-3d896bf98140         Chat
        2  96fc4d80-8cb0-4f1b-af01-e71ad7e7104a        Pains


        Sample rows of a view with timestamp.
        >>> catalog.get_view("GROCERYINVOICE").sample(  # doctest: +SKIP
        ...     size=3,
        ...     from_timestamp=datetime(2019, 1, 1),
        ...     to_timestamp=datetime(2019, 1, 31),
        ... )

        See Also
        --------
        - [View.preview](/reference/featurebyte.api.view.View.preview/):
          Retrieve a preview of a view.
        - [View.sample](/reference/featurebyte.api.view.View.sample/):
          Retrieve a sample of a view.
        """
        return super().sample(size, seed, from_timestamp, to_timestamp, **kwargs)

    @property
    def entity_columns(self) -> list[str]:
        """
        List the names of the columns in the view identifying or referencing entities.

        Returns
        -------
        list[str]
        """
        return [col.name for col in self.columns_info if col.entity_id]

    @property
    def protected_attributes(self) -> list[str]:
        """
        List of protected attributes used to extract protected_columns

        Returns
        -------
        list[str]
        """
        return ["entity_columns"]

    @cached_property
    def inherited_columns(self) -> set[str]:
        """
        Special columns set which will be automatically added to the object of same class
        derived from current object

        Returns
        -------
        set[str]
        """
        additional_columns = self._get_additional_inherited_columns()
        join_col = self._get_join_column()
        if not join_col:
            return additional_columns
        return {join_col}.union(self._get_additional_inherited_columns())

    @cached_property
    def _reference_column_map(self) -> dict[str, list[str]]:
        """
        Contains the mapping of one column that references another column(s) in the view.

        Returns
        -------
        dict[str, list[str]]
        """
        output = {}
        for col_info in self.columns_info:
            dtype_metadata = col_info.dtype_metadata
            timestamp_schema = dtype_metadata.timestamp_schema if dtype_metadata else None
            timezone = timestamp_schema.timezone if timestamp_schema else None
            if isinstance(timezone, TimeZoneColumn):
                output[col_info.name] = [timezone.column_name]
        return output

    def _get_additional_inherited_columns(self) -> set[str]:
        """
        Additional columns set to be added to inherited_columns. To be overridden by subclasses of
        View when necessary.

        Returns
        -------
        set[str]
        """
        return set()

    def _conditionally_expand_columns(self, columns: list[str]) -> list[str]:
        """
        Conditionally expand columns based on the item provided. If the column of the selected columns is
        referring other columns, the expanded columns will include the referred columns as well.

        Parameters
        ----------
        columns: list[str]
            List of columns

        Returns
        -------
        list[str]
            Expanded list of columns
        """
        additional_cols = set()
        for col in columns:
            if col in self._reference_column_map:
                additional_cols.update(self._reference_column_map[col])
        return columns + list(additional_cols)

    @typechecked
    def __getitem__(
        self, item: Union[str, List[str], FrozenSeries]
    ) -> Union[FrozenSeries, FrozenFrame]:
        if isinstance(item, list) and all(isinstance(elem, str) for elem in item):
            item = sorted(self.inherited_columns.union(self._conditionally_expand_columns(item)))
        output = super().__getitem__(item)
        return output

    @typechecked
    def __setitem__(
        self,
        key: Union[str, Tuple[FrozenSeries, str]],
        value: Union[int, float, str, bool, FrozenSeries],
    ) -> None:
        key_to_check = key if not isinstance(key, tuple) else key[1]
        if key_to_check in self.protected_columns:
            raise ValueError(f"Column '{key}' cannot be modified!")
        super().__setitem__(key, value)

    def validate_join(self, other_view: View) -> None:
        """
        Validate join should be implemented by view classes that have extra requirements.

        Parameters
        ----------
        other_view: View
            the other view that we are joining with
        """

    def get_join_column(self) -> str:
        """
        Returns the column from the view that serves as the linking column when the view is joined with another view.

        Returns
        -------
        str
            the column name for the join key
        """
        join_column = self._get_join_column()
        assert join_column is not None, "Join column is not found"
        return join_column

    @abstractmethod
    def _get_join_column(self) -> Optional[str]:
        """
        Returns the column from the view that serves as the linking column when the view is joined with another view.

        Returns
        -------
        Optional[str]
            the column name for the join key or None if it is not found
        """

    def _get_join_parameters(self, calling_view: View) -> dict[str, Any]:
        """
        Returns additional query node parameters for join operation

        Note that self is the other view in the join, not the calling view.

        Parameters
        ----------
        calling_view: View
            Calling view of the join

        Returns
        -------
        dict[str, Any]
        """
        _ = calling_view
        return {}

    def get_additional_lookup_parameters(self, offset: Optional[str] = None) -> dict[str, Any]:
        """
        Returns any additional query node parameters for lookup operations - as_feature (LookupNode), or
        as_target (LookupTargetNode).

        This is a no-op unless the lookup is time-aware (currently only available for
        SCDView)

        Parameters
        ----------
        offset : str
            Optional offset parameter

        Returns
        -------
        dict[str, Any]
        """
        if offset is not None:
            logger.warning("offset parameter is provided but has no effect")
        return {}

    def _get_create_joined_view_parameters(self) -> dict[str, Any]:
        """
        Returns additional view type specific parameters for _create_joined_view operation

        Returns
        -------
        dict[str, Any]
        """
        return {}

    def _create_joined_view(
        self: ViewT,
        new_node_name: str,
        joined_columns_info: List[ColumnInfo],
        **kwargs: Any,
    ) -> ViewT:
        """
        Create a new joined view with the given node name and columns info.

        Parameters
        ----------
        new_node_name: str
            new node name
        joined_columns_info: List[ColumnInfo]
            joined columns info
        kwargs: Any
            Additional keyword arguments used to override the underlying metadata

        Returns
        -------
        ViewT
        """
        return type(self)(
            feature_store=self.feature_store,
            **{
                **self.model_dump(by_alias=True, exclude={"feature_store": True}),
                "graph": self.graph,
                "node_name": new_node_name,
                "columns_info": joined_columns_info,
                **self._get_create_joined_view_parameters(),
                **kwargs,
            },
        )

    def _get_key_if_entity(self, other_view: View) -> Optional[tuple[str, str]]:
        """
        Returns a key if there's a match based on entity.

        Parameters
        ----------
        other_view: View
            the other view we are joining on

        Returns
        -------
        Optional[tuple[str, str]]
            the left and right columns to join on, or None if there isn't exactly one match.
        """
        other_join_key = other_view.get_join_column()
        # If the other join key is not an entity, skip this search.
        entity_id = None
        for col in other_view.columns_info:
            if col.entity_id and col.name == other_join_key:
                entity_id = col.entity_id

        if entity_id is None:
            return None

        # Find if there's a match. Check to see if there's only exactly one match. If there are multiple, return empty
        # and log a debug message.
        num_of_matches = 0
        calling_col_name = ""
        for col in self.columns_info:
            if col.entity_id == entity_id:
                num_of_matches += 1
                calling_col_name = col.name

        if num_of_matches == 0:
            return None
        if num_of_matches == 1:
            return calling_col_name, other_join_key
        logger.debug(
            f"{num_of_matches} matches found for entity id {entity_id}. "
            f"Unable to automatically return a join key."
        )
        return None

    def _get_join_keys(self, other_view: View, on_column: Optional[str] = None) -> tuple[str, str]:
        """
        Returns the join keys of the two tables.

        Parameters
        ----------
        other_view: View
            the other view we are joining with
        on_column: Optional[str]
            the optional column we want to join on

        Returns
        -------
        tuple[str, str]
            the columns from the left and right tables that we want to join on

        Raises
        ------
        NoJoinKeyFoundError
            raised when no suitable join key has been found
        ValueError
            raised if the `on_column` passed in is an empty string
        """
        if on_column is not None:
            if on_column == "":
                raise ValueError(
                    "The `on` column should not be empty. Please provide a value for this parameter."
                )
            return on_column, other_view.get_join_column()

        # Check if the keys are entities
        response = self._get_key_if_entity(other_view)
        if response is not None:
            return response[0], response[1]

        # Check that the target join column is present in the calling list of columns.
        # If it is not present, the name of the column of the calling view should be specified.
        other_join_key = other_view.get_join_column()
        if is_column_name_in_columns(other_join_key, self.columns_info):
            return other_join_key, other_join_key

        raise NoJoinKeyFoundError(
            "Unable to automatically find a default join column key based on:\n"
            "- matching entities, or\n"
            f"- the join column '{other_join_key}' in the target view as it is not present in the"
            f" calling view\n"
            f"Please consider adding the `on` parameter in `join()` to explicitly specify a "
            f"column to join on."
        )

    def _validate_join(
        self,
        other_view: View,
        rsuffix: str = "",
        on: Optional[str] = None,
        rprefix: str = "",
    ) -> None:
        """
        Main validate call for the join. This checks that
        - If there are overlapping column names but rsuffix is empty, throw an error
        - the join column provided via `on` is present in the columns of the calling view
        - Calls the other validate_join function which can be overriden for implementation specific validation

        Parameters
        ----------
        other_view: View
            the other view that we are joining with
        rsuffix: str
            a suffix to append on to the right columns
        on: Optional[str]
            the column to join on
        rprefix: str
            a prefix to prepend on to the right columns

        Raises
        ------
        RepeatedColumnNamesError
            raised when there are overlapping columns, but no rsuffix has been provided
        NoJoinKeyFoundError
            raised when the on column provided, is not present in the columns
        """
        # Validate whether there are overlapping column names
        _, right_join_key = self._get_join_keys(other_view, on)
        right_excluded_columns = apply_column_name_modifiers(
            other_view.get_excluded_columns_as_other_view(right_join_key),
            rsuffix=rsuffix,
            rprefix=rprefix,
        )
        current_column_names = {col.name for col in self.columns_info}
        repeated_column_names = []
        for other_col in apply_column_name_modifiers_columns_info(
            other_view.columns_info, rsuffix, rprefix
        ):
            # Raise an error if the name is repeated and will be included in the join result
            if (
                other_col.name in current_column_names
                and other_col.name not in right_excluded_columns
            ):
                repeated_column_names.append(other_col.name)
        if len(repeated_column_names) > 0:
            raise RepeatedColumnNamesError(
                f"Duplicate column names {repeated_column_names} found between the "
                "calling view, and the target view.\nTo resolve this error, do consider "
                "setting the rsuffix parameter in `join()` to disambiguate the "
                "resulting columns in the joined view."
            )

        # Validate whether the join column provided is present in the columns
        if on is not None:
            current_column_names = {col.name for col in self.columns_info}
            if on not in current_column_names:
                raise NoJoinKeyFoundError(
                    f"The `on` column name provided '{on}' is not found in the calling view. "
                    f"Please pick a valid column name from {sorted(current_column_names)} to join on."
                )

        # Perform other validation
        self.validate_join(other_view)

    def get_excluded_columns_as_other_view(self, join_key: str) -> list[str]:
        """
        Get the columns to be excluded from the view when it is used as other_view in a join. By
        default, join key is always excluded. Specific views can opt to exclude additional columns
        by overriding _get_additional_excluded_columns_as_other_view().

        Parameters
        ----------
        join_key: str
            Join key

        Returns
        -------
        list[str]
            List of column names to be excluded
        """
        excluded_columns = [join_key]
        excluded_columns.extend(self._get_additional_excluded_columns_as_other_view())
        return excluded_columns

    def _get_additional_excluded_columns_as_other_view(self) -> list[str]:
        return []

    @typechecked
    def join(
        self: ViewT,
        other_view: View,
        on: Optional[str] = None,
        how: Literal["left", "inner"] = "left",
        rsuffix: str = "",
        rprefix: str = "",
    ) -> ViewT:
        """
        To join two views, use the join() method of the left view and specify the right view object in the other_view
        parameter. The method will match rows from both views based on a shared key, which is either the primary key
        of the right view or the natural key if the right view is a Slowly Changing Dimension (SCD) view.

        If the shared key identifies an entity that is referenced in the left view or the column name of the shared
        key is the same in both views, the join() method will automatically identify the column in the left view to
        use for the join.

        By default, a left join is performed, and the resulting view will have the same number of rows as the left
        view. However, you can set the how parameter to 'inner' to perform an inner join. In this case, the resulting
        view will only contain rows where there is a match between the columns in both tables.

        When the right view is an SCD view, the event timestamp of the left view determines which record of the right
        view to join.

        Parameters
        ----------
        other_view: View
            The right view that you want to join. Its primary key (natural key if the view is an SCD) must be
            represented by a column in the left view.
        on: Optional[str]
            Column name in the left view to use for the join. Note the ‘on’ argument is optional if:
            - the column name of the shared key is the same in both views.
            - the shared key identifies an entity that is referenced in the left view and the columns in the two
            views have been tagged with the entity reference.
        how: str
            The argument is optional. The default value is ‘left’, which indicates a left join. The resulting view
            will have the same number of rows as the left view. If ‘inner’ is selected, the resulting view will only
            contain rows where there is a match between the columns in both tables.
        rsuffix: str
            The argument is used to disambiguate overalapping column names after join by adding the
            suffix to the right view's column names. The default rsuffix is an empty string.
        rprefix: str
            The argument is used to disambiguate overalapping column names after join by adding the
            prefix to the right view's column names. The default rprefix is an empty string.

        Returns
        -------
        ViewT
            The joined view. Its type is the same as the left view.

        Examples
        --------
        Use the automated mode if one of the 2 following conditions are met:

        - the column name of the shared key is the same in both views.
        - the shared key identifies an entity that is referenced in the left view and the columns in the two views
        have been tagged with the entity reference.

        >>> items_view = catalog.get_view("INVOICEITEMS")
        >>> product_view = catalog.get_view("GROCERYPRODUCT")
        >>> items_view_with_product_group = items_view.join(product_view)


        If not, specify the column name in the left view that represents the right view’s primary key (natural key if
        the view is an SCD).

        >>> items_view = catalog.get_view("INVOICEITEMS")
        >>> product_view = catalog.get_view("GROCERYPRODUCT")
        >>> items_view_with_product_group = items_view.join(product_view, on="GroceryProductGuid")


        Use an inner join if you want the returned view to be filtered and contain only the rows that have matching
        values in both views.

        >>> items_view = catalog.get_view("INVOICEITEMS")
        >>> product_view = catalog.get_view("GROCERYPRODUCT")
        >>> items_view_with_non_missing_product_group = items_view.join(
        ...     product_view, on="GroceryProductGuid", how="inner"
        ... )
        """
        self._validate_join(other_view, rsuffix, on=on, rprefix=rprefix)

        left_input_columns = self.columns
        left_output_columns = self.columns

        left_on, right_on = self._get_join_keys(other_view, on)
        other_view_excluded_columns = other_view.get_excluded_columns_as_other_view(right_on)
        filtered_other_columns = filter_columns(
            other_view.columns, exclude_columns=other_view_excluded_columns
        )
        right_input_columns = filtered_other_columns
        right_output_columns = apply_column_name_modifiers(filtered_other_columns, rsuffix, rprefix)

        node_params = {
            "left_on": left_on,
            "right_on": right_on,
            "left_input_columns": left_input_columns,
            "left_output_columns": left_output_columns,
            "right_input_columns": right_input_columns,
            "right_output_columns": right_output_columns,
            "join_type": how,
            "metadata": JoinMetadata(rsuffix=rsuffix, rprefix=rprefix),
        }
        node_params.update(other_view._get_join_parameters(self))

        node = self.graph.add_operation(
            node_type=NodeType.JOIN,
            node_params=node_params,
            node_output_type=NodeOutputType.FRAME,
            input_nodes=[self.node, other_view.node],
        )

        # Construct new columns_info
        filtered_column_infos = filter_columns_info(
            other_view.columns_info, other_view_excluded_columns
        )
        joined_columns_info = combine_column_info_of_views(
            self.columns_info,
            apply_column_name_modifiers_columns_info(filtered_column_infos, rsuffix, rprefix),
        )

        # create a new view and return it
        return self._create_joined_view(
            new_node_name=node.name, joined_columns_info=joined_columns_info
        )

    def project_target_from_node(
        self,
        input_node: Node,
        target_name: str,
        target_dtype: DBVarType,
    ) -> Target:
        """
        Create a Target object from a node that produces targets, such as groupby, lookup, etc.

        Parameters
        ----------
        input_node: Node
            Query graph node
        target_name: str
            Target name
        target_dtype: DBVarType
            Variable type of the Target

        Returns
        -------
        Feature
        """
        # Project target node.
        target_node = self.graph.add_operation(
            node_type=NodeType.PROJECT,
            node_params={"columns": [target_name]},
            node_output_type=NodeOutputType.SERIES,
            input_nodes=[input_node],
        )
        # Build and return Target
        entity_ids = self.graph.get_entity_ids(node_name=target_node.name)
        return Target(
            name=target_name,
            entity_ids=entity_ids,
            graph=self.graph,
            node_name=target_node.name,
            tabular_source=self.tabular_source,
            feature_store=self.feature_store,
            dtype=target_dtype,
        )

    def project_feature_from_node(
        self,
        node: Node,
        feature_name: str,
        feature_dtype: DBVarType,
    ) -> Feature:
        """
        Create a Feature object from a node that produces features, such as groupby, lookup, etc.

        Parameters
        ----------
        node: Node
            Query graph node
        feature_name: str
            Feature name
        feature_dtype: DBVarType
            Variable type of the Feature

        Returns
        -------
        Feature
        """
        feature_node = self.graph.add_operation(
            node_type=NodeType.PROJECT,
            node_params={"columns": [feature_name]},
            node_output_type=NodeOutputType.SERIES,
            input_nodes=[node],
        )

        feature = Feature(
            name=feature_name,
            feature_store=self.feature_store,
            tabular_source=self.tabular_source,
            node_name=feature_node.name,
            dtype=feature_dtype,
        )
        return feature

    def _validate_as_features_input_columns(
        self,
        column_names: list[str],
        feature_names: list[str],
    ) -> None:
        if len(column_names) == 0:
            raise ValueError("column_names is empty")

        for column in column_names:
            if column not in self.columns:
                raise ValueError(f"Column '{column}' not found")

        if len(set(feature_names)) != len(feature_names):
            raise ValueError("feature_names contains duplicated value(s)")

        if len(feature_names) != len(column_names):
            raise ValueError(
                f"Length of feature_names ({len(feature_names)}) should be the same as column_names"
                f" ({len(column_names)})"
            )

    def get_input_node_for_lookup_node(self) -> Node:
        """
        Get the node before any projection(s) to be used as the input node for the lookup node in
        as_features(). The view before such projection(s) must also have those columns and can be
        used as the input instead. Removing redundant projections allows joins to be shared for
        lookup operations using the same source.

        Example:

        features_ab = view[columns].as_features(["a", "b"], ["FeatureA", "FeatureB"])
        features_c = view.as_features(["c"], ["FeatureC"])

        When features_ab and features_c are materialized in the same feature list, they can be
        retrieved using the same join query.

        Returns
        -------
        Node
        """
        # Find the first ancestor that is not a Project
        node_before_projection = self.node

        while node_before_projection.type == NodeType.PROJECT:
            input_node_names = self.graph.get_input_node_names(node_before_projection)
            assert len(input_node_names) == 1
            node_before_projection = self.graph.get_node_by_name(input_node_names[0])

        return node_before_projection

    def get_lookup_node_params(
        self, column_names: List[str], feature_names: List[str], offset: Optional[str]
    ) -> Dict[str, Any]:
        """
        Get the parameters for the lookup node in as_features().

        Parameters
        ----------
        column_names: List[str]
            List of column names to be used as input to the lookup node
        feature_names: List[str]
            List of feature names to be used as output of the lookup node
        offset: Optional[str]
            Offset to be used for the lookup node

        Returns
        -------
        Dict[str, Any]

        Raises
        ------
        ValueError
            If the entity_column is not found in the columns_info
        """
        # Get entity_column
        entity_column = self.get_join_column()

        # Get serving_name
        columns_info = self.columns_info
        column_entity_map = {col.name: col.entity_id for col in columns_info if col.entity_id}
        if entity_column not in column_entity_map:
            raise ValueError(f'Column "{entity_column}" is not an entity!')
        entity_id = column_entity_map[entity_column]
        entity = Entity.get_by_id(entity_id)
        serving_name = entity.serving_name

        # Set up Lookup node
        additional_params = self.get_additional_lookup_parameters(offset=offset)
        return {
            "input_column_names": column_names,
            "feature_names": feature_names,
            "entity_column": entity_column,
            "serving_name": serving_name,
            "entity_id": entity_id,
            "offset": offset,
            **additional_params,
        }

    @typechecked
    def as_features(
        self,
        column_names: List[str],
        feature_names: List[str],
        offset: Optional[str] = None,
    ) -> FeatureGroup:
        """
        Creates lookup features directly from the column in the View. The primary entity associated with the features
        is identified by the primary key of the table. However, if the View is a Slowly Changing Dimension (SCD) view,
        the primary entity is identified by the natural key of the table.

        For SCD views, lookup features are materialized through point-in-time joins, and the resulting value represents
        the active row at the point-in-time indicated in the feature request. For instance, a customer feature could
        be the customer's street address at the point-in-time indicated in the request. To obtain a feature value at
        a specific time prior to the request's point-in-time, an offset can be specified. For example, setting the
        offset to 9 weeks would represent the customer's street address 9 weeks prior to the request's point-in-time.

        The returned object is a FeatureGroup which is a temporary collection of Feature objects.

        It is possible to perform additional transformations on the Feature objects, and the Feature objects are added
        to the catalog solely when explicitly saved.

        Parameters
        ----------
        column_names: List[str]
            Column names to be used to create the features
        feature_names: List[str]
            Feature names corresponding to column_names
        offset: Optional[str]
            When specified, retrieve feature values as of this offset prior to the point-in-time

        Returns
        -------
        FeatureGroup

        Examples
        --------
        >>> features = dimension_view.as_features(  # doctest: +SKIP
        ...     column_names=["column_a", "column_b"],
        ...     feature_names=["Feature A", "Feature B"],
        ... )
        >>> features.feature_names  # doctest: +SKIP
        ['Feature A', 'Feature B']
        """
        self._validate_as_features_input_columns(
            column_names=column_names,
            feature_names=feature_names,
        )

        validate_offset(offset)

        lookup_node_params = self.get_lookup_node_params(column_names, feature_names, offset)
        input_node = self.get_input_node_for_lookup_node()
        lookup_node = self.graph.add_operation(
            node_type=NodeType.LOOKUP,
            node_params=lookup_node_params,
            node_output_type=NodeOutputType.FRAME,
            input_nodes=[input_node],
        )
        features = []
        for input_column_name, feature_name in zip(column_names, feature_names):
            feature = self.project_feature_from_node(
                node=lookup_node,
                feature_name=feature_name,
                feature_dtype=self.column_var_type_map[input_column_name],
            )
            features.append(feature)

        return FeatureGroup(features)

    def create_observation_table(
        self,
        name: str,
        sample_rows: Optional[int] = None,
        columns: Optional[list[str]] = None,
        columns_rename_mapping: Optional[dict[str, str]] = None,
        context_name: Optional[str] = None,
        skip_entity_validation_checks: Optional[bool] = False,
        primary_entities: Optional[List[str]] = None,
        target_column: Optional[str] = None,
        sample_from_timestamp: Optional[Union[datetime, str]] = None,
        sample_to_timestamp: Optional[Union[datetime, str]] = None,
    ) -> ObservationTable:
        """
        Creates an ObservationTable from the View.

        When you specify the columns and the columns_rename_mapping parameters, make sure that the table has:

        - a column containing entity values with an accepted serving name.
        - a column containing historical points-in-time in UTC. The column name must be "POINT_IN_TIME".

        Parameters
        ----------
        name: str
            Name of the ObservationTable.
        sample_rows: Optional[int]
            Optionally sample the source table to this number of rows before creating the
            observation table.
        columns: Optional[list[str]]
            Include only these columns in the view when creating the observation table. If None, all
            columns are included.
        columns_rename_mapping: Optional[dict[str, str]]
            Rename columns in the view using this mapping from old column names to new column names
            when creating the observation table. If None, no columns are renamed.
        context_name: Optional[str]
            Context name for the observation table.
        skip_entity_validation_checks: Optional[bool]
            Skip entity validation checks when creating the observation table.
        primary_entities: Optional[List[str]]
            List of primary entities for the observation table. If None, the primary entities are
            inferred from the view.
        target_column: Optional[str]
            Name of the column in the observation table that stores the target values.
            The target column name must match an existing target namespace in the catalog.
            The data type and primary entities must match the those in the target namespace.
        sample_from_timestamp: Optional[Union[datetime, str]]
            Start of date range to sample from.
        sample_to_timestamp: Optional[Union[datetime, str]]
            End of date range to sample from.

        Returns
        -------
        ObservationTable
            ObservationTable object.

        Raises
        ------
        ValueError
            If no primary entities are found.

        Examples
        --------
        >>> observation_table = view.create_observation_table(  # doctest: +SKIP
        ...     name="<observation_table_name>",
        ...     sample_rows=10000,
        ...     columns=["timestamp", "<entity_serving_name>"],
        ...     columns_rename_mapping={"timestamp": "POINT_IN_TIME"},
        ...     context_id=context_id,
        ... )
        """

        from featurebyte.api.context import Context

        context_id = Context.get(context_name).id if context_name else None
        primary_entity_ids = []
        if primary_entities is not None:
            for entity_name in primary_entities:
                primary_entity_ids.append(Entity.get(entity_name).id)
        else:
            # infer the primary entity from the view
            for column_info in self.columns_info:
                if columns and column_info.name not in columns:
                    # for special columns that are inherited in the view (like event_id_column),
                    # exclude them from the primary entity check
                    continue
                if column_info.entity_id:
                    primary_entity_ids.append(column_info.entity_id)

            if not primary_entity_ids:
                raise ValueError(
                    "No primary entities found. Please specify the primary entities when creating the observation table."
                )

        # Validate timestamp inputs
        sample_from_timestamp = (
            validate_datetime_input(sample_from_timestamp) if sample_from_timestamp else None
        )
        sample_to_timestamp = (
            validate_datetime_input(sample_to_timestamp) if sample_to_timestamp else None
        )

        pruned_graph, mapped_node = self.extract_pruned_graph_and_node()
        definition = get_definition_for_obs_table_creation_from_view(
            pruned_graph,
            mapped_node,
            name,
            sample_rows,
            columns,
            columns_rename_mapping,
            context_name,
            skip_entity_validation_checks,
            primary_entities,
            sample_from_timestamp,
            sample_to_timestamp,
        )

        payload = ObservationTableCreate(
            name=name,
            feature_store_id=self.feature_store.id,
            request_input=ViewObservationInput(
                graph=pruned_graph,
                node_name=mapped_node.name,
                columns=columns,
                columns_rename_mapping=columns_rename_mapping,
                definition=definition,
            ),
            sample_rows=sample_rows,
            context_id=context_id,
            skip_entity_validation_checks=skip_entity_validation_checks,
            primary_entity_ids=primary_entity_ids,
            target_column=target_column,
            sample_from_timestamp=sample_from_timestamp,
            sample_to_timestamp=sample_to_timestamp,
        )
        observation_table_doc = ObservationTable.post_async_task(
            route="/observation_table", payload=payload.json_dict()
        )
        return ObservationTable.get_by_id(observation_table_doc["_id"])

    def get_batch_request_input(
        self,
        columns: Optional[list[str]] = None,
        columns_rename_mapping: Optional[dict[str, str]] = None,
    ) -> BatchRequestInput:
        """
        Get a BatchRequestInput object for the SourceTable.

        Parameters
        ----------
        columns: Optional[list[str]]
            Include only these columns in the view for the batch request input. If None,
            all columns are included.
        columns_rename_mapping: Optional[dict[str, str]]
            Rename columns in the view using this mapping from old column names to new column names
            for the batch request input. If None, no columns are renamed.

        Returns
        -------
        BatchRequestInput
            BatchRequestInput object.
        """
        pruned_graph, mapped_node = self.extract_pruned_graph_and_node()
        return ViewBatchRequestInput(
            graph=pruned_graph,
            node_name=mapped_node.name,
            columns=columns,
            columns_rename_mapping=columns_rename_mapping,
        )

    def create_batch_request_table(
        self,
        name: str,
        columns: Optional[list[str]] = None,
        columns_rename_mapping: Optional[dict[str, str]] = None,
    ) -> BatchRequestTable:
        """
        Creates an BatchRequestTable from the View.

        When you specify the columns and the columns_rename_mapping parameters, make sure that the table has a
        column containing entity values with an accepted serving name.

        Parameters
        ----------
        name: str
            Name of the BatchRequestTable.
        columns: Optional[list[str]]
            Include only these columns in the view when creating the batch request table. If None,
            all columns are included.
        columns_rename_mapping: Optional[dict[str, str]]
            Rename columns in the view using this mapping from old column names to new column names
            when creating the batch request table. If None, no columns are renamed.

        Returns
        -------
        BatchRequestTable
            BatchRequestTable object.

        Examples
        --------
        >>> batch_request_table = view.create_batch_request_table(  # doctest: +SKIP
        ...   name="<batch_request_table_name>",
        ...   columns=[<entity_column_name>],
        ...   columns_rename_mapping={
        ...     <entity_column_name>: <entity_serving_name>,
        ...   }
        ... )
        """
        payload = BatchRequestTableCreate(
            name=name,
            feature_store_id=self.feature_store.id,
            request_input=self.get_batch_request_input(
                columns=columns, columns_rename_mapping=columns_rename_mapping
            ),
        )
        batch_request_table_doc = BatchRequestTable.post_async_task(
            route="/batch_request_table", payload=payload.json_dict()
        )
        return BatchRequestTable.get_by_id(batch_request_table_doc["_id"])

    def create_static_source_table(
        self,
        name: str,
        sample_rows: Optional[int] = None,
        columns: Optional[list[str]] = None,
        columns_rename_mapping: Optional[dict[str, str]] = None,
    ) -> StaticSourceTable:
        """
        Creates an StaticSourceTable from the View.

        Parameters
        ----------
        name: str
            Name of the StaticSourceTable.
        sample_rows: Optional[int]
            Optionally sample the source table to this number of rows before creating the
            static source table.
        columns: Optional[list[str]]
            Include only these columns in the view when creating the static source table. If None, all
            columns are included.
        columns_rename_mapping: Optional[dict[str, str]]
            Rename columns in the view using this mapping from old column names to new column names
            when creating the static source table. If None, no columns are renamed.

        Returns
        -------
        StaticSourceTable
            StaticSourceTable object.

        Examples
        --------
        >>> static_source_table = view.create_static_source_table(  # doctest: +SKIP
        ...     name="<static_source_table_name>",
        ...     sample_rows=10000,
        ... )
        """
        pruned_graph, mapped_node = self.extract_pruned_graph_and_node()
        payload = StaticSourceTableCreate(
            name=name,
            feature_store_id=self.feature_store.id,
            request_input=ViewStaticSourceInput(
                graph=pruned_graph,
                node_name=mapped_node.name,
                columns=columns,
                columns_rename_mapping=columns_rename_mapping,
            ),
            sample_rows=sample_rows,
        )
        static_source_table_doc = StaticSourceTable.post_async_task(
            route="/static_source_table", payload=payload.json_dict()
        )
        return StaticSourceTable.get_by_id(static_source_table_doc["_id"])
