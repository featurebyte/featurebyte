"""
This module contains groupby related class
"""
from __future__ import annotations

from typing import List, Literal, Optional, Union

from typeguard import typechecked

from featurebyte import FeatureJobSetting
from featurebyte.api.aggregator.forward_aggregator import ForwardAggregator
from featurebyte.api.asat_aggregator import AsAtAggregator
from featurebyte.api.change_view import ChangeView
from featurebyte.api.entity import Entity
from featurebyte.api.event_view import EventView
from featurebyte.api.feature import Feature
from featurebyte.api.feature_group import FeatureGroup
from featurebyte.api.item_view import ItemView
from featurebyte.api.scd_view import SCDView
from featurebyte.api.simple_aggregator import SimpleAggregator
from featurebyte.api.target import Target
from featurebyte.api.window_aggregator import WindowAggregator
from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.common.typing import OptionalScalar
from featurebyte.enum import AggFunc


class GroupBy:
    """
    The groupby method of a view returns a GroupBy class that can be used to group data based on one or more columns
    representing entities (specified in the key parameter). Within each entity or group of entities, the GroupBy
    class applies aggregation function(s) to the data.

    The grouping keys determine the primary entity for the declared features in the aggregation function.

    Moreover, the groupby method's category parameter allows you to define a categorical column, which can be used to
    generate Cross Aggregate Features. These features involve aggregating data across categories of the categorical
    column, enabling the extraction of patterns in an entity across these categories. For instance, you can calculate
    the amount spent by a customer on each product category during a specific time period using this approach.

    Examples
    --------
    Groupby for Aggregate features.

    >>> items_view = catalog.get_view("INVOICEITEMS")
    >>> # Group items by the column GroceryCustomerGuid that references the customer entity
    >>> items_by_customer = items_view.groupby("GroceryCustomerGuid")
    >>> # Declare features that measure the discount received by customer
    >>> customer_discounts = items_by_customer.aggregate_over(  # doctest: +SKIP
    ...   "Discount",
    ...   method=fb.AggFunc.SUM,
    ...   feature_names=["CustomerDiscounts_7d", "CustomerDiscounts_28d"],
    ...   fill_value=0,
    ...   windows=['7d', '28d']
    ... )


    Groupby for Cross Aggregate features

    >>> # Join product view to items view
    >>> product_view = catalog.get_view("GROCERYPRODUCT")
    >>> items_view = items_view.join(product_view)
    >>> # Group items by the column GroceryCustomerGuid that references the customer entity
    >>> # And use ProductGroup as the column to perform operations across
    >>> items_by_customer_across_product_group = items_view.groupby(
    ...   by_keys = "GroceryCustomerGuid", category="ProductGroup"
    ... )
    >>> # Cross Aggregate feature of the customer purchases across product group over the past 4 weeks
    >>> customer_inventory_28d = items_by_customer_across_product_group.aggregate_over(
    ...   "TotalCost",
    ...   method=fb.AggFunc.SUM,
    ...   feature_names=["CustomerInventory_28d"],
    ...   windows=['28d']
    ... )
    """

    # documentation metadata
    __fbautodoc__ = FBAutoDoc()

    @typechecked
    def __init__(
        self,
        obj: Union[EventView, ItemView, ChangeView, SCDView],
        keys: Union[str, List[str]],
        category: Optional[str] = None,
    ):
        keys_value = []
        if isinstance(keys, str):
            keys_value.append(keys)
        elif isinstance(keys, list):
            keys_value = keys

        # construct column name entity mapping
        columns_info = obj.columns_info
        column_entity_map = {col.name: col.entity_id for col in columns_info if col.entity_id}

        # construct serving_names
        serving_names = []
        entity_ids = []
        for key in keys_value:
            if key not in obj.columns:
                raise KeyError(f'Column "{key}" not found!')
            if key not in column_entity_map:
                raise ValueError(f'Column "{key}" is not an entity!')

            entity = Entity.get_by_id(column_entity_map[key])
            serving_names.append(entity.serving_name)
            entity_ids.append(entity.id)

        if category is not None and category not in obj.columns:
            raise KeyError(f'Column "{category}" not found!')

        self.view_obj = obj
        self.keys = keys_value
        self.category = category
        self.serving_names = serving_names
        self.entity_ids = entity_ids

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self.view_obj}, keys={self.keys})"

    def __str__(self) -> str:
        return repr(self)

    @typechecked
    def aggregate_over(
        self,
        value_column: Optional[str] = None,
        method: Optional[Literal[tuple(AggFunc)]] = None,  # type: ignore[misc]
        windows: Optional[List[Optional[str]]] = None,
        feature_names: Optional[List[str]] = None,
        timestamp_column: Optional[str] = None,
        feature_job_setting: Optional[FeatureJobSetting] = None,
        fill_value: OptionalScalar = None,
        skip_fill_na: bool = False,
    ) -> FeatureGroup:
        """
        The aggregate_over method of a GroupBy instance returns a FeatureGroup containing Aggregate Over a Window
        Feature objects. These Feature objects aggregate data from the column specified by the value_column parameter,
        using the aggregation method provided by the method parameter. The aggregation is performed within specific
        time frames prior to the point-in-time indicated in the feature request. The time frames are defined by the
        windows parameter. Each Feature object within the FeatureGroup corresponds to a window in the list provided
        by the windows parameter. The primary entity of the Feature is determined by the grouping key of the GroupBy
        instance.

        These features are often used for analyzing event and item data.

        If the GroupBy instance involves computation across a categorical column, the resulting Feature object is a
        Cross Aggregate Over a Window Feature. In this scenario, the feature value after materialization is a
        dictionary with keys representing the categories of the categorical column and their corresponding values
        indicating the aggregated values for each category.

        You can choose to fill the feature value with a default value if the column being aggregated is empty.

        Additional transformations can be performed on the Feature objects, and the Feature objects within the
        FeatureGroup are added to the catalog only when explicitly saved.

        Parameters
        ----------
        value_column: Optional[str]
            Column to be aggregated
        method: Optional[Literal[tuple(AggFunc)]]
            Aggregation method
        windows: List[str]
            List of aggregation window sizes. Use `None` to indicated unbounded window size (only
            applicable to "latest" method). Format of a window size is "{size}{unit}",
            where size is a positive integer and unit is one of the following:

            "ns": nanosecond
            "us": microsecond
            "ms": millisecond
            "s": second
            "m": minute
            "h": hour
            "d": day
            "w": week

            **Note**: Window sizes must be multiples of feature job frequency

        feature_names: List[str]
            Output feature names
        timestamp_column: Optional[str]
            Timestamp column used to specify the window (if not specified, event table timestamp is used)
        feature_job_setting: Optional[FeatureJobSetting]
            Dictionary contains `blind_spot`, `frequency` and `time_modulo_frequency` keys which are
            feature job setting parameters
        fill_value: OptionalScalar
            Value to fill if the value in the column is empty
        skip_fill_na: bool
            Whether to skip filling NaN values

        Returns
        -------
        FeatureGroup

        Examples
        --------
        Sum of discounts by grocerycustomer entity over the past 7 and 28 days.

        >>> items_view = catalog.get_view("INVOICEITEMS")
        >>> # Group items by the column GroceryCustomerGuid that references the customer entity
        >>> items_by_customer = items_view.groupby("GroceryCustomerGuid")
        >>> # Declare features that measure the discount received by customer
        >>> customer_discounts = items_by_customer.aggregate_over(
        ...   "Discount",
        ...   method=fb.AggFunc.SUM,
        ...   feature_names=["CustomerDiscounts_7d", "CustomerDiscounts_28d"],
        ...   fill_value=0,
        ...   windows=['7d', '28d']
        ... )


        Sum spent by grocerycustomer entity across product group over the past 28 days.

        >>> # Join product view to items view
        >>> product_view = catalog.get_view("GROCERYPRODUCT")
        >>> items_view = items_view.join(product_view)
        >>> # Group items by the column GroceryCustomerGuid that references the customer entity
        >>> # And use ProductGroup as the column to perform operations across
        >>> items_by_customer_across_product_group = items_view.groupby(
        ...   by_keys="GroceryCustomerGuid", category="ProductGroup"
        ... )
        >>> # Cross Aggregate feature of the customer purchases across product group over the past 4 weeks
        >>> customer_inventory_28d = items_by_customer_across_product_group.aggregate_over(
        ...   "TotalCost",
        ...   method=fb.AggFunc.SUM,
        ...   feature_names=["CustomerInventory_28d"],
        ...   windows=['28d']
        ... )

        See Also
        --------
        - [FeatureGroup](/reference/featurebyte.api.feature_group.FeatureGroup/): FeatureGroup object
        - [Feature](/reference/featurebyte.api.feature.Feature/): Feature object
        """
        return WindowAggregator(
            self.view_obj, self.category, self.entity_ids, self.keys, self.serving_names
        ).aggregate_over(
            value_column=value_column,
            method=method,
            windows=windows,
            feature_names=feature_names,
            timestamp_column=timestamp_column,
            feature_job_setting=feature_job_setting,
            fill_value=fill_value,
            skip_fill_na=skip_fill_na,
        )

    @typechecked
    def aggregate_asat(
        self,
        value_column: Optional[str] = None,
        method: Optional[Literal[tuple(AggFunc)]] = None,  # type: ignore[misc]
        feature_name: Optional[str] = None,
        offset: Optional[str] = None,
        backward: bool = True,
        fill_value: OptionalScalar = None,
        skip_fill_na: bool = False,
    ) -> Feature:
        """
        The aggregate_as_at method of a GroupBy instance returns an Aggregate ""as at"" Feature object. The object
        aggregates data from the column specified by the value_column parameter using the aggregation method provided
        by the method parameter. By default, the aggrgegation is done on rows active at the point-in-time indicated in
        the feature request. The primary entity of the Feature is determined by the grouping key of the GroupBy
        instance,

        These aggregation operations are exclusively available for Slowly Changing Dimension (SCD) views, and the
        grouping key used in the GroupBy instance should not be the natural key of the SCD view.

        For instance, a possible example of an aggregate ‘as at’ feature from a Credit Cards table could be the
        count of credit cards held by a customer at the point-in-time indicated in the feature request.

        If an offset is defined, the aggregation uses the active rows of the SCD view's data at the point-in-time
        indicated in the feature request, minus the specified offset.

        If the GroupBy instance involves computation across a categorical column, the returned Feature object is a
        Cross Aggregate "as at" Feature. In this scenario, the feature value after materialization is a dictionary
        with keys representing the categories of the categorical column and their corresponding values indicating
        the aggregated values for each category.

        You may choose to fill the feature value with a default value if the column to be aggregated is empty.

        It is possible to perform additional transformations on the Feature object, and the Feature object is added
        to the catalog solely when explicitly saved.

        Parameters
        ----------
        value_column: Optional[str]
            Column to be aggregated
        method: Optional[Literal[tuple(AggFunc)]]
            Aggregation method
        feature_name: str
            Output feature name
        offset: Optional[str]
            Optional offset to apply to the point in time column in the feature request. The
            aggregation result will be as at the point in time adjusted by this offset. Format of
            offset is "{size}{unit}", where size is a positive integer and unit is one of the
            following:

            "ns": nanosecond
            "us": microsecond
            "ms": millisecond
            "s": second
            "m": minute
            "h": hour
            "d": day
            "w": week

        backward: bool
            Whether the offset should be applied backward or forward
        fill_value: OptionalScalar
            Value to fill if the value in the column is empty
        skip_fill_na: bool
            Whether to skip filling NaN values

        Returns
        -------
        Feature

        Examples
        --------
        Count number of active cards per customer at a point-in-time.

        >>> # Filter active cards
        >>> cond = credit_card_accounts['status'] == "active"  # doctest: +SKIP
        >>> # Group by customer
        >>> active_credit_card_by_cust = credit_card_accounts[cond].groupby(  # doctest: +SKIP
        ...   "CustomerID"
        ... )
        >>> feature = active_credit_card_by_cust.aggregate_asat(  # doctest: +SKIP
        ...   method=fb.AggFunc.COUNT,
        ...   feature_name="Number of Active Credit Cards",
        ... )


        Count number of active cards per customer 12 weeks prior to a point-in-time

        >>> feature_12w_before = active_credit_card_by_cust.aggregate_asat(  # doctest: +SKIP
        ...   method=fb.AggFunc.COUNT,
        ...   feature_name="Number of Active Credit Cards 12 w before",
        ...   offset="12w"
        ... )
        """
        return AsAtAggregator(
            self.view_obj, self.category, self.entity_ids, self.keys, self.serving_names
        ).aggregate_asat(
            value_column=value_column,
            method=method,
            feature_name=feature_name,
            offset=offset,
            backward=backward,
            fill_value=fill_value,
            skip_fill_na=skip_fill_na,
        )

    @typechecked
    def aggregate(
        self,
        value_column: Optional[str] = None,
        method: Optional[Literal[tuple(AggFunc)]] = None,  # type: ignore[misc]
        feature_name: Optional[str] = None,
        fill_value: OptionalScalar = None,
        skip_fill_na: bool = False,
    ) -> Feature:
        """
        The aggregate method of a GroupBy class instance returns a Simple Aggregate Feature object. This object
        aggregates data from the column specified by the value_column parameter using the aggregation method
        provided by the method parameter, without taking into account the order or sequence of the data. The primary
        entity of the Feature is determined by the grouping key of the GroupBy instance.

        If the GroupBy class instance involves computation across a categorical column, the resulting Feature object
        is a Simple Cross Aggregate Feature. In this scenario, the feature value after materialization is a dictionary
        with keys representing the categories of the categorical column and their corresponding values indicating the
        aggregated values for each category.

        You can choose to fill the feature value with a default value if the column being aggregated is empty.

        It's important to note that additional transformations can be performed on the Feature object. The Feature
        object is added to the catalog only when explicitly saved.

        To avoid time leakage, simple aggregation is exclusively supported for Item views. This is applicable when
        the grouping key corresponds to the event key of the Item view. An example of such features includes the count
        of items in an Order.

        Parameters
        ----------
        value_column: Optional[str]
            Column to be aggregated
        method: Optional[Literal[tuple(AggFunc)]]
            Aggregation method
        feature_name: Optional[str]
            Output feature name
        fill_value: OptionalScalar
            Value to fill if the value in the column is empty
        skip_fill_na: bool
            Whether to skip filling NaN values

        Returns
        -------
        Feature

        Examples
        --------
        >>> items_view = catalog.get_view("INVOICEITEMS")
        >>> # Group items by the column GroceryInvoiceGuid that references the customer entity
        >>> items_by_invoice = items_view.groupby("GroceryInvoiceGuid")
        >>> # Get the number of items in each invoice
        >>> invoice_item_count = items_by_invoice.aggregate(  # doctest: +SKIP
        ...   None,
        ...   method=fb.AggFunc.COUNT,
        ...   feature_name="InvoiceItemCount",
        ... )
        """
        return SimpleAggregator(
            self.view_obj, self.category, self.entity_ids, self.keys, self.serving_names
        ).aggregate(
            value_column=value_column,
            method=method,
            feature_name=feature_name,
            fill_value=fill_value,
            skip_fill_na=skip_fill_na,
        )

    def forward_aggregate(
        self,
        value_column: str,
        method: str,
        window: Optional[str] = None,
        target_name: Optional[str] = None,
    ) -> Target:
        """
        The forward_aggregate method of a GroupBy class instance returns a Forward Aggregated Target object. This object
        aggregates data from the column specified by the value_column parameter using the aggregation method
        provided by the method parameter, without taking into account the order or sequence of the data. The primary
        entity of the Target is determined by the grouping key of the GroupBy instance.

        Parameters
        ----------
        value_column: str
            Column to be aggregated
        method: str
            Aggregation method.
        window: Optional[str]
            Optional window to apply to the point in time column in the target request.
        target_name: Optional[str]
            Output target name

        Returns
        -------
        Target

        Examples
        --------
        >>> items_view = catalog.get_view("INVOICEITEMS")
        >>> # Group items by the column GroceryInvoiceGuid that references the customer entity
        >>> items_by_invoice = items_view.groupby("GroceryInvoiceGuid")
        >>> # Get the number of items in each invoice
        >>> invoice_item_count = items_by_invoice.forward_aggregate(  # doctest: +SKIP
        ...   "TotalCost",
        ...   method=fb.AggFunc.SUM,
        ...   target_name="TargetCustomerInventory_28d",
        ...   window='28d'
        ... )
        """
        return ForwardAggregator(
            self.view_obj, self.category, self.entity_ids, self.keys, self.serving_names
        ).forward_aggregate(
            value_column=value_column,
            method=method,
            window=window,
            target_name=target_name,
        )
