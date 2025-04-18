"""
Setup for running doctests.
"""

import featurebyte as fb
from featurebyte import FeatureList, FunctionParameter, MySQLOnlineStoreDetails, UserDefinedFunction
from featurebyte.enum import DBVarType


def setup() -> None:
    """
    Setup featurebyte environment for running doctests.
    """
    # start playground
    fb.playground(datasets=["doctest_grocery"])

    # create online store
    fb.OnlineStore.get_or_create(
        name="mysql_online_store",
        details=MySQLOnlineStoreDetails(
            host="mysql_host",
            database="mysql_database",
            user="mysql_user",
            password="mysql_password",
            port=3306,
        ),
    )

    # create catalog
    feature_store = fb.FeatureStore.get("playground")
    fb.Catalog.get_or_create("grocery", feature_store_name=feature_store.name)
    fb.Catalog.activate("grocery")

    data_source = feature_store.get_data_source()

    # EventTable: GROCERYINVOICE
    if "GROCERYINVOICE" not in fb.Table.list()["name"].tolist():
        event_source_table = data_source.get_source_table(
            database_name="spark_catalog",
            schema_name="doctest_grocery",
            table_name="GROCERYINVOICE",
        )
        grocery_invoice_table = event_source_table.create_event_table(
            name="GROCERYINVOICE",
            event_id_column="GroceryInvoiceGuid",
            event_timestamp_column="Timestamp",
            record_creation_timestamp_column="record_available_at",
        )
        grocery_invoice_table.save(conflict_resolution="retrieve")
    else:
        grocery_invoice_table = fb.Table.get("GROCERYINVOICE")

    # ItemTable: INVOICEITEMS
    if "INVOICEITEMS" not in fb.Table.list()["name"].tolist():
        item_source_table = data_source.get_source_table(
            database_name="spark_catalog", schema_name="doctest_grocery", table_name="INVOICEITEMS"
        )
        grocery_items_table = item_source_table.create_item_table(
            name="INVOICEITEMS",
            event_id_column="GroceryInvoiceGuid",
            item_id_column="GroceryInvoiceItemGuid",
            event_table_name="GROCERYINVOICE",
        )
        grocery_items_table.save(conflict_resolution="retrieve")
    else:
        grocery_items_table = fb.Table.get("INVOICEITEMS")

    # SCDTable: GROCERYCUSTOMER
    if "GROCERYCUSTOMER" not in fb.Table.list()["name"].tolist():
        scd_source_table = data_source.get_source_table(
            database_name="spark_catalog",
            schema_name="doctest_grocery",
            table_name="GROCERYCUSTOMER",
        )
        grocery_customer_table = scd_source_table.create_scd_table(
            name="GROCERYCUSTOMER",
            surrogate_key_column="RowID",
            natural_key_column="GroceryCustomerGuid",
            effective_timestamp_column="ValidFrom",
            current_flag_column="CurrentRecord",
            record_creation_timestamp_column="record_available_at",
        )
        grocery_customer_table.save(conflict_resolution="retrieve")
    else:
        grocery_customer_table = fb.Table.get("GROCERYCUSTOMER")

    # DimensionTable: GROCERYPRODUCT
    if "GROCERYPRODUCT" not in fb.Table.list()["name"].tolist():
        dimension_source_table = data_source.get_source_table(
            database_name="spark_catalog",
            schema_name="doctest_grocery",
            table_name="GROCERYPRODUCT",
        )
        grocery_product_table = dimension_source_table.create_dimension_table(
            name="GROCERYPRODUCT",
            dimension_id_column="GroceryProductGuid",
        )
        grocery_product_table.save(conflict_resolution="retrieve")
    else:
        grocery_product_table = fb.Table.get("GROCERYPRODUCT")

    # register new entities
    fb.Entity.get_or_create(name="grocerycustomer", serving_names=["GROCERYCUSTOMERGUID"])
    fb.Entity.get_or_create(name="groceryinvoice", serving_names=["GROCERYINVOICEGUID"])
    fb.Entity.get_or_create(name="groceryproduct", serving_names=["GROCERYPRODUCTGUID"])
    fb.Entity.get_or_create(name="frenchstate", serving_names=["FRENCHSTATE"])

    # tag the entities for the grocery customer table
    grocery_customer_table.GroceryCustomerGuid.as_entity("grocerycustomer")
    grocery_customer_table.State.as_entity("frenchstate")

    # tag the entities for the grocery invoice table
    grocery_invoice_table.GroceryInvoiceGuid.as_entity("groceryinvoice")
    grocery_invoice_table.GroceryCustomerGuid.as_entity("grocerycustomer")

    # tag the entities for the grocery items table
    grocery_items_table.GroceryInvoiceGuid.as_entity("groceryinvoice")
    grocery_items_table.GroceryProductGuid.as_entity("groceryproduct")

    # tag the entities for the grocery product table
    grocery_product_table.GroceryProductGuid.as_entity("groceryproduct")

    grocery_invoice_table.update_default_feature_job_setting(
        fb.FeatureJobSetting(blind_spot="145", period="60m", offset="90s")
    )

    # Feature: InvoiceCount_60days
    grocery_invoice_view = grocery_invoice_table.get_view()
    invoice_count_60days = (
        grocery_invoice_view.groupby("GroceryCustomerGuid")
        .aggregate_over(
            value_column=None,
            method="count",
            feature_names=["InvoiceCount_60days"],
            windows=["60d"],
        )["InvoiceCount_60days"]
        .astype(float)
    )
    invoice_count_60days.name = "InvoiceCount_60days"
    invoice_count_60days.save(conflict_resolution="retrieve")
    invoice_count_60days.update_readiness("PRODUCTION_READY")

    # Feature: InvoiceAmountAvg_60days
    invoice_amount_avg_60days = grocery_invoice_view.groupby("GroceryCustomerGuid").aggregate_over(
        value_column="Amount",
        method="avg",
        feature_names=["InvoiceAmountAvg_60days"],
        windows=["60d"],
    )["InvoiceAmountAvg_60days"]
    invoice_amount_avg_60days.save(conflict_resolution="retrieve")
    invoice_amount_avg_60days.update_readiness("PRODUCTION_READY")

    # Feature: CustomerProductGroupCounts
    grocery_items_table = fb.Table.get("INVOICEITEMS")
    grocery_item_view = grocery_items_table.get_view()
    grocery_product_view = grocery_product_table.get_view()
    grocery_item_view = grocery_item_view.join(grocery_product_view)
    feature_group = grocery_item_view.groupby(
        "GroceryCustomerGuid", category="ProductGroup"
    ).aggregate_over(
        value_column=None,
        method="count",
        feature_names=["CustomerProductGroupCounts_7d", "CustomerProductGroupCounts_90d"],
        windows=["7d", "90d"],
    )
    feature_group["CustomerProductGroupCounts_7d"].save(conflict_resolution="retrieve")
    feature_group["CustomerProductGroupCounts_90d"].save(conflict_resolution="retrieve")

    feature_group = grocery_item_view.groupby(
        "GroceryCustomerGuid", category="ProductGroup"
    ).aggregate_over(
        value_column="TotalCost",
        method="sum",
        feature_names=["CustomerProductGroupTotalCost_7d"],
        windows=["7d"],
    )
    feature_group["CustomerProductGroupTotalCost_7d"].save(conflict_resolution="retrieve")

    # Feature: ProductGroup (lookup feature)
    product_group_feature = grocery_product_view["ProductGroup"].as_feature(
        feature_name="ProductGroupLookup"
    )
    product_group_feature.save(conflict_resolution="retrieve")

    # Feature: InvoiceCount - non time based
    invoice_count = grocery_item_view.groupby("GroceryInvoiceGuid").aggregate(
        value_column=None,
        method="count",
        feature_name="InvoiceCount",
    )
    invoice_count.save(conflict_resolution="retrieve")

    # Feature: Latest Invoice Timestamp by User
    invoice_latest_timestamp_90d = grocery_invoice_view.groupby(
        "GroceryCustomerGuid"
    ).aggregate_over(
        value_column="Timestamp",
        method="latest",
        feature_names=["CustomerLatestInvoiceTimestamp"],
        windows=["90d"],
    )[
        "CustomerLatestInvoiceTimestamp"
    ]
    invoice_latest_timestamp_90d.save(conflict_resolution="retrieve")
    invoice_latest_timestamp_90d.update_readiness("PRODUCTION_READY")

    # FeatureList:
    FeatureList([invoice_count_60days], name="invoice_feature_list").save(
        conflict_resolution="retrieve"
    )

    # UserDefinedFunction: cos
    UserDefinedFunction.create(
        name="cos",
        sql_function_name="cos",
        function_parameters=[FunctionParameter(name="x", dtype=DBVarType.FLOAT)],
        output_dtype=fb.enum.DBVarType.FLOAT,
        is_global=True,
    )

    # Target - latest invoice timestamp
    target_latest_invoice_timestamp = grocery_invoice_view.groupby(
        "GroceryCustomerGuid"
    ).forward_aggregate(
        value_column="Timestamp",
        method="max",
        window="7d",
        target_name="target_latest_invoice_timestamp",
        fill_value=None,
    )
    target_latest_invoice_timestamp.save(conflict_resolution="retrieve")

    # UseCase setup
    context = fb.Context.create(name="context", primary_entity=["grocerycustomer"])
    fb.UseCase.create(
        name="use_case", target_name=target_latest_invoice_timestamp.name, context_name=context.name
    )


if __name__ == "__main__":
    setup()
