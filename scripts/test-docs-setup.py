"""
Setup for running doctests.
"""
import featurebyte as fb
from featurebyte import FeatureList


def setup() -> None:
    """
    Setup featurebyte environment for running doctests.
    """
    # start playground
    fb.playground(local=True, datasets=["grocery"], docs_enabled=False)

    # create catalog
    fb.Catalog.get_or_create("grocery")
    fb.Catalog.activate("grocery")

    data_source = fb.FeatureStore.get("playground").get_data_source()

    # EventTable: GROCERYINVOICE
    if "GROCERYINVOICE" not in fb.Table.list()["name"].tolist():
        event_source_table = data_source.get_table(
            database_name="spark_catalog", schema_name="GROCERY", table_name="GROCERYINVOICE"
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
        item_source_table = data_source.get_table(
            database_name="spark_catalog", schema_name="GROCERY", table_name="INVOICEITEMS"
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
        scd_source_table = data_source.get_table(
            database_name="spark_catalog", schema_name="GROCERY", table_name="GROCERYCUSTOMER"
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
        dimension_source_table = data_source.get_table(
            database_name="spark_catalog", schema_name="GROCERY", table_name="GROCERYPRODUCT"
        )
        dimension_source_table.create_dimension_table(
            name="GROCERYPRODUCT",
            dimension_id_column="GroceryProductGuid",
        ).save(conflict_resolution="retrieve")

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

    grocery_invoice_table.update_default_feature_job_setting(
        fb.FeatureJobSetting(
            blind_spot="145",
            frequency="60m",
            time_modulo_frequency="90s",
        )
    )

    # Feature: InvoiceCount_60days
    grocery_invoice_view = grocery_invoice_table.get_view()
    invoice_count_60days = grocery_invoice_view.groupby("GroceryCustomerGuid").aggregate_over(
        value_column=None, method="count", feature_names=["InvoiceCount_60days"], windows=["60d"]
    )
    invoice_count_60days["InvoiceCount_60days"] = invoice_count_60days[
        "InvoiceCount_60days"
    ].astype(float)
    invoice_count_60days["InvoiceCount_60days"].save(conflict_resolution="retrieve")

    # Feature: InvoiceAmountAvg_60days
    invoice_amount_avg_60days = grocery_invoice_view.groupby("GroceryCustomerGuid").aggregate_over(
        value_column="Amount",
        method="avg",
        feature_names=["InvoiceAmountAvg_60days"],
        windows=["60d"],
    )
    invoice_amount_avg_60days["InvoiceAmountAvg_60days"].save(conflict_resolution="retrieve")

    # FeatureList:
    FeatureList([invoice_count_60days], name="invoice_feature_list").save(
        conflict_resolution="retrieve"
    )


if __name__ == "__main__":
    setup()
