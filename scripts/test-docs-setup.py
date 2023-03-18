"""
Setup for running doctests.
"""

import featurebyte as fb


def setup() -> None:
    """
    Setup featurebyte environment for running doctests.
    """
    # start playground
    fb.playground(local=True, datasets=["grocery"])

    # create catalog
    fb.Catalog.get_or_create("grocery")
    fb.Catalog.activate("grocery")

    data_source = fb.FeatureStore.get("playground").get_data_source()

    # EventTable: GROCERYINVOICE
    if "GROCERYINVOICE" not in fb.Table.list()["name"].tolist():
        grocery_invoice_table = fb.EventTable.from_tabular_source(
            name="GROCERYINVOICE",
            event_id_column="GroceryInvoiceGuid",
            event_timestamp_column="Timestamp",
            record_creation_timestamp_column="record_available_at",
            tabular_source=data_source.get_table(
                database_name="spark_catalog", schema_name="GROCERY", table_name="GROCERYINVOICE"
            ),
        )
        grocery_invoice_table.save(conflict_resolution="retrieve")
    else:
        grocery_invoice_table = fb.Table.get("GROCERYINVOICE")

    # ItemTable: INVOICEITEMS
    if "INVOICEITEMS" not in fb.Table.list()["name"].tolist():
        grocery_items_table = fb.ItemTable.from_tabular_source(
            name="INVOICEITEMS",
            event_id_column="GroceryInvoiceGuid",
            item_id_column="GroceryInvoiceItemGuid",
            event_data_name="GROCERYINVOICE",
            tabular_source=data_source.get_table(
                database_name="spark_catalog", schema_name="GROCERY", table_name="INVOICEITEMS"
            ),
        )
        grocery_items_table.save(conflict_resolution="retrieve")
    else:
        grocery_items_table = fb.Table.get("INVOICEITEMS")

    # SCDTable: GROCERYCUSTOMER
    if "GROCERYCUSTOMER" not in fb.Table.list()["name"].tolist():
        grocery_customer_table = fb.SCDTable.from_tabular_source(
            name="GROCERYCUSTOMER",
            surrogate_key_column="RowID",
            natural_key_column="GroceryCustomerGuid",
            effective_timestamp_column="ValidFrom",
            current_flag_column="CurrentRecord",
            record_creation_timestamp_column="record_available_at",
            tabular_source=data_source.get_table(
                database_name="spark_catalog", schema_name="GROCERY", table_name="GROCERYCUSTOMER"
            ),
        )
        grocery_customer_table.save(conflict_resolution="retrieve")
    else:
        grocery_customer_table = fb.Table.get("GROCERYCUSTOMER")

    # register new entities
    entity1 = fb.Entity(name="grocerycustomer", serving_names=["GROCERYCUSTOMERGUID"])
    entity1.save(conflict_resolution="retrieve")
    entity2 = fb.Entity(name="groceryinvoice", serving_names=["GROCERYINVOICEGUID"])
    entity2.save(conflict_resolution="retrieve")
    entity3 = fb.Entity(name="groceryproduct", serving_names=["GROCERYPRODUCTGUID"])
    entity3.save(conflict_resolution="retrieve")
    entity4 = fb.Entity(name="frenchstate", serving_names=["FRENCHSTATE"])
    entity4.save(conflict_resolution="retrieve")

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
    invoice_count_60days["InvoiceCount_60days"].save(conflict_resolution="retrieve")


if __name__ == "__main__":
    setup()
