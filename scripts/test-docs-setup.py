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


if __name__ == "__main__":
    setup()
