# load the featurebyte SDK
# load the regular expressions module
import re

# load the datetime module
from datetime import datetime

# load enums
from enum import Enum

import featurebyte as fb


# define an enum class containing the names of the prebuilt catalogs
class PrebuiltCatalog(Enum):
    QuickStartFeatureEngineeering = 1
    DeepDiveFeatureEngineeering = 2
    Playground_CreditCard = 3
    DeepDiveMaterializingFeatures = 4
    QuickStartReusingFeatures = 5
    Playground_Healthcare = 6
    QuickStartFeatureManagement = 7
    QuickStartModelTraining = 8


def create_tutorial_catalog(catalog_type: PrebuiltCatalog):
    print("Cleaning up existing tutorial catalogs")
    clean_catalogs()

    if catalog_type == PrebuiltCatalog.QuickStartFeatureEngineeering:
        return create_quick_start_feature_engineering_catalog()
    if catalog_type == PrebuiltCatalog.DeepDiveFeatureEngineeering:
        return create_deep_dive_feature_engineering_catalog()
    if catalog_type == PrebuiltCatalog.Playground_CreditCard:
        return create_playground_credit_card_catalog()
    if catalog_type == PrebuiltCatalog.DeepDiveMaterializingFeatures:
        return create_deep_dive_materializing_features_catalog()
    if catalog_type == PrebuiltCatalog.QuickStartReusingFeatures:
        return create_quick_start_reusing_features_catalog()
    if catalog_type == PrebuiltCatalog.Playground_Healthcare:
        return create_playground_healthcare_catalog()
    if catalog_type == PrebuiltCatalog.QuickStartFeatureManagement:
        return create_quick_start_feature_management_catalog()
    if catalog_type == PrebuiltCatalog.QuickStartModelTraining:
        return create_quick_start_model_training_catalog()

    print("Error: Prebuilt catalog type not found")
    return None


def to_python_variable_name(name: str):
    result = name.lower().replace(" ", "_").replace("-", "_")
    if result[0].isdigit():
        result = "digit_" + result
    # replace any non-alphanumeric characters with an underscore
    result = re.sub(r"\W+", "_", result)
    # make the characters lower case
    result = result.lower()
    return result


def is_tutorial_catalog(catalog_name):
    # does the catalog name contain playground? if so, it is not a tutorial catalog
    if catalog_name.lower().find("playground") != -1:
        return False

    # does the catalog name begin with "quick start " or "deep dive "? if so, it is probably a tutorial catalog
    if not (
        catalog_name.lower().startswith("quick start ")
        or catalog_name.lower().startswith("deep dive ")
    ):
        return True

    return True


def clean_catalogs(verbose=True):
    # get active catalog
    current_catalog = fb.Catalog.get_active()

    cleaned = False

    # loop through the catalogs
    for catalog_name in fb.list_catalogs().name:
        if is_tutorial_catalog(catalog_name):
            temp_catalog = fb.Catalog.get(catalog_name)

            # get lists of each object type that may need to be removed
            deployments = temp_catalog.list_deployments()
            batch_feature_tables = temp_catalog.list_batch_feature_tables()
            batch_request_tables = temp_catalog.list_batch_request_tables()
            historical_feature_tables = temp_catalog.list_historical_feature_tables()
            observation_tables = temp_catalog.list_observation_tables()

            # get a count of existing objects
            num_deployments = 0
            for id in deployments.id:
                deployment = temp_catalog.get_deployment_by_id(id)
                if deployment.enabled:
                    num_deployments = num_deployments + 1
            num_batch_feature_tables = batch_feature_tables.shape[0]
            num_batch_request_tables = batch_request_tables.shape[0]
            num_historical_feature_tables = historical_feature_tables.shape[0]
            num_observation_tables = observation_tables.shape[0]

            if (
                num_deployments
                + num_batch_feature_tables
                + num_batch_request_tables
                + num_historical_feature_tables
                + num_observation_tables
                > 0
            ):
                if verbose:
                    print("Cleaning catalog: " + catalog_name)

                    if num_deployments > 0:
                        print(f"  {num_deployments} deployments")
                    if num_batch_feature_tables > 0:
                        print(f"  {num_batch_feature_tables} batch feature tables")
                    if num_batch_request_tables:
                        print(f"  {num_batch_request_tables} batch request tables")
                    if num_historical_feature_tables > 0:
                        print(f"  {num_historical_feature_tables} historical feature tables")
                    if num_observation_tables > 0:
                        print(f"  {num_observation_tables} observation tables")

                temp_catalog = fb.activate_and_get_catalog(temp_catalog.name)

                for id in deployments.id:
                    deployment = temp_catalog.get_deployment_by_id(id)
                    if deployment.enabled:
                        deployment.disable()

                for id in batch_feature_tables.id:
                    table = temp_catalog.get_batch_feature_table_by_id(id)
                    table.delete()

                for id in batch_request_tables.id:
                    table = temp_catalog.get_batch_request_table_by_id(id)
                    table.delete()

                for id in historical_feature_tables.id:
                    table = temp_catalog.get_historical_feature_table_by_id(id)
                    table.delete()

                for id in observation_tables.id:
                    table = temp_catalog.get_observation_table_by_id(id)
                    table.delete()

                cleaned = True

    if cleaned and current_catalog:
        catalog = fb.activate_and_get_catalog(current_catalog.name)


def generate_catalog_boilerplate_code(catalog):
    tables = catalog.list_tables()
    print("")
    print("##################################################################")
    print("# suggested script to load the tables and views into your notebook")
    print("")
    print("# get the table objects")
    # loop through the rows of table
    for i in range(tables.shape[0]):
        # get the table name
        table_name = tables.iloc[i]["name"]
        print(
            to_python_variable_name(table_name) + '_table = catalog.get_table("' + table_name + '")'
        )
    print("")
    print("# get the view objects")
    # loop through the rows of table
    for i in range(tables.shape[0]):
        # get the table name
        table_name = tables.iloc[i]["name"]
        print(
            to_python_variable_name(table_name)
            + "_view = "
            + table_name.lower()
            + "_table.get_view()"
        )
    print("")
    print("##################################################################")


def register_grocery_tables():
    # connect to the feature store
    # get data source from the local spark feature store
    ds = fb.FeatureStore.get("playground").get_data_source()

    # get the active catalog
    catalog = fb.Catalog.get_active()

    # check whether the customer data is already registered
    # check whether the data is already registered
    if not catalog.list_tables().name.str.contains("GROCERYCUSTOMER").any():
        GroceryCustomer = ds.get_source_table(
            database_name="spark_catalog", schema_name="GROCERY", table_name="GROCERYCUSTOMER"
        ).create_scd_table(
            name="GROCERYCUSTOMER",
            surrogate_key_column="RowID",
            natural_key_column="GroceryCustomerGuid",
            effective_timestamp_column="ValidFrom",
            current_flag_column="CurrentRecord",
            record_creation_timestamp_column="record_available_at",
        )
    else:
        GroceryCustomer = catalog.get_source_table("GROCERYCUSTOMER")

    # check whether the data is already registered
    if not catalog.list_tables().name.str.contains("GROCERYINVOICE").any():
        # register GroceryInvoice as an event data
        GroceryInvoice = ds.get_source_table(
            database_name="spark_catalog", schema_name="GROCERY", table_name="GROCERYINVOICE"
        ).create_event_table(
            name="GROCERYINVOICE",
            event_id_column="GroceryInvoiceGuid",
            event_timestamp_column="Timestamp",
            event_timestamp_timezone_offset_column="tz_offset",
            record_creation_timestamp_column="record_available_at",
        )
    else:
        GroceryInvoice = catalog.get_source_table("GROCERYINVOICE")

    # choose conservative feature job settings - tell featurebyte that the GroceryInvoice event data is updated 1.5 minutes after the end of each hour, and may miss data from the last 145 seconds of each hour
    GroceryInvoice.update_default_feature_job_setting(
        fb.FeatureJobSetting(
            blind_spot="145",
            frequency="60m",
            time_modulo_frequency="90s",
        )
    )

    # check whether the data is already registered
    if not catalog.list_tables().name.str.contains("INVOICEITEMS").any():
        InvoiceItems = ds.get_source_table(
            database_name="spark_catalog", schema_name="GROCERY", table_name="INVOICEITEMS"
        ).create_item_table(
            name="INVOICEITEMS",
            event_id_column="GroceryInvoiceGuid",
            item_id_column="GroceryInvoiceItemGuid",
            event_table_name="GROCERYINVOICE",
        )
    else:
        InvoiceItems = catalog.get_source_table("INVOICEITEMS")

    # check whether the grocery product data is already registered
    # check whether the data is already registered
    if not catalog.list_tables().name.str.contains("GROCERYPRODUCT").any():
        GroceryProduct = ds.get_source_table(
            database_name="spark_catalog", schema_name="GROCERY", table_name="GROCERYPRODUCT"
        ).create_dimension_table(name="GROCERYPRODUCT", dimension_id_column="GroceryProductGuid")
    else:
        GroceryProduct = catalog.get_source_table("GROCERYPRODUCT")

    return [GroceryCustomer, GroceryInvoice, InvoiceItems, GroceryProduct]


def register_credit_card_tables():
    # connect to the feature store
    # get data source from the local spark feature store
    ds = fb.FeatureStore.get("playground").get_data_source()

    # get the active catalog
    catalog = fb.Catalog.get_active()

    # check whether the customer data is already registered
    # check whether the data is already registered
    if not catalog.list_tables().name.str.contains("BANKCUSTOMER").any():
        BankCustomer = ds.get_source_table(
            database_name="spark_catalog", schema_name="CREDITCARD", table_name="BANKCUSTOMER"
        ).create_scd_table(
            name="BANKCUSTOMER",
            surrogate_key_column="RowID",
            natural_key_column="BankCustomerID",
            effective_timestamp_column="ValidFrom",
            end_timestamp_column="ValidTo",
            record_creation_timestamp_column="record_available_at",
        )
    else:
        BankCustomer = catalog.get_source_table("BANKCUSTOMER")

    # check whether the state details data is already registered
    # check whether the data is already registered
    if not catalog.list_tables().name.str.contains("STATEDETAILS").any():
        StateDetails = ds.get_source_table(
            database_name="spark_catalog", schema_name="CREDITCARD", table_name="STATEDETAILS"
        ).create_scd_table(
            name="STATEDETAILS",
            surrogate_key_column="StateGuid",
            natural_key_column="StateCode",
            effective_timestamp_column="ValidFrom",
        )
    else:
        StateDetails = catalog.get_source_table("STATEDETAILS")

    # check whether the data is already registered
    if not catalog.list_tables().name.str.contains("CREDITCARD").any():
        CreditCard = ds.get_source_table(
            database_name="spark_catalog", schema_name="CREDITCARD", table_name="CREDITCARD"
        ).create_scd_table(
            name="CREDITCARD",
            surrogate_key_column="RowID",
            natural_key_column="AccountID",
            effective_timestamp_column="ValidFrom",
            end_timestamp_column="ValidTo",
        )
    else:
        CreditCard = catalog.get_source_table("CREDITCARD")

    # check whether the data is already registered
    if not catalog.list_tables().name.str.contains("CARDTRANSACTIONS").any():
        # register GroceryInvoice as an event data
        CardTransactions = ds.get_source_table(
            database_name="spark_catalog", schema_name="CREDITCARD", table_name="CARDTRANSACTIONS"
        ).create_event_table(
            name="CARDTRANSACTIONS",
            event_id_column="CardTransactionID",
            event_timestamp_column="Timestamp",
            event_timestamp_timezone_offset_column="tz_offset",
            record_creation_timestamp_column="record_available_at",
        )
    else:
        CardTransactions = catalog.get_source_table("CARDTRANSACTIONS")

    # choose reasonable feature job settings - based upon the feature job analysis
    CardTransactions.update_default_feature_job_setting(
        fb.FeatureJobSetting(
            blind_spot="120s",
            frequency="3600s",
            time_modulo_frequency="65s",
        )
    )

    # check whether the data is already registered
    if not catalog.list_tables().name.str.contains("CARDFRAUDSTATUS").any():
        CardFraudStatus = ds.get_source_table(
            database_name="spark_catalog", schema_name="CREDITCARD", table_name="CARDFRAUDSTATUS"
        ).create_scd_table(
            name="CARDFRAUDSTATUS",
            surrogate_key_column="RowID",
            natural_key_column="CardTransactionID",
            effective_timestamp_column="ValidFrom",
            end_timestamp_column="ValidTo",
            record_creation_timestamp_column="record_available_at",
        )
    else:
        CardFraudStatus = catalog.get_source_table("CARDFRAUDSTATUS")

    # check whether the data is already registered
    if not catalog.list_tables().name.str.contains("CARDTRANSACTIONGROUPS").any():
        CardTransactionGroups = ds.get_source_table(
            database_name="spark_catalog",
            schema_name="CREDITCARD",
            table_name="CARDTRANSACTIONGROUPS",
        ).create_dimension_table(
            name="CARDTRANSACTIONGROUPS", dimension_id_column="CardTransactionDescription"
        )
    else:
        CardTransactionGroups = catalog.get_source_table("CARDTRANSACTIONGROUPS")

    return [
        BankCustomer,
        StateDetails,
        CreditCard,
        CardTransactions,
        CardFraudStatus,
        CardTransactionGroups,
    ]


def register_healthcare_tables():
    # connect to the feature store
    # get data source from the local spark feature store
    ds = fb.FeatureStore.get("playground").get_data_source()

    # get the active catalog
    catalog = fb.Catalog.get_active()

    # check whether the data is already registered
    source_table_name = "PATIENT"
    if not catalog.list_tables().name.str.contains(source_table_name).any():
        Patient = ds.get_source_table(
            database_name="spark_catalog", schema_name="HEALTHCARE", table_name=source_table_name
        ).create_scd_table(
            name=source_table_name,
            surrogate_key_column="RowID",
            natural_key_column="PatientGuid",
            effective_timestamp_column="ValidFrom",
            current_flag_column="CurrentRecord",
            record_creation_timestamp_column="record_available_at",
        )
    else:
        Patient = catalog.get_source_table(source_table_name)

    # check whether the data is already registered
    source_table_name = "DIAGNOSIS"
    if not catalog.list_tables().name.str.contains(source_table_name).any():
        Diagnosis = ds.get_source_table(
            database_name="spark_catalog", schema_name="HEALTHCARE", table_name=source_table_name
        ).create_scd_table(
            name=source_table_name,
            surrogate_key_column="RowID",
            natural_key_column="DiagnosisGuid",
            effective_timestamp_column="ValidFrom",
            end_timestamp_column="ValidTo",
            record_creation_timestamp_column="record_available_at",
        )
    else:
        Diagnosis = catalog.get_source_table(source_table_name)

    # check whether the data is already registered
    source_table_name = "PATIENTSMOKINGSTATUS"
    if not catalog.list_tables().name.str.contains(source_table_name).any():
        PatientSmokingStatus = ds.get_source_table(
            database_name="spark_catalog", schema_name="HEALTHCARE", table_name=source_table_name
        ).create_scd_table(
            name=source_table_name,
            # surrogate_key_column='RowID',
            natural_key_column="PatientSmokingStatusGuid",
            effective_timestamp_column="ValidFrom",
            current_flag_column="CurrentRecord",
            record_creation_timestamp_column="record_available_at",
        )
    else:
        PatientSmokingStatus = catalog.get_source_table(source_table_name)

    # check whether the data is already registered
    source_table_name = "ALLERGY"
    if not catalog.list_tables().name.str.contains(source_table_name).any():
        Allergy = ds.get_source_table(
            database_name="spark_catalog", schema_name="HEALTHCARE", table_name=source_table_name
        ).create_scd_table(
            name=source_table_name,
            # surrogate_key_column='AllergyGuid',
            natural_key_column="AllergyGuid",
            effective_timestamp_column="StartDate",
            record_creation_timestamp_column="record_available_at",
        )
    else:
        Allergy = catalog.get_source_table(source_table_name)

    # check whether the data is already registered
    source_table_name = "STATEDETAILS"
    if not catalog.list_tables().name.str.contains(source_table_name).any():
        StateDetails = ds.get_source_table(
            database_name="spark_catalog", schema_name="HEALTHCARE", table_name=source_table_name
        ).create_scd_table(
            name=source_table_name,
            surrogate_key_column="StateGuid",
            natural_key_column="StateCode",
            effective_timestamp_column="ValidFrom",
        )
    else:
        StateDetails = catalog.get_source_table(source_table_name)

    # check whether the data is already registered
    source_table_name = "VISIT"
    if not catalog.list_tables().name.str.contains(source_table_name).any():
        # register GroceryInvoice as an event data
        Visit = ds.get_source_table(
            database_name="spark_catalog", schema_name="HEALTHCARE", table_name=source_table_name
        ).create_event_table(
            name=source_table_name,
            event_id_column="VisitGuid",
            event_timestamp_column="VisitDate",
            event_timestamp_timezone_offset_column="tz_offset",
            record_creation_timestamp_column="record_available_at",
        )
    else:
        Visit = catalog.get_source_table(source_table_name)

    # choose reasonable feature job settings - based upon the feature job analysis
    Visit.update_default_feature_job_setting(
        fb.FeatureJobSetting(
            blind_spot="18h",
            frequency="24h",
            time_modulo_frequency="5s",
        )
    )

    # check whether the data is already registered
    source_table_name = "PRESCRIPTION"
    if not catalog.list_tables().name.str.contains(source_table_name).any():
        # register GroceryInvoice as an event data
        Prescription = ds.get_source_table(
            database_name="spark_catalog", schema_name="HEALTHCARE", table_name=source_table_name
        ).create_event_table(
            name=source_table_name,
            event_id_column="PrescriptionGuid",
            event_timestamp_column="PrescriptionDate",
            event_timestamp_timezone_offset_column="tz_offset",
            record_creation_timestamp_column="record_available_at",
        )
    else:
        Prescription = catalog.get_source_table(source_table_name)

    # choose reasonable feature job settings - based upon the feature job analysis
    Prescription.update_default_feature_job_setting(
        fb.FeatureJobSetting(
            blind_spot="18h",
            frequency="24h",
            time_modulo_frequency="5s",
        )
    )

    # check whether the data is already registered
    source_table_name = "LABRESULT"
    if not catalog.list_tables().name.str.contains(source_table_name).any():
        # register GroceryInvoice as an event data
        LabResult = ds.get_source_table(
            database_name="spark_catalog", schema_name="HEALTHCARE", table_name=source_table_name
        ).create_event_table(
            name=source_table_name,
            event_id_column="LabResultGuid",
            event_timestamp_column="ReportDate",
            event_timestamp_timezone_offset_column="tz_offset",
            record_creation_timestamp_column="record_available_at",
        )
    else:
        LabResult = catalog.get_source_table(source_table_name)

    # choose reasonable feature job settings - based upon the feature job analysis
    LabResult.update_default_feature_job_setting(
        fb.FeatureJobSetting(
            blind_spot="18h",
            frequency="24h",
            time_modulo_frequency="5s",
        )
    )

    # check whether the data is already registered
    source_table_name = "LABOBSERVATION"
    if not catalog.list_tables().name.str.contains(source_table_name).any():
        LabObservation = ds.get_source_table(
            database_name="spark_catalog", schema_name="HEALTHCARE", table_name=source_table_name
        ).create_item_table(
            name=source_table_name,
            event_id_column="LabResultGuid",
            item_id_column="LabObservationGuid",
            event_table_name="LABRESULT",
        )
    else:
        LabObservation = catalog.get_source_table(source_table_name)

    # check whether the data is already registered
    source_table_name = "ICD9HIERARCHY"
    if not catalog.list_tables().name.str.contains(source_table_name).any():
        ICD9Hierarchy = ds.get_source_table(
            database_name="spark_catalog", schema_name="HEALTHCARE", table_name=source_table_name
        ).create_dimension_table(name=source_table_name, dimension_id_column="ICD9Code")
    else:
        ICD9Hierarchy = catalog.get_source_table(source_table_name)

    # check whether the data is already registered
    source_table_name = "SPECIALTYGROUP"
    if not catalog.list_tables().name.str.contains(source_table_name).any():
        SpecialtyGroup = ds.get_source_table(
            database_name="spark_catalog", schema_name="HEALTHCARE", table_name=source_table_name
        ).create_dimension_table(name=source_table_name, dimension_id_column="PhysicianSpecialty")
    else:
        SpecialtyGroup = catalog.get_source_table(source_table_name)

    # check whether the data is already registered
    source_table_name = "MEDICALPRODUCT"
    if not catalog.list_tables().name.str.contains(source_table_name).any():
        MedicalProduct = ds.get_source_table(
            database_name="spark_catalog", schema_name="HEALTHCARE", table_name=source_table_name
        ).create_dimension_table(name=source_table_name, dimension_id_column="NdcCode")
    else:
        MedicalProduct = catalog.get_source_table(source_table_name)

    return [
        Patient,
        Diagnosis,
        PatientSmokingStatus,
        Allergy,
        StateDetails,
        Visit,
        Prescription,
        LabResult,
        LabObservation,
        ICD9Hierarchy,
        SpecialtyGroup,
        MedicalProduct,
    ]


def register_grocery_entities():
    # register new entities
    entity1 = fb.Entity.get_or_create(name="grocerycustomer", serving_names=["GROCERYCUSTOMERGUID"])
    entity2 = fb.Entity.get_or_create(name="groceryinvoice", serving_names=["GROCERYINVOICEGUID"])
    entity3 = fb.Entity.get_or_create(name="groceryproduct", serving_names=["GROCERYPRODUCTGUID"])
    entity4 = fb.Entity.get_or_create(name="frenchstate", serving_names=["FRENCHSTATE"])


def register_credit_card_entities():
    # register new entities
    entity1 = fb.Entity.get_or_create(name="bank_customer", serving_names=["BANKCUSTOMERID"])
    entity2 = fb.Entity.get_or_create(name="USA_state", serving_names=["STATECODE"])
    entity3 = fb.Entity.get_or_create(name="credit_card", serving_names=["ACCOUNTID"])
    entity4 = fb.Entity.get_or_create(name="card_transaction", serving_names=["CARDTRANSACTIONID"])
    entity5 = fb.Entity.get_or_create(
        name="card_transaction_description", serving_names=["CARDTRANSACTIONDESCRIPTION"]
    )
    entity6 = fb.Entity.get_or_create(name="gender", serving_names=["GENDER"])


def register_healthcare_entities():
    # register new entities
    entity1 = fb.Entity.get_or_create(name="patient", serving_names=["PATIENTGUID"])
    entity2 = fb.Entity.get_or_create(name="USA_state", serving_names=["STATECODE"])
    entity3 = fb.Entity.get_or_create(
        name="patient_smoking_status", serving_names=["PATIENTSMOKINGSTATUSGUID"]
    )
    entity4 = fb.Entity.get_or_create(name="prescription", serving_names=["PRESCRIPTIONGUID"])
    entity5 = fb.Entity.get_or_create(name="ndc_code", serving_names=["NDCCODE"])
    entity6 = fb.Entity.get_or_create(name="allergy", serving_names=["ALLERGYGUID"])
    entity7 = fb.Entity.get_or_create(name="visit", serving_names=["VISITGUID"])
    entity8 = fb.Entity.get_or_create(
        name="physician_specialty", serving_names=["PHYSICIANSPECIALTY"]
    )
    entity8 = fb.Entity.get_or_create(name="diagnosis", serving_names=["DIAGNOSISGUID"])
    entity9 = fb.Entity.get_or_create(name="icd9", serving_names=["ICD9CODE"])
    entity9 = fb.Entity.get_or_create(name="lab_result", serving_names=["LABRESULTGUID"])
    entity10 = fb.Entity.get_or_create(name="lab_observation", serving_names=["LABOBSERVATIONGUID"])
    entity11 = fb.Entity.get_or_create(name="gender", serving_names=["GENDER"])


def tag_grocery_entities_to_columns(
    grocery_customer_table, grocery_invoice_table, grocery_items_table, grocery_product_table
):
    # tag the entities for the grocery customer table
    # tag columns as entities
    grocery_customer_table.GroceryCustomerGuid.as_entity("grocerycustomer")
    grocery_customer_table.State.as_entity("frenchstate")

    # tag the entities for the grocery invoice table
    # tag columns as entities
    grocery_invoice_table.GroceryInvoiceGuid.as_entity("groceryinvoice")
    grocery_invoice_table.GroceryCustomerGuid.as_entity("grocerycustomer")

    # tag the entities for the grocery items table
    # tag columns as entities
    grocery_items_table.GroceryInvoiceGuid.as_entity("groceryinvoice")
    grocery_items_table.GroceryProductGuid.as_entity("groceryproduct")

    # tag the entities for the grocery items table
    # tag columns as entities
    grocery_product_table.GroceryProductGuid.as_entity("groceryproduct")


def tag_credit_card_entities_to_columns(
    bank_customer_table,
    state_details_table,
    credit_card_table,
    card_transaction_table,
    card_fraud_status_table,
    card_transaction_group_table,
):
    # tag the entities for the bank customer table
    bank_customer_table.BankCustomerID.as_entity("bank_customer")
    bank_customer_table.StateCode.as_entity("USA_state")
    bank_customer_table.Gender.as_entity("gender")

    # tag the entities for the state details table
    state_details_table.StateCode.as_entity("USA_state")

    # tag the entities for the credit card table
    credit_card_table.AccountID.as_entity("credit_card")
    credit_card_table.BankCustomerID.as_entity("bank_customer")

    # tag the entities for the card transaction table
    card_transaction_table.CardTransactionID.as_entity("card_transaction")
    card_transaction_table.AccountID.as_entity("credit_card")
    card_transaction_table.CardTransactionDescription.as_entity("card_transaction_description")

    # tag the entities for the card fraud status table
    card_fraud_status_table.CardTransactionID.as_entity("card_transaction")

    # tag the entities for the card transaction group table
    card_transaction_group_table.CardTransactionDescription.as_entity(
        "card_transaction_description"
    )


def tag_healthcare_entities_to_columns(
    Patient,
    Diagnosis,
    PatientSmokingStatus,
    Allergy,
    StateDetails,
    Visit,
    Prescription,
    LabResult,
    LabObservation,
    ICD9Hierarchy,
    SpecialtyGroup,
    MedicalProduct,
):
    # tag the entities for the patient table
    Patient.PatientGuid.as_entity("patient")
    Patient.StateCode.as_entity("USA_state")

    # tag the entities for the diagnosis table
    Diagnosis.DiagnosisGuid.as_entity("diagnosis")
    Diagnosis.ICD9Code.as_entity("icd9")
    Diagnosis.PatientGuid.as_entity("patient")

    # tag the entities for the patient smoking status table
    PatientSmokingStatus.PatientSmokingStatusGuid.as_entity("patient_smoking_status")
    PatientSmokingStatus.PatientGuid.as_entity("patient")

    # tag the entities for the allergy table
    Allergy.AllergyGuid.as_entity("allergy")
    Allergy.PatientGuid.as_entity("patient")

    # tag the entities for the state details table
    StateDetails.StateCode.as_entity("USA_state")

    # tag the entities for the visit table
    Visit.VisitGuid.as_entity("visit")
    Visit.PatientGuid.as_entity("patient")
    Visit.PhysicianSpecialty.as_entity("physician_specialty")

    # tag the entities for the prescription table
    Prescription.PrescriptionGuid.as_entity("prescription")
    Prescription.PatientGuid.as_entity("patient")
    Prescription.NdcCode.as_entity("ndc_code")

    # tag the entities for the lab result table
    LabResult.LabResultGuid.as_entity("lab_result")
    LabResult.PatientGuid.as_entity("patient")

    # tag the entities for the lab observation table
    LabObservation.LabObservationGuid.as_entity("lab_observation")
    LabObservation.LabResultGuid.as_entity("lab_result")

    # tag the entities for the ICD9 hierarchy table
    ICD9Hierarchy.ICD9Code.as_entity("icd9")

    # tag the entities for the specialty group table
    SpecialtyGroup.PhysicianSpecialty.as_entity("physician_specialty")

    # tag the entities for the medical product table
    MedicalProduct.NdcCode.as_entity("ndc_code")


def create_quick_start_feature_engineering_catalog():
    catalog_name = "quick start feature engineering " + datetime.now().strftime("%Y%m%d:%H%M")

    print("Building a quick start catalog for feature engineering named [" + catalog_name + "]")

    # get a list of catalogs
    catalog_list = fb.Catalog.list()

    # check whether catalog_name exists in the name column of catalog_list
    if catalog_name in catalog_list["name"].values:
        print("Catalog already exists")
    else:
        print("Creating new catalog")
        # creating a catalog activates it
        catalog = fb.Catalog.create(catalog_name, "playground")
        print("Catalog created")
    catalog = fb.activate_and_get_catalog(catalog_name)

    print("Registering the source tables")
    [
        grocery_customer_table,
        grocery_invoice_table,
        grocery_items_table,
        grocery_product_table,
    ] = register_grocery_tables()

    print("Registering the entities")
    register_grocery_entities()

    print("Tagging the entities to columns in the data tables")
    tag_grocery_entities_to_columns(
        grocery_customer_table, grocery_invoice_table, grocery_items_table, grocery_product_table
    )

    print("Populating the feature store with example features")

    # create an event view for the grocery invoice table
    grocery_invoice_view = grocery_invoice_table.get_view()

    # create an item data view for the grocery items table
    grocery_items_view = grocery_items_table.get_view()

    # count the number of invoices per customer over the past 60 days
    # the target is the number of invoices in the next 30 days, grouped by customer
    invoice_count_60days = grocery_invoice_view.groupby("GroceryCustomerGuid").aggregate_over(
        None,
        method=fb.AggFunc.COUNT,
        feature_names=["CustomerInvoiceCount_60days"],
        windows=["60d"],
    )

    # change from a FeatureGroup to a Feature
    invoice_count_60days = invoice_count_60days["CustomerInvoiceCount_60days"]

    # save the feature
    invoice_count_60days.save(conflict_resolution="retrieve")

    # get the cross-aggregation of the items purchased over the past 28 days, grouped by customer
    customer_inventory_28d = grocery_items_view.groupby(
        "GroceryCustomerGuid", category="GroceryProductGuid"
    ).aggregate_over(
        None, method=fb.AggFunc.COUNT, feature_names=["CustomerInventory_28d"], windows=["28d"]
    )
    customer_inventory_28d = customer_inventory_28d["CustomerInventory_28d"]

    # get the entropy of the inventory
    customer_inventory_entropy_28d = customer_inventory_28d.cd.entropy()
    customer_inventory_entropy_28d.name = "CustomerPurchasedItemsEntropy_28d"

    # save the feature
    customer_inventory_entropy_28d.save(conflict_resolution="retrieve")

    print("Catalog created and pre-populated with data and features")

    return catalog


def create_deep_dive_feature_engineering_catalog():
    catalog_name = "deep dive feature engineering " + datetime.now().strftime("%Y%m%d:%H%M")

    print("Building a deep dive catalog for feature engineering named [" + catalog_name + "]")

    # get a list of catalogs
    catalog_list = fb.Catalog.list()

    # check whether catalog_name exists in the name column of catalog_list
    if catalog_name in catalog_list["name"].values:
        print("Catalog already exists")
    else:
        print("Creating new catalog")
        # creating a catalog activates it
        catalog = fb.Catalog.create(catalog_name, "playground")
        print("Catalog created")
    catalog = fb.activate_and_get_catalog(catalog_name)

    print("Registering the source tables")
    [
        grocery_customer_table,
        grocery_invoice_table,
        grocery_items_table,
        grocery_product_table,
    ] = register_grocery_tables()

    print("Registering the entities")
    register_grocery_entities()

    print("Tagging the entities to columns in the data tables")
    tag_grocery_entities_to_columns(
        grocery_customer_table, grocery_invoice_table, grocery_items_table, grocery_product_table
    )

    print("Populating the feature store with example features")

    # create an event view for the grocery invoice table
    grocery_invoice_view = grocery_invoice_table.get_view()

    # create an item data view for the grocery items table
    grocery_items_view = grocery_items_table.get_view()

    # count the number of invoices per customer over the past 60 days
    invoice_count_60days = grocery_invoice_view.groupby("GroceryCustomerGuid").aggregate_over(
        None, method=fb.AggFunc.COUNT, feature_names=["InvoiceCount_60days"], windows=["60d"]
    )

    # change from a FeatureGroup to a Feature
    invoice_count_60days = invoice_count_60days["InvoiceCount_60days"]

    # save the feature
    invoice_count_60days.save(conflict_resolution="retrieve")

    # get the cross-aggregation of the items purchased over the past 30 days, grouped by customer
    inventory = grocery_items_view.groupby(
        "GroceryCustomerGuid", category="GroceryProductGuid"
    ).aggregate_over(None, method=fb.AggFunc.COUNT, feature_names=["Inventory"], windows=["30d"])
    inventory = inventory["Inventory"]

    # get the entropy of the inventory
    inventoryEntropy = inventory.cd.entropy()
    inventoryEntropy.name = "PurchasedItemsEntropy_30d"

    # save the feature
    inventoryEntropy.save(conflict_resolution="retrieve")

    print("Catalog created and pre-populated with data and features")

    return catalog


def create_deep_dive_materializing_features_catalog():
    catalog_name = "deep dive materializing features " + datetime.now().strftime("%Y%m%d:%H%M")

    print("Building a deep dive catalog for materializing features named [" + catalog_name + "]")

    # get a list of catalogs
    catalog_list = fb.Catalog.list()

    # check whether catalog_name exists in the name column of catalog_list
    if catalog_name in catalog_list["name"].values:
        print("Catalog already exists")
    else:
        print("Creating new catalog")
        # creating a catalog activates it
        catalog = fb.Catalog.create(catalog_name, "playground")
        print("Catalog created")
    catalog = fb.activate_and_get_catalog(catalog_name)

    print("Registering the source tables")
    [
        grocery_customer_table,
        grocery_invoice_table,
        grocery_items_table,
        grocery_product_table,
    ] = register_grocery_tables()

    print("Registering the entities")
    register_grocery_entities()

    print("Tagging the entities to columns in the data tables")
    tag_grocery_entities_to_columns(
        grocery_customer_table, grocery_invoice_table, grocery_items_table, grocery_product_table
    )

    print("Populating the feature store with example features")

    # create the views
    grocery_customer_view = grocery_customer_table.get_view()
    grocery_invoice_view = grocery_invoice_table.get_view()
    grocery_items_view = grocery_items_table.get_view()
    grocery_product_view = grocery_product_table.get_view()

    # join the product view to the items view
    grocery_items_view = grocery_items_view.join(grocery_product_view, on="GroceryProductGuid")

    # get the cross-aggregation of the items purchased over the past 30 days, grouped by customer, subgrouped (i.e. categorized) by product
    customer_inventory_4w = grocery_items_view.groupby(
        "GroceryCustomerGuid", category="ProductGroup"
    ).aggregate_over(
        None, method=fb.AggFunc.COUNT, feature_names=["CustomerInventory_4w"], windows=["4w"]
    )

    # get the entropy of the inventory
    customer_inventory_entropy_4w = customer_inventory_4w["CustomerInventory_4w"].cd.entropy()
    customer_inventory_entropy_4w.name = "CustomerInventoryEntropy_4w"

    # get the most frequent item purchased
    customer_inventory_most_frequent_4w = customer_inventory_4w[
        "CustomerInventory_4w"
    ].cd.most_frequent()
    customer_inventory_most_frequent_4w.name = "CustomerInventoryMostFrequent_4w"

    # get the average latitude of the customers in each French state, weighted by customer location
    state_mean_latitude = grocery_customer_view.groupby("State").aggregate_asat(
        value_column="Latitude", method=fb.AggFunc.AVG, feature_name="StateMeanLatitude"
    )
    # get the average latitude of the customers in each French state, weighted by customer location
    state_mean_longitude = grocery_customer_view.groupby("State").aggregate_asat(
        value_column="Longitude", method=fb.AggFunc.AVG, feature_name="StateMeanLongitude"
    )

    # combine the two features into a feature group
    state_centroids = fb.FeatureGroup([state_mean_latitude, state_mean_longitude])

    # create a feature list
    feature_list = fb.FeatureList(
        [customer_inventory_entropy_4w, customer_inventory_most_frequent_4w, state_centroids],
        name="CustomerFeatures",
    )

    # create a feature that is the total sales for each customer over the past 2 weeks
    customer_sales_14d = grocery_invoice_view.groupby("GroceryCustomerGuid").aggregate_over(
        value_column="Amount",
        method=fb.AggFunc.SUM,
        feature_names=["Target"],
        windows=["14d"],
        fill_value=0,
    )

    # create a feature list for the target
    target_list = fb.FeatureList([customer_sales_14d], name="TargetFeature")

    # save the feature list to the catalog
    feature_list.save(conflict_resolution="retrieve")

    # save the target list to the catalog
    target_list.save(conflict_resolution="retrieve")

    print("Catalog created and pre-populated with data and features")

    return catalog


def create_quick_start_reusing_features_catalog():
    catalog_name = "quick start reusing features " + datetime.now().strftime("%Y%m%d:%H%M")

    print("Building a quick start catalog for reusing features named [" + catalog_name + "]")

    # get a list of catalogs
    catalog_list = fb.Catalog.list()

    # check whether catalog_name exists in the name column of catalog_list
    if catalog_name in catalog_list["name"].values:
        print("Catalog already exists")
    else:
        print("Creating new catalog")
        # creating a catalog activates it
        catalog = fb.Catalog.create(catalog_name, "playground")
        print("Catalog created")
    catalog = fb.activate_and_get_catalog(catalog_name)

    print("Registering the source tables")
    [
        grocery_customer_table,
        grocery_invoice_table,
        grocery_items_table,
        grocery_product_table,
    ] = register_grocery_tables()

    print("Registering the entities")
    register_grocery_entities()

    print("Tagging the entities to columns in the data tables")
    tag_grocery_entities_to_columns(
        grocery_customer_table, grocery_invoice_table, grocery_items_table, grocery_product_table
    )

    print("Populating the feature store with example features")

    # create the views
    grocery_customer_view = grocery_customer_table.get_view()
    grocery_invoice_view = grocery_invoice_table.get_view()
    grocery_items_view = grocery_items_table.get_view()
    grocery_product_view = grocery_product_table.get_view()

    ### join the tables in preparation for feature engineering

    # join the product group to the items table
    grocery_items_view = grocery_items_view.join(grocery_product_view)

    # Join selected columns from the grocery customer view with the grocery invoice view
    grocery_invoice_view = grocery_invoice_view.join(
        grocery_customer_view[["Gender", "State"]], rsuffix="_Customer"
    )

    # join the items table to the customer table
    grocery_items_view = grocery_items_view.join(grocery_customer_view)

    ### create features for the French state entity

    # create a lookup feature for the state
    state = grocery_customer_view.State.as_feature(feature_name="StateName")
    state.save()

    # create a feature that counts the population of each French state
    state_population = grocery_customer_view.groupby("State").aggregate_asat(
        None, method=fb.AggFunc.COUNT, feature_name="StatePopulation"
    )
    state_population.save()

    # create a feature that is the cross-aggregation of product groups over the past 28 days grouped by state
    state_inventory_28d = grocery_items_view.groupby(
        "State", category="ProductGroup"
    ).aggregate_over(
        None, method=fb.AggFunc.COUNT, feature_names=["StateInventory_28d"], windows=["28d"]
    )
    state_inventory_28d.save()

    # create a feature that is the average invoice amount for each French state
    state_avg_invoice_amount_28d = grocery_invoice_view.groupby("State_Customer").aggregate_over(
        value_column="Amount",
        method=fb.AggFunc.AVG,
        feature_names=["StateAvgInvoiceAmount_28d"],
        windows=["28d"],
    )
    state_avg_invoice_amount_28d.save()

    # create a feature that is the average latitude of the customers in each French state, weighted by customer location
    state_mean_latitude = grocery_customer_view.groupby("State").aggregate_asat(
        value_column="Latitude", method=fb.AggFunc.AVG, feature_name="StateMeanLatitude"
    )
    state_mean_latitude.save()

    # create a feature that is the average longitude of the customers in each French state, weighted by customer location
    state_mean_longitude = grocery_customer_view.groupby("State").aggregate_asat(
        value_column="Longitude", method=fb.AggFunc.AVG, feature_name="StateMeanLongitude"
    )
    state_mean_longitude.save()

    ### create features for the customer entity

    # create a feature that is the cross-aggregation of the items purchased over the past 28 days, grouped by customer, subgrouped (i.e. categorized) by product group
    customer_inventory_28d = grocery_items_view.groupby(
        "GroceryCustomerGuid", category="ProductGroup"
    ).aggregate_over(
        None, method=fb.AggFunc.COUNT, feature_names=["CustomerInventory_28d"], windows=["28d"]
    )
    customer_inventory_28d.save()

    # create a feature that is the inventory of the items purchased over the past 28 days, grouped by customer, subgrouped (i.e. categorized) by product group
    customer_inventory_24w = grocery_items_view.groupby(
        "GroceryCustomerGuid", category="ProductGroup"
    ).aggregate_over(
        None, method=fb.AggFunc.COUNT, feature_names=["CustomerInventory_24w"], windows=["24w"]
    )
    customer_inventory_24w.save()

    # create a feature that is the total sales for each customer over the past 2 weeks
    customer_spend_14d = grocery_invoice_view.groupby("GroceryCustomerGuid").aggregate_over(
        value_column="Amount",
        method=fb.AggFunc.SUM,
        feature_names=["CustomerSpend_14d"],
        windows=["14d"],
        fill_value=0,
    )
    customer_spend_14d.save()

    # create a feature that is the year of birth of each customer
    # add a year of birth column feature
    grocery_customer_view["YearOfBirth"] = grocery_customer_view.DateOfBirth.dt.year
    # create a feature from the year of birth column
    year_of_birth = grocery_customer_view.YearOfBirth.as_feature("CustomerYearOfBirth")
    year_of_birth.save()

    ### create features for the invoice entity

    # create a feature that is the number of items in each invoice
    invoice_item_count = grocery_items_view.groupby("GroceryInvoiceGuid").aggregate(
        value_column="Quantity", method=fb.AggFunc.SUM, feature_name="InvoiceItemCount"
    )
    invoice_item_count.save()

    # create a feature that is the total discount amount for each invoice
    invoice_discount_amount = grocery_items_view.groupby("GroceryInvoiceGuid").aggregate(
        value_column="Discount", method=fb.AggFunc.SUM, feature_name="InvoiceDiscountAmount"
    )
    invoice_discount_amount.save()

    # create a feature that is the number of unique product groups in each invoice
    invoice_unique_product_groups = grocery_items_view.groupby(
        "GroceryInvoiceGuid", category="ProductGroup"
    ).aggregate(None, method=fb.AggFunc.COUNT, feature_name="InvoiceUniqueProductGroups")
    invoice_unique_product_count = invoice_unique_product_groups.cd.unique_count()
    invoice_unique_product_count.name = "InvoiceUniqueProductGroupCount"
    invoice_unique_product_count.save()

    ### create a feature list
    state_feature_list = fb.FeatureList(
        [
            state_inventory_28d,
            state_avg_invoice_amount_28d,
            state_population,
            state_mean_latitude,
            state_mean_longitude,
        ],
        name="StateFeatureList",
    )
    state_feature_list.save()

    return catalog


def create_playground_credit_card_catalog():
    catalog_name = "credit card playground " + datetime.now().strftime("%Y%m%d:%H%M")

    print("Building a playground catalog for credit cards named [" + catalog_name + "]")

    # get a list of catalogs
    catalog_list = fb.Catalog.list()

    # check whether catalog_name exists in the name column of catalog_list
    if catalog_name in catalog_list["name"].values:
        print("Catalog already exists")
    else:
        print("Creating new catalog")
        # creating a catalog activates it
        catalog = fb.Catalog.create(catalog_name, "playground")
        print("Catalog created")
    catalog = fb.activate_and_get_catalog(catalog_name)

    print("Registering the source tables")
    [
        bank_customer_table,
        state_details_table,
        credit_card_table,
        card_transaction_table,
        card_fraud_status_table,
        card_transaction_group_table,
    ] = register_credit_card_tables()

    print("Registering the entities")
    register_credit_card_entities()

    print("Tagging the entities to columns in the data tables")
    tag_credit_card_entities_to_columns(
        bank_customer_table,
        state_details_table,
        credit_card_table,
        card_transaction_table,
        card_fraud_status_table,
        card_transaction_group_table,
    )

    generate_catalog_boilerplate_code(catalog)

    return catalog


def create_playground_healthcare_catalog():
    catalog_name = "healthcare playground " + datetime.now().strftime("%Y%m%d:%H%M")

    print("Building a playground catalog for healthcare named [" + catalog_name + "]")

    # get a list of catalogs
    catalog_list = fb.Catalog.list()

    # check whether catalog_name exists in the name column of catalog_list
    if catalog_name in catalog_list["name"].values:
        print("Catalog already exists")
    else:
        print("Creating new catalog")
        # creating a catalog activates it
        catalog = fb.Catalog.create(catalog_name, "playground")
        print("Catalog created")
    catalog = fb.activate_and_get_catalog(catalog_name)

    print("Registering the source tables")
    [
        Patient,
        Diagnosis,
        PatientSmokingStatus,
        Allergy,
        StateDetails,
        Visit,
        Prescription,
        LabResult,
        LabObservation,
        ICD9Hierarchy,
        SpecialtyGroup,
        MedicalProduct,
    ] = register_healthcare_tables()

    print("Registering the entities")
    register_healthcare_entities()

    print("Tagging the entities to columns in the data tables")
    tag_healthcare_entities_to_columns(
        Patient,
        Diagnosis,
        PatientSmokingStatus,
        Allergy,
        StateDetails,
        Visit,
        Prescription,
        LabResult,
        LabObservation,
        ICD9Hierarchy,
        SpecialtyGroup,
        MedicalProduct,
    )

    generate_catalog_boilerplate_code(catalog)

    return catalog


def create_quick_start_feature_management_catalog():
    catalog_name = "quick start feature management " + datetime.now().strftime("%Y%m%d:%H%M")

    print("Building a quick start catalog for feature management named [" + catalog_name + "]")

    # get a list of catalogs
    catalog_list = fb.Catalog.list()

    # check whether catalog_name exists in the name column of catalog_list
    if catalog_name in catalog_list["name"].values:
        print("Catalog already exists")
    else:
        print("Creating new catalog")
        # creating a catalog activates it
        catalog = fb.Catalog.create(catalog_name, "playground")
        print("Catalog created")
    catalog = fb.activate_and_get_catalog(catalog_name)

    print("Registering the source tables")
    [
        grocery_customer_table,
        grocery_invoice_table,
        grocery_items_table,
        grocery_product_table,
    ] = register_grocery_tables()

    print("Registering the entities")
    register_grocery_entities()

    print("Tagging the entities to columns in the data tables")
    tag_grocery_entities_to_columns(
        grocery_customer_table, grocery_invoice_table, grocery_items_table, grocery_product_table
    )

    print("Populating the feature store with example features")

    # create the views
    grocery_customer_view = grocery_customer_table.get_view()
    grocery_invoice_view = grocery_invoice_table.get_view()
    grocery_items_view = grocery_items_table.get_view()
    grocery_product_view = grocery_product_table.get_view()

    ### join the tables in preparation for feature engineering

    # join the product group to the items table
    grocery_items_view = grocery_items_view.join(grocery_product_view)

    # Join selected columns from the grocery customer view with the grocery invoice view
    grocery_invoice_view = grocery_invoice_view.join(
        grocery_customer_view[["Gender", "State"]], rsuffix="_Customer"
    )

    # join the items table to the customer table
    grocery_items_view = grocery_items_view.join(grocery_customer_view[["Gender", "State"]])

    ### create features for the French state entity

    # create a lookup feature for the state
    state = grocery_customer_view.State.as_feature(feature_name="StateName")
    state.save()

    # create a lookup feature for the customer date of birth - this will be deleted during the tutorial
    dob = grocery_customer_view.DateOfBirth.as_feature(feature_name="unused experimental feature")
    dob.save()

    # create a very short feature list
    very_short_feature_list = fb.FeatureList([state], name="very short feature list")
    very_short_feature_list.save()

    # create a feature that counts the population of each French state
    state_population = grocery_customer_view.groupby("State").aggregate_asat(
        None, method=fb.AggFunc.COUNT, feature_name="StatePopulation"
    )
    state_population.save()

    # create a feature that is the cross-aggregation of product groups over the past 28 days grouped by state
    state_inventory_28d = grocery_items_view.groupby(
        "State", category="ProductGroup"
    ).aggregate_over(
        None, method=fb.AggFunc.COUNT, feature_names=["StateInventory_28d"], windows=["28d"]
    )
    state_inventory_28d.save()

    # create a feature that is the average invoice amount for each French state
    state_avg_invoice_amount_28d = grocery_invoice_view.groupby("State_Customer").aggregate_over(
        value_column="Amount",
        method=fb.AggFunc.AVG,
        feature_names=["StateAvgInvoiceAmount_28d"],
        windows=["28d"],
    )
    state_avg_invoice_amount_28d.save()

    # create a feature that is the average latitude of the customers in each French state, weighted by customer location
    state_mean_latitude = grocery_customer_view.groupby("State").aggregate_asat(
        value_column="Latitude", method=fb.AggFunc.AVG, feature_name="StateMeanLatitude"
    )
    state_mean_latitude.save()

    # create a feature that is the average longitude of the customers in each French state, weighted by customer location
    state_mean_longitude = grocery_customer_view.groupby("State").aggregate_asat(
        value_column="Longitude", method=fb.AggFunc.AVG, feature_name="StateMeanLongitude"
    )
    state_mean_longitude.save()

    ### create features for the customer entity

    # create a feature that is the cross-aggregation of the items purchased over the past 28 days, grouped by customer, subgrouped (i.e. categorized) by product group
    customer_inventory_28d = grocery_items_view.groupby(
        "GroceryCustomerGuid", category="ProductGroup"
    ).aggregate_over(
        None, method=fb.AggFunc.COUNT, feature_names=["CustomerInventory_28d"], windows=["28d"]
    )
    customer_inventory_28d.save()

    # create a feature that is the inventory of the items purchased over the past 28 days, grouped by customer, subgrouped (i.e. categorized) by product group
    customer_inventory_24w = grocery_items_view.groupby(
        "GroceryCustomerGuid", category="ProductGroup"
    ).aggregate_over(
        None, method=fb.AggFunc.COUNT, feature_names=["CustomerInventory_24w"], windows=["24w"]
    )
    customer_inventory_24w.save()

    # create a feature that is the total sales for each customer over the past 2 weeks
    customer_spend_14d = grocery_invoice_view.groupby("GroceryCustomerGuid").aggregate_over(
        value_column="Amount",
        method=fb.AggFunc.SUM,
        feature_names=["CustomerSpend_14d"],
        windows=["14d"],
        fill_value=0,
    )
    customer_spend_14d.save()

    # create a feature that is the year of birth of each customer
    # add a year of birth column feature
    grocery_customer_view["YearOfBirth"] = grocery_customer_view.DateOfBirth.dt.year
    # create a feature from the year of birth column
    year_of_birth = grocery_customer_view.YearOfBirth.as_feature("CustomerYearOfBirth")
    year_of_birth.save()

    ### create features for the invoice entity

    # create a feature that is the number of items in each invoice
    invoice_item_count = grocery_items_view.groupby("GroceryInvoiceGuid").aggregate(
        value_column="Quantity", method=fb.AggFunc.SUM, feature_name="InvoiceItemCount"
    )
    invoice_item_count.save()

    # create a feature that is the total discount amount for each invoice
    invoice_discount_amount = grocery_items_view.groupby("GroceryInvoiceGuid").aggregate(
        value_column="Discount", method=fb.AggFunc.SUM, feature_name="InvoiceDiscountAmount"
    )
    invoice_discount_amount.save()

    # create a feature that is the number of unique product groups in each invoice
    invoice_unique_product_groups = grocery_items_view.groupby(
        "GroceryInvoiceGuid", category="ProductGroup"
    ).aggregate(None, method=fb.AggFunc.COUNT, feature_name="InvoiceUniqueProductGroups")
    invoice_unique_product_count = invoice_unique_product_groups.cd.unique_count()
    invoice_unique_product_count.name = "InvoiceUniqueProductGroupCount"
    invoice_unique_product_count.save()

    ### create a feature list
    state_feature_list = fb.FeatureList(
        [
            state_inventory_28d,
            state_avg_invoice_amount_28d,
            state_population,
            state_mean_latitude,
            state_mean_longitude,
        ],
        name="StateFeatureList",
    )
    state_feature_list.save()

    customer_feature_list = fb.FeatureList(
        [customer_inventory_28d, customer_inventory_24w, customer_spend_14d, year_of_birth],
        name="CustomerFeatureList",
    )
    customer_feature_list.save()

    invoice_feature_list = fb.FeatureList(
        [invoice_item_count, invoice_discount_amount, invoice_unique_product_groups],
        name="InvoiceFeatureList",
    )
    invoice_feature_list.save()

    print("Setting feature readiness")

    # change the customer features to be production ready
    for feature_name in catalog.list_features().name:
        feature = fb.Feature.get(feature_name)

        # does the feature name contain the word "customer"?
        if "Customer" in feature.name:
            feature.update_readiness("PRODUCTION_READY")

    # deprecate the InvoiceUniqueProductGroups feature
    invoice_unique_product_groups_feature = fb.Feature.get("InvoiceUniqueProductGroups")
    invoice_unique_product_groups_feature.update_readiness("PUBLIC_DRAFT")
    invoice_unique_product_groups_feature.update_readiness("DEPRECATED")

    print("Deploying feature list")

    deployment = customer_feature_list.deploy(make_production_ready=True)
    deployment.enable()

    return catalog


def create_quick_start_model_training_catalog():
    catalog_name = "quick start model training " + datetime.now().strftime("%Y%m%d:%H%M")

    print("Building a quick start catalog for model training named [" + catalog_name + "]")

    # get a list of catalogs
    catalog_list = fb.Catalog.list()

    # check whether catalog_name exists in the name column of catalog_list
    if catalog_name in catalog_list["name"].values:
        print("Catalog already exists")
    else:
        print("Creating new catalog")
        # creating a catalog activates it
        catalog = fb.Catalog.create(catalog_name, "playground")
        print("Catalog created")
    catalog = fb.activate_and_get_catalog(catalog_name)

    print("Registering the source tables")
    [
        grocery_customer_table,
        grocery_invoice_table,
        grocery_items_table,
        grocery_product_table,
    ] = register_grocery_tables()

    print("Registering the entities")
    register_grocery_entities()

    print("Tagging the entities to columns in the data tables")
    tag_grocery_entities_to_columns(
        grocery_customer_table, grocery_invoice_table, grocery_items_table, grocery_product_table
    )

    print("Populating the feature store with example features")

    # create the views
    grocery_customer_view = grocery_customer_table.get_view()
    grocery_invoice_view = grocery_invoice_table.get_view()
    grocery_items_view = grocery_items_table.get_view()
    grocery_product_view = grocery_product_table.get_view()

    ### join the tables in preparation for feature engineering

    # join the product group to the items table
    grocery_items_view = grocery_items_view.join(grocery_product_view)

    # Join selected columns from the grocery customer view with the grocery invoice view
    grocery_invoice_view = grocery_invoice_view.join(
        grocery_customer_view[["Gender", "State"]], rsuffix="_Customer"
    )

    # join the items table to the customer table
    grocery_items_view = grocery_items_view.join(grocery_customer_view)

    # create a lookup feature for the state
    state = grocery_customer_view.State.as_feature(feature_name="StateName")
    state.save()

    # create a feature that counts the population of each French state
    state_population = grocery_customer_view.groupby("State").aggregate_asat(
        None, method=fb.AggFunc.COUNT, feature_name="StatePopulation"
    )
    state_population.save()

    # create a feature that is the cross-aggregation of product groups over the past 28 days grouped by state
    state_inventory_28d = grocery_items_view.groupby(
        "State", category="ProductGroup"
    ).aggregate_over(
        None, method=fb.AggFunc.COUNT, feature_names=["StateInventory_28d"], windows=["28d"]
    )
    # state_inventory_28d.save()

    # create a feature that is the average invoice amount for each French state
    state_avg_invoice_amount_28d = grocery_invoice_view.groupby("State_Customer").aggregate_over(
        value_column="Amount",
        method=fb.AggFunc.AVG,
        feature_names=["StateAvgInvoiceAmount_28d"],
        windows=["28d"],
    )
    state_avg_invoice_amount_28d.save()

    # create a feature that is the average latitude of the customers in each French state, weighted by customer location
    state_mean_latitude = grocery_customer_view.groupby("State").aggregate_asat(
        value_column="Latitude", method=fb.AggFunc.AVG, feature_name="StateMeanLatitude"
    )
    state_mean_latitude.save()

    # create a feature that is the average longitude of the customers in each French state, weighted by customer location
    state_mean_longitude = grocery_customer_view.groupby("State").aggregate_asat(
        value_column="Longitude", method=fb.AggFunc.AVG, feature_name="StateMeanLongitude"
    )
    state_mean_longitude.save()

    ### create features for the customer entity

    # create a feature that is the cross-aggregation of the items purchased over the past 28 days, grouped by customer, subgrouped (i.e. categorized) by product group
    customer_inventory_28d = grocery_items_view.groupby(
        "GroceryCustomerGuid", category="ProductGroup"
    ).aggregate_over(
        None, method=fb.AggFunc.COUNT, feature_names=["CustomerInventory_28d"], windows=["28d"]
    )
    # customer_inventory_28d.save()

    # create a feature that is the inventory of the items purchased over the past 28 days, grouped by customer, subgrouped (i.e. categorized) by product group
    customer_inventory_14d = grocery_items_view.groupby(
        "GroceryCustomerGuid", category="ProductGroup"
    ).aggregate_over(
        None, method=fb.AggFunc.COUNT, feature_names=["CustomerInventory_14d"], windows=["14d"]
    )
    # customer_inventory_14d.save()

    customer_inventory_stability_14d28d = customer_inventory_14d[
        "CustomerInventory_14d"
    ].cd.cosine_similarity(customer_inventory_28d["CustomerInventory_28d"])
    customer_inventory_stability_14d28d.fillna(0)
    customer_inventory_stability_14d28d.name = "CustomerInventoryStability_14d28d"
    customer_inventory_stability_14d28d.save()

    customer_state_similarity_28d = customer_inventory_28d[
        "CustomerInventory_28d"
    ].cd.cosine_similarity(state_inventory_28d["StateInventory_28d"])
    customer_state_similarity_28d.fillna(0)
    customer_state_similarity_28d.name = "CustomerStateSimilarity_28d"
    customer_state_similarity_28d.save()

    # create a feature that is the total sales for each customer over the past 2 weeks
    customer_spend_28d = grocery_invoice_view.groupby("GroceryCustomerGuid").aggregate_over(
        value_column="Amount",
        method=fb.AggFunc.SUM,
        feature_names=["CustomerSpend_28d"],
        windows=["28d"],
        fill_value=0,
    )
    customer_spend_28d.save()

    # create a feature that is the average invoice amount over the past 28 days, grouped by customer
    customer_avg_invoice_amount_28d = grocery_invoice_view.groupby(
        "GroceryCustomerGuid"
    ).aggregate_over(
        value_column="Amount",
        method=fb.AggFunc.AVG,
        feature_names=["CustomerAvgInvoiceAmount_28d"],
        windows=["28d"],
    )
    customer_avg_invoice_amount_28d.save()

    # create a feature that is the total sales for each customer over the past 2 weeks
    customer_spend_14d = grocery_invoice_view.groupby("GroceryCustomerGuid").aggregate_over(
        value_column="Amount",
        method=fb.AggFunc.SUM,
        feature_names=["CustomerSpend_14d"],
        windows=["14d"],
        fill_value=0,
    )
    customer_spend_14d.save()

    # create a feature that is the year of birth of each customer
    # add a year of birth column feature
    grocery_customer_view["YearOfBirth"] = grocery_customer_view.DateOfBirth.dt.year
    # create a feature from the year of birth column
    year_of_birth = grocery_customer_view.YearOfBirth.as_feature("CustomerYearOfBirth")
    # year_of_birth.save()

    ### create a feature list

    print("Setting feature readiness")

    # change the features to be production ready
    for feature_name in catalog.list_features().name:
        feature = fb.Feature.get(feature_name)
        feature.update_readiness("PRODUCTION_READY")

    # create a feature list
    feature_list = fb.FeatureList(
        [
            state_population,
            state_avg_invoice_amount_28d,
            state_mean_latitude,
            state_mean_longitude,
            customer_inventory_stability_14d28d,
            customer_state_similarity_28d,
            customer_spend_28d,
            customer_avg_invoice_amount_28d,
        ],
        name="Features",
    )

    # create a feature that is the total sales for each customer over the past 2 weeks
    customer_sales_14d = grocery_invoice_view.groupby("GroceryCustomerGuid").aggregate_over(
        value_column="Amount",
        method=fb.AggFunc.SUM,
        feature_names=["Target"],
        windows=["14d"],
        fill_value=0,
    )
    customer_sales_14d.save()
    customer_sales_14d["Target"].update_readiness("PRODUCTION_READY")

    # create a feature list for the target
    target_list = fb.FeatureList([customer_sales_14d], name="TargetFeature")
    target_list.save(conflict_resolution="retrieve")

    # save the feature list to the catalog
    feature_list.save(conflict_resolution="retrieve")

    # Create a Target and save it.
    next_customer_sales_14d = grocery_invoice_view.groupby("GroceryCustomerGuid").forward_aggregate(
        value_column="Amount",
        method=fb.AggFunc.SUM,
        window="14d",
        target_name="next_customer_sales_14d",
    )
    next_customer_sales_14d.save()

    print("Catalog created and pre-populated with data and features")

    return catalog
