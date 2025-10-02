"""
Unit test for Table class
"""

from __future__ import annotations

import pytest

from featurebyte import FeatureList
from featurebyte.api.dimension_table import DimensionTable
from featurebyte.api.entity import Entity
from featurebyte.api.event_table import EventTable
from featurebyte.api.item_table import ItemTable
from featurebyte.api.scd_table import SCDTable
from featurebyte.api.table import Table
from featurebyte.exception import RecordDeletionException, RecordRetrievalException


def test_get_event_table(saved_event_table, snowflake_event_table):
    """
    Test Table.get function to retrieve EventTable
    """
    # load the event table from the persistent
    loaded_event_table = Table.get(snowflake_event_table.name)
    assert loaded_event_table.saved is True
    assert loaded_event_table == snowflake_event_table
    assert EventTable.get_by_id(id=snowflake_event_table.id) == snowflake_event_table

    # load the event table use get_by_id
    loaded_table = Table.get_by_id(snowflake_event_table.id)
    assert loaded_table == loaded_event_table

    with pytest.raises(RecordRetrievalException) as exc:
        Table.get("unknown_event_table")
    expected_msg = (
        'Table (name: "unknown_event_table") not found. Please save the Table object first.'
    )
    assert expected_msg in str(exc.value)


def test_get_item_table(snowflake_item_table, saved_item_table):
    """
    Test Table.get function to retrieve ItemTable
    """
    # load the item table from the persistent
    loaded_table = Table.get(saved_item_table.name)
    assert loaded_table.saved is True
    assert loaded_table == snowflake_item_table
    assert ItemTable.get_by_id(id=loaded_table.id) == snowflake_item_table

    with pytest.raises(RecordRetrievalException) as exc:
        Table.get("unknown_item_table")
    expected_msg = (
        'Table (name: "unknown_item_table") not found. Please save the Table object first.'
    )
    assert expected_msg in str(exc.value)


def test_get_scd_table(saved_scd_table, snowflake_scd_table):
    """
    Test Table.get function to retrieve SCDTable
    """
    # load the scd table from the persistent
    loaded_scd_table = Table.get(snowflake_scd_table.name)
    assert loaded_scd_table.saved is True
    assert loaded_scd_table == snowflake_scd_table
    assert SCDTable.get_by_id(id=snowflake_scd_table.id) == snowflake_scd_table

    with pytest.raises(RecordRetrievalException) as exc:
        Table.get("unknown_scd_table")
    expected_msg = (
        'Table (name: "unknown_scd_table") not found. Please save the Table object first.'
    )
    assert expected_msg in str(exc.value)


def test_get_dimension_table(saved_dimension_table, snowflake_dimension_table):
    """
    Test Table.get function to retrieve DimensionTable
    """
    # load the dimension table from the persistent
    loaded_scd_table = Table.get(snowflake_dimension_table.name)
    assert loaded_scd_table.saved is True
    assert loaded_scd_table == snowflake_dimension_table
    assert DimensionTable.get_by_id(id=snowflake_dimension_table.id) == snowflake_dimension_table

    with pytest.raises(RecordRetrievalException) as exc:
        Table.get("unknown_dimension_table")
    expected_msg = (
        'Table (name: "unknown_dimension_table") not found. Please save the Table object first.'
    )
    assert expected_msg in str(exc.value)


def test_delete_table_and_entity_referenced_in_feature_entity_relationship(
    saved_event_table,
    saved_scd_table,
    saved_dimension_table,
    feature_group_feature_job_setting,
    patch_initialize_entity_dtype,
):
    """
    Test delete table (and entity) referenced in feature's relationships_info but not in feature's table_ids
    (feature's entity_ids)
    """
    # transaction -> order -> customer
    transaction = Entity.create(name="transaction", serving_names=["transaction"])
    order = Entity.create(name="order", serving_names=["order"])
    customer = Entity.create(name="customer", serving_names=["customer"])

    # event table's event id column: col_int
    # dimension table's dimension id column: col_int
    # scd table's natural key column: col_text
    saved_event_table.col_int.as_entity(transaction.name)
    saved_event_table.col_text.as_entity(order.name)
    saved_dimension_table.col_int.as_entity(order.name)
    saved_dimension_table.col_text.as_entity(customer.name)
    saved_scd_table.col_text.as_entity(customer.name)
    event_view = saved_event_table.get_view()
    scd_view = saved_scd_table.get_view()

    # create feat comp1 with transaction entity
    feat_comp1 = event_view.groupby("col_int").aggregate_over(
        None,
        "count",
        windows=["1d"],
        feature_names=["1d_count"],
        feature_job_setting=feature_group_feature_job_setting,
    )["1d_count"]
    assert feat_comp1.entity_ids == [transaction.id]

    # create feat comp2 with customer entity
    feat_comp2 = scd_view["col_int"].as_feature("col_int")
    assert feat_comp2.entity_ids == [customer.id]

    # create a feat with feat comp1 and feat comp2
    feat = feat_comp1 + feat_comp2
    feat.name = "feat"
    feat.save()

    # note that
    # * order entity is in relationships but not in entity_ids
    # * dimension table is in relationships but not in table_ids
    assert set(feat.entity_ids) == {transaction.id, customer.id}
    relation_entity_triples = set(
        (relation.entity_id, relation.related_entity_id, relation.relation_table_id)
        for relation in feat.cached_model.relationships_info
    )
    assert relation_entity_triples == {
        (transaction.id, order.id, saved_event_table.id),
        (order.id, customer.id, saved_dimension_table.id),
    }

    # expect to fail because feat still references order entity in its relationships
    with pytest.raises(RecordDeletionException) as exc:
        order.delete()
    assert "Entity is referenced by Feature: feat" in str(exc.value)

    # check feature table
    assert set(feat.table_ids) == {saved_event_table.id, saved_scd_table.id}

    # check delete dimension table
    with pytest.raises(RecordDeletionException) as exc:
        saved_dimension_table.delete()
    assert "DimensionTable is referenced by Entity: order" in str(exc.value)

    # untag order from dimension table and attempt to delete dimension table again
    saved_dimension_table.col_int.as_entity(None)

    # expect to fail because feat still references dimension table in its relationships
    with pytest.raises(RecordDeletionException) as exc:
        saved_dimension_table.delete()
    assert "DimensionTable is referenced by Feature: feat" in str(exc.value)


def test_delete_table_and_entity_referenced_in_feature_list_entity_relationship(
    saved_event_table,
    saved_dimension_table,
    float_feature,
    transaction_entity,
    cust_id_entity,
    patch_initialize_entity_dtype,
):
    """Test delete table (and entity) referenced in feature list's relationships_info"""
    # construct following entity relationship
    # sub_transaction -> transaction -> cust_id
    sub_transaction = Entity.create(name="sub_transaction", serving_names=["sub_transaction"])
    saved_dimension_table.col_int.as_entity(sub_transaction.name)
    saved_dimension_table.col_text.as_entity(transaction_entity.name)

    # save the feature list so that the entity relationship is captured
    feature_list = FeatureList([float_feature], name="test_feature_list")
    feature_list.save()

    # note that
    # * sub_transaction entity is captured only in relationships
    # * dimension table is captured only in relationships
    relation_entity_triples = set(
        (relation.entity_id, relation.related_entity_id, relation.relation_table_id)
        for relation in feature_list.cached_model.relationships_info
    )
    assert relation_entity_triples == {
        (sub_transaction.id, transaction_entity.id, saved_dimension_table.id),
        (transaction_entity.id, cust_id_entity.id, saved_event_table.id),
    }

    # attempt to delete sub_transaction entity
    with pytest.raises(RecordDeletionException) as exc:
        sub_transaction.delete()
    assert "Entity is referenced by FeatureList: test_feature_list" in str(exc.value)

    # attempt to delete dimension table
    with pytest.raises(RecordDeletionException) as exc:
        saved_dimension_table.delete()
    assert "DimensionTable is referenced by Entity: sub_transaction" in str(exc.value)

    # untag sub_transaction from dimension table and attempt to delete dimension table again
    saved_dimension_table.col_int.as_entity(None)

    # expect to fail because feature list still references dimension table in its relationships
    with pytest.raises(RecordDeletionException) as exc:
        saved_dimension_table.delete()
    assert "DimensionTable is referenced by FeatureList: test_feature_list" in str(exc.value)
