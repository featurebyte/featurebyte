"""
Test feature list service class
"""
import pytest
from bson.objectid import ObjectId

from featurebyte import FeatureJobSetting, TableFeatureJobSetting
from featurebyte.exception import DocumentError, DocumentInconsistencyError
from featurebyte.models.entity import ParentEntity
from featurebyte.schema.entity import EntityCreate
from featurebyte.schema.feature import FeatureNewVersionCreate, FeatureServiceCreate
from featurebyte.schema.feature_list import (
    FeatureListNewVersionCreate,
    FeatureListServiceCreate,
    FeatureVersionInfo,
)


@pytest.mark.asyncio
async def test_update_document__duplicated_feature_error(feature_list_service, feature_list):
    """Test feature creation - document inconsistency error"""
    data_dict = feature_list.dict(by_alias=True)
    data_dict["_id"] = ObjectId()
    data_dict["feature_ids"] = data_dict["feature_ids"] * 2
    with pytest.raises(DocumentError) as exc:
        await feature_list_service.create_document(
            data=FeatureListServiceCreate(**data_dict),
        )
    expected_msg = "Two Feature objects must not share the same name in a FeatureList object."
    assert expected_msg in str(exc.value)


@pytest.mark.asyncio
async def test_update_document__inconsistency_error(
    feature_list_service, feature_service, feature_list, feature
):
    """Test feature creation - document inconsistency error"""
    feat_data_dict = feature.dict(by_alias=True)
    feat_data_dict["_id"] = ObjectId()
    feat_data_dict["name"] = "random_name"
    feat_data_dict["feature_namespace_id"] = ObjectId()
    new_feat = await feature_service.create_document(data=FeatureServiceCreate(**feat_data_dict))

    flist_data_dict = feature_list.dict(by_alias=True)
    flist_data_dict["_id"] = ObjectId()
    flist_data_dict["name"] = "random_name"
    flist_data_dict["feature_ids"] = [new_feat.id]
    with pytest.raises(DocumentInconsistencyError) as exc:
        await feature_list_service.create_document(
            data=FeatureListServiceCreate(**flist_data_dict),
        )
    expected_msg = (
        'FeatureList (name: "random_name") object(s) within the same namespace must have the same '
        '"name" value (namespace: "sf_feature_list", feature_list: "random_name").'
    )
    assert expected_msg in str(exc.value)

    flist_data_dict = feature_list.dict(by_alias=True)
    flist_data_dict["_id"] = ObjectId()
    flist_data_dict["version"] = {"name": "V220917"}
    flist_data_dict["feature_ids"] += [new_feat.id]
    with pytest.raises(DocumentInconsistencyError) as exc:
        await feature_list_service.create_document(
            data=FeatureListServiceCreate(**flist_data_dict),
        )
    expected_msg = (
        'FeatureList (name: "sf_feature_list") object(s) within the same namespace must share the '
        "same feature name(s)."
    )
    assert expected_msg in str(exc.value)


async def create_entity_family(
    entity_service,
    table_columns_info_service,
    entity_relationship_service,
    target_entity_id,
    dimension_table,
    item_table,
):
    """Create a family of entities"""
    # create a parent & child entity & associate them with the target entity
    parent_entity = await entity_service.create_document(
        data=EntityCreate(name="parent_entity", serving_name="parent_name")
    )
    grand_parent_entity = await entity_service.create_document(
        data=EntityCreate(name="grand_parent_entity", serving_name="grand_parent_name")
    )
    child_entity = await entity_service.create_document(
        data=EntityCreate(name="child_entity", serving_name="child_name")
    )
    grand_child_entity = await entity_service.create_document(
        data=EntityCreate(name="grand_child_entity", serving_name="grand_child_name")
    )
    sibling_entity = await entity_service.create_document(
        data=EntityCreate(name="sibling_entity", serving_name="sibling_name")
    )
    unrelated_entity = await entity_service.create_document(
        data=EntityCreate(name="unrelated_entity", serving_name="unrelated_name")
    )
    unrelated_parent_entity = await entity_service.create_document(
        data=EntityCreate(name="unrelated_parent_entity", serving_name="unrelated_parent_name")
    )
    parent_child_table_triples = [
        (target_entity_id, parent_entity.id, dimension_table),
        (parent_entity.id, grand_parent_entity.id, dimension_table),
        (child_entity.id, target_entity_id, item_table),
        (grand_child_entity.id, child_entity.id, item_table),
        (sibling_entity.id, parent_entity.id, dimension_table),
        (unrelated_entity.id, unrelated_parent_entity.id, dimension_table),
    ]
    for child_id, parent_id, table in parent_child_table_triples:
        await table_columns_info_service._add_new_child_parent_relationships(
            entity_id=child_id,
            table_id=table.id,
            parent_entity_ids_to_add=[parent_id],
        )
        await entity_relationship_service.add_relationship(
            parent=ParentEntity(id=parent_id, table_id=table.id, table_type=table.type),
            child_id=child_id,
        )
        entity = await entity_service.get_document(document_id=child_id)
        assert parent_id in entity.ancestor_ids

    ancestor_ids = [grand_parent_entity.id, parent_entity.id]
    descendant_ids = [child_entity.id, grand_child_entity.id]
    return ancestor_ids, descendant_ids, [sibling_entity.id]


@pytest.mark.asyncio
async def test_feature_list__contains_relationships_info(
    feature_list_service,
    entity_service,
    dimension_table,
    item_table,
    feature,
    app_container,
):
    """Test feature list contains relationships info"""
    target_entity_id = feature.entity_ids[0]

    # create a parent & child entity & associate them with the target entity
    ancestor_ids, descendant_ids, sibling_ids = await create_entity_family(
        entity_service,
        app_container.table_columns_info_service,
        app_container.entity_relationship_service,
        target_entity_id,
        dimension_table,
        item_table,
    )

    # check these relationships are included in the feature list
    feature_list = await feature_list_service.create_document(
        data=FeatureListServiceCreate(name="my_feature_list", feature_ids=[feature.id])
    )
    relationships_info = feature_list.relationships_info
    assert len(relationships_info) == 5
    expected_entities = set(ancestor_ids + descendant_ids + sibling_ids + [target_entity_id])
    for relationship_info in relationships_info:
        assert relationship_info.entity_id in expected_entities
        assert relationship_info.related_entity_id in expected_entities
    expected_relationships_info = relationships_info

    # remove relationship and check relationship info is updated when create a new feature list version
    grand_parent_entity_id, parent_entity_id = ancestor_ids
    await app_container.table_columns_info_service._remove_parent_entity_ids(
        primary_entity_id=parent_entity_id,
        parent_entity_ids_to_remove=[grand_parent_entity_id],
    )
    await app_container.entity_relationship_service.remove_relationship(
        parent_id=grand_parent_entity_id, child_id=parent_entity_id
    )
    new_feature = await app_container.version_service.create_new_feature_version(
        data=FeatureNewVersionCreate(
            source_feature_id=feature.id,
            table_feature_job_settings=[
                TableFeatureJobSetting(
                    table_name="sf_event_table",
                    feature_job_setting=FeatureJobSetting(
                        blind_spot="1d", frequency="1d", time_modulo_frequency="1h"
                    ),
                )
            ],
        )
    )
    new_feature_list = await app_container.version_service.create_new_feature_list_version(
        data=FeatureListNewVersionCreate(
            source_feature_list_id=feature_list.id,
            features=[FeatureVersionInfo(name=new_feature.name, version=new_feature.version)],
        )
    )

    # check the relationship info in the source feature list first
    feature_list = await feature_list_service.get_document(document_id=feature_list.id)
    assert feature_list.relationships_info == expected_relationships_info

    # check the newly created feature list
    relationships_info = new_feature_list.relationships_info
    expected_entities = set(
        entity_id for entity_id in expected_entities if entity_id != grand_parent_entity_id
    )
    assert len(relationships_info) == 4
    for relationship_info in relationships_info:
        assert relationship_info.entity_id in expected_entities
        assert relationship_info.related_entity_id in expected_entities
