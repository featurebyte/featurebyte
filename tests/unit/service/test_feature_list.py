"""
Test feature list service class
"""
import pytest
from bson.objectid import ObjectId

from featurebyte import FeatureJobSetting, TableFeatureJobSetting
from featurebyte.exception import DocumentError, DocumentInconsistencyError
from featurebyte.schema.entity import EntityCreate
from featurebyte.schema.feature import FeatureCreate, FeatureNewVersionCreate
from featurebyte.schema.feature_list import (
    FeatureListCreate,
    FeatureListNewVersionCreate,
    FeatureVersionInfo,
)


@pytest.mark.asyncio
async def test_update_document__duplicated_feature_error(feature_list_service, feature_list):
    """Test feature creation - document inconsistency error"""
    data_dict = feature_list.dict(by_alias=True)
    data_dict["_id"] = ObjectId()
    data_dict["version"] = {"name": "V220917"}
    data_dict["feature_ids"] = data_dict["feature_ids"] * 2
    with pytest.raises(DocumentError) as exc:
        await feature_list_service.create_document(
            data=FeatureListCreate(**data_dict),
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
    new_feat = await feature_service.create_document(data=FeatureCreate(**feat_data_dict))

    flist_data_dict = feature_list.dict(by_alias=True)
    flist_data_dict["_id"] = ObjectId()
    flist_data_dict["name"] = "random_name"
    flist_data_dict["feature_ids"] = [new_feat.id]
    with pytest.raises(DocumentInconsistencyError) as exc:
        await feature_list_service.create_document(
            data=FeatureListCreate(**flist_data_dict),
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
            data=FeatureListCreate(**flist_data_dict),
        )
    expected_msg = (
        'FeatureList (name: "sf_feature_list") object(s) within the same namespace must share the '
        "same feature name(s)."
    )
    assert expected_msg in str(exc.value)


@pytest.mark.asyncio
async def test_feature_list__contains_relationships_info(
    feature_list_service,
    entity_service,
    dimension_table,
    item_table,
    table_columns_info_service,
    version_service,
    feature,
):
    """Test feature list contains relationships info"""
    target_entity_id = feature.entity_ids[0]

    # create a parent & child entity & associate them with the target entity
    parent_entity = await entity_service.create_document(
        data=EntityCreate(name="parent_entity", serving_name="parent_name")
    )
    child_entity = await entity_service.create_document(
        data=EntityCreate(name="child_entity", serving_name="child_name")
    )
    await table_columns_info_service._add_new_child_parent_relationships(
        entity_id=target_entity_id,
        table_id=dimension_table.id,
        parent_entity_ids_to_add=[parent_entity.id],
    )
    await table_columns_info_service._add_new_child_parent_relationships(
        entity_id=child_entity.id,
        table_id=item_table.id,
        parent_entity_ids_to_add=[target_entity_id],
    )

    # check these relationships are included in the feature list
    feature_list = await feature_list_service.create_document(
        data=FeatureListCreate(name="my_feature_list", feature_ids=[feature.id])
    )
    relationships_info = feature_list.relationships_info
    assert len(relationships_info) == 2, relationships_info
    assert relationships_info[0].dict() == {
        "id": relationships_info[0].id,
        "entity_id": child_entity.id,
        "related_entity_id": target_entity_id,
        "relationship_type": "child_parent",
        "relation_table_id": item_table.id,
    }
    assert relationships_info[1].dict() == {
        "id": relationships_info[1].id,
        "entity_id": target_entity_id,
        "related_entity_id": parent_entity.id,
        "relationship_type": "child_parent",
        "relation_table_id": dimension_table.id,
    }

    # remove relationship and check relationship info is updated when create a new feature list version
    await table_columns_info_service._remove_parent_entity_ids(
        primary_entity_id=target_entity_id,
        parent_entity_ids_to_remove=[parent_entity.id],
    )
    new_feature = await version_service.create_new_feature_version(
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
    feature_list = await version_service.create_new_feature_list_version(
        data=FeatureListNewVersionCreate(
            source_feature_list_id=feature_list.id,
            features=[FeatureVersionInfo(name=new_feature.name, version=new_feature.version)],
        )
    )
    relationships_info = feature_list.relationships_info
    assert len(relationships_info) == 1, relationships_info
    assert relationships_info[0].dict() == {
        "id": relationships_info[0].id,
        "entity_id": child_entity.id,
        "related_entity_id": target_entity_id,
        "relationship_type": "child_parent",
        "relation_table_id": item_table.id,
    }
