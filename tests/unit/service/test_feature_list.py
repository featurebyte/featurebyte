"""
Test feature list service class
"""

import os
from pathlib import Path
from unittest.mock import patch

import pytest
from bson import ObjectId

from featurebyte import FeatureJobSetting, TableFeatureJobSetting
from featurebyte.enum import DBVarType
from featurebyte.exception import (
    DocumentError,
    DocumentInconsistencyError,
    DocumentModificationBlockedError,
    DocumentNotFoundError,
)
from featurebyte.models.base import ReferenceInfo
from featurebyte.models.feature_list import FeatureReadinessDistribution
from featurebyte.models.feature_list_namespace import FeatureListNamespaceModel
from featurebyte.query_graph.model.column_info import ColumnInfo
from featurebyte.schema.entity import EntityCreate
from featurebyte.schema.feature import FeatureNewVersionCreate, FeatureServiceCreate
from featurebyte.schema.feature_list import (
    FeatureListNewVersionCreate,
    FeatureListServiceCreate,
    FeatureVersionInfo,
)


@pytest.mark.asyncio
async def test_create_document__duplicated_feature_error(feature_list_service, feature_list):
    """Test feature creation - document inconsistency error"""
    data_dict = feature_list.model_dump(by_alias=True)
    data_dict["_id"] = ObjectId()
    data_dict["feature_ids"] = data_dict["feature_ids"] * 2
    with pytest.raises(DocumentError) as exc:
        await feature_list_service.create_document(
            data=FeatureListServiceCreate(**data_dict),
        )
    expected_msg = "Two Feature objects must not share the same name in a FeatureList object."
    assert expected_msg in str(exc.value)


@pytest.mark.asyncio
async def test_create_document__clean_up_remote_attributes_on_error(
    feature, feature_list_service, storage
):
    """Test clean up remote attributes on feature list creation error"""
    feature_list_id = ObjectId()
    catalog_id = feature.catalog_id
    expected_full_path = os.path.join(
        storage.base_path,
        f"catalog/{catalog_id}/feature_list/{feature_list_id}/feature_clusters.json",
    )

    def _create_feature_list(feature_list, features):
        _ = features
        assert feature_list.id == feature_list_id

        # check that the remote path exists
        full_path = os.path.join(storage.base_path, feature_list.feature_clusters_path)
        assert full_path == expected_full_path
        assert os.path.exists(full_path)

        # raise an error to simulate an error during feature list creation
        raise DocumentError("Some random error")

    with patch(
        "featurebyte.service.feature_list.FeatureListService._create_document"
    ) as mock_feature_list:
        mock_feature_list.side_effect = _create_feature_list
        with pytest.raises(DocumentError) as exc:
            await feature_list_service.create_document(
                data=FeatureListServiceCreate(
                    _id=feature_list_id,
                    name="some_name",
                    feature_ids=[feature.id],
                )
            )
            assert "Some random error" in str(exc.value)

    # check that the remote path has been removed
    assert not os.path.exists(expected_full_path)


@pytest.mark.asyncio
async def test_create_document__clean_up_mongo(
    feature, feature_list_service, feature_list_namespace_service
):
    """Test clean up mongo on feature list creation error"""
    feature_list_id = ObjectId()
    namespace_id = ObjectId()
    create_data = FeatureListServiceCreate(
        _id=feature_list_id,
        name="some_name",
        feature_ids=[feature.id],
        feature_list_namespace_id=namespace_id,
    )

    # test error during feature list creation (namespace not found)
    with patch.object(
        feature_list_service.feature_list_namespace_service, "create_document"
    ) as mock_namespace_create:
        mock_namespace_create.side_effect = DocumentError("Some random error")
        with pytest.raises(DocumentError) as exc:
            await feature_list_service.create_document(data=create_data)
        assert "Some random error" in str(exc.value)

    # check that the feature list has been removed
    with pytest.raises(DocumentNotFoundError):
        await feature_list_service.get_document(document_id=feature_list_id)

    with patch.object(
        feature_list_service.feature_service, "update_documents"
    ) as mock_features_update:
        mock_features_update.side_effect = DocumentError("Some random error")
        with pytest.raises(DocumentError) as exc:
            await feature_list_service.create_document(data=create_data)
        assert "Some random error" in str(exc.value)

    # check that the feature list has been removed & namespace not created
    with pytest.raises(DocumentNotFoundError):
        await feature_list_service.get_document(document_id=feature_list_id)

    with pytest.raises(DocumentNotFoundError):
        await feature_list_namespace_service.get_document(document_id=namespace_id)

    # create a new namespace
    default_feature_list_id = ObjectId()
    await feature_list_namespace_service.create_document(
        data=FeatureListNamespaceModel(
            _id=namespace_id,
            name=create_data.name,
            feature_namespace_ids=[feature.feature_namespace_id],
            feature_list_ids=[default_feature_list_id],
            default_feature_list_id=default_feature_list_id,
            catalog_id=feature_list_service.catalog_id,
            user_id=feature_list_service.user.id,
        )
    )

    # test error during feature list creation (namespace found)
    with patch.object(
        feature_list_service.feature_list_namespace_service, "update_document"
    ) as mock_namespace_update:
        mock_namespace_update.side_effect = DocumentError("Some random error")
        with pytest.raises(DocumentError) as exc:
            await feature_list_service.create_document(data=create_data)
        assert "Some random error" in str(exc.value)

    # check that the feature list has been removed
    with pytest.raises(DocumentNotFoundError):
        await feature_list_service.get_document(document_id=feature_list_id)

    with patch.object(
        feature_list_service.feature_service, "update_documents"
    ) as mock_features_update:
        mock_features_update.side_effect = DocumentError("Some random error")
        with pytest.raises(DocumentError) as exc:
            await feature_list_service.create_document(data=create_data)
        assert "Some random error" in str(exc.value)

    # check that the feature list has been removed & namespace not updated
    with pytest.raises(DocumentNotFoundError):
        await feature_list_service.get_document(document_id=feature_list_id)

    namespace = await feature_list_namespace_service.get_document(document_id=namespace_id)
    assert namespace.feature_list_ids == [default_feature_list_id]


@pytest.mark.asyncio
async def test_update_document__inconsistency_error(
    feature_list_service, feature_service, feature_list, feature
):
    """Test feature creation - document inconsistency error"""
    feat_data_dict = feature.model_dump(by_alias=True)
    feat_data_dict["_id"] = ObjectId()
    feat_data_dict["name"] = "random_name"
    feat_data_dict["feature_namespace_id"] = ObjectId()
    new_feat = await feature_service.create_document(data=FeatureServiceCreate(**feat_data_dict))

    flist_data_dict = feature_list.model_dump(by_alias=True)
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

    flist_data_dict = feature_list.model_dump(by_alias=True)
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
    all_entity_ids = set()
    for parent_entity_id, child_entity_id, _ in parent_child_table_triples:
        all_entity_ids.add(parent_entity_id)
        all_entity_ids.add(child_entity_id)
    mock_columns_info = [
        ColumnInfo(
            name=f"col_{entity_id}",
            dtype=DBVarType.VARCHAR,
            entity_id=entity_id,
        )
        for entity_id in all_entity_ids
    ]
    for child_id, parent_id, table in parent_child_table_triples:
        await table_columns_info_service._add_new_child_parent_relationships(
            entity_id=child_id,
            table_id=table.id,
            parent_entity_ids_to_add=[parent_id],
            updated_columns_info=mock_columns_info,
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
    _, descendant_ids, _ = await create_entity_family(
        entity_service,
        app_container.table_columns_info_service,
        app_container.entity_relationship_service,
        target_entity_id,
        dimension_table,
        item_table,
    )

    # check only descendant relationships are included
    feature_list = await feature_list_service.create_document(
        data=FeatureListServiceCreate(name="my_feature_list", feature_ids=[feature.id])
    )
    relationships_info = feature_list.relationships_info
    assert len(relationships_info) == 2
    expected_entities = set(descendant_ids + [target_entity_id])
    for relationship_info in relationships_info:
        assert relationship_info.entity_id in expected_entities
        assert relationship_info.related_entity_id in expected_entities
    expected_relationships_info = relationships_info

    # remove relationship and check relationship info is updated when create a new feature list version
    descendant_id_to_remove = descendant_ids[0]  # child_entity
    await app_container.table_columns_info_service._remove_parent_entity_ids(
        primary_entity_id=descendant_id_to_remove,
        parent_entity_ids_to_remove=[target_entity_id],
    )
    new_feature = await app_container.version_service.create_new_feature_version(
        data=FeatureNewVersionCreate(
            source_feature_id=feature.id,
            table_feature_job_settings=[
                TableFeatureJobSetting(
                    table_name="sf_event_table",
                    feature_job_setting=FeatureJobSetting(
                        blind_spot="1d", period="1d", offset="1h"
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

    # check the newly created feature list, the relationship info should be empty as
    # the child entity is no longer a descendant of the target entity,
    # and grand_child_entity is not included in the feature list
    assert new_feature_list.relationships_info == []

    # should contain only features that require additional entity lookups
    assert new_feature_list.features_entity_lookup_info == []


@pytest.mark.asyncio
async def test_update_readiness_distribution(feature_list_service, feature_list):
    """Test update readiness distribution method checks for block modification by"""
    reference_info = ReferenceInfo(asset_name="some_asset_name", document_id=ObjectId())
    await feature_list_service.add_block_modification_by(
        query_filter={"_id": feature_list.id}, reference_info=reference_info
    )
    updated_feature_list = await feature_list_service.get_document(document_id=feature_list.id)
    assert updated_feature_list.block_modification_by == [reference_info]

    with pytest.raises(DocumentModificationBlockedError):
        await feature_list_service.update_readiness_distribution(
            document_id=feature_list.id,
            readiness_distribution=FeatureReadinessDistribution(root=[]),
        )

    # remove block modification by so that the feature list can be removed later
    await feature_list_service.remove_block_modification_by(
        query_filter={"_id": feature_list.id}, reference_info=reference_info
    )
    updated_feature_list = await feature_list_service.get_document(document_id=feature_list.id)
    assert updated_feature_list.block_modification_by == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "situation",
    [
        "missing_feature_clusters_path",
        "feature_list_namespace_id_not_found",
        "feature_does_not_reference_feature_list",
    ],
)
async def test_delete_feature_list(
    feature_list_service,
    feature_list_namespace_service,
    feature_service,
    feature,
    storage,
    situation,
):
    """Test delete feature list method"""
    feature_list = await feature_list_service.create_document(
        data=FeatureListServiceCreate(name="my_feature_list", feature_ids=[feature.id])
    )

    if situation == "missing_feature_clusters_path":
        await storage.delete(Path(feature_list.feature_clusters_path))
        with pytest.raises(FileNotFoundError):
            await storage.get_text(Path(feature_list.feature_clusters_path))

    if situation == "feature_list_namespace_id_not_found":
        await feature_list_namespace_service.delete_document(
            document_id=feature_list.feature_list_namespace_id
        )
        with pytest.raises(DocumentNotFoundError):
            await feature_list_namespace_service.get_document(
                document_id=feature_list.feature_list_namespace_id
            )

    if situation == "feature_does_not_reference_feature_list":
        await feature_service.update_documents(
            query_filter={"_id": feature.id},
            update={"$pull": {"feature_list_ids": feature_list.id}},
        )
        feature = await feature_service.get_document(document_id=feature.id)
        assert feature.feature_list_ids == []

    await feature_list_service.delete_document(document_id=feature_list.id)

    # check that the feature list has been removed
    with pytest.raises(DocumentNotFoundError):
        await feature_list_service.get_document(document_id=feature_list.id)
