"""
Test FeatureListNamespaceService
"""
import pytest
from bson.objectid import ObjectId

from featurebyte.models.feature_list import FeatureListStatus
from featurebyte.schema.feature import FeatureServiceCreate
from featurebyte.schema.feature_list import FeatureListCreate
from featurebyte.schema.feature_list_namespace import FeatureListNamespaceServiceUpdate


@pytest.mark.asyncio
async def test_update_document(
    feature_service, feature_list_namespace_service, feature_list_service, feature, feature_list
):
    """Test update document"""
    namespace = await feature_list_namespace_service.get_document(
        document_id=feature_list.feature_list_namespace_id
    )
    assert namespace.feature_list_ids == [feature_list.id]

    # add new feature with the same feature namespace ID
    feat_dict = feature.dict(by_alias=True)
    feat_dict["_id"] = ObjectId()
    feat_dict["version"] = {"name": "V220917"}
    new_feat = await feature_service.create_document(data=FeatureServiceCreate(**feat_dict))

    # add new feature list with the same feature list namespace ID
    flist_dict = feature_list.dict(by_alias=True)
    flist_dict["_id"] = ObjectId()
    flist_dict["version"] = {"name": "V220917"}
    flist_dict["feature_ids"] = [new_feat.id]
    new_flist = await feature_list_service.create_document(data=FeatureListCreate(**flist_dict))

    # check updated namespace
    namespace = await feature_list_namespace_service.get_document(
        document_id=feature_list.feature_list_namespace_id
    )
    assert namespace.feature_list_ids == sorted([feature_list.id, new_flist.id])

    # add a new feature with different feature namespace ID
    feat_dict = feature.dict(by_alias=True)
    feat_dict["_id"] = ObjectId()
    feat_dict["name"] = "random_name"
    feat_dict["feature_namespace_id"] = ObjectId()
    new_feat = await feature_service.create_document(data=FeatureServiceCreate(**feat_dict))
    assert new_feat.feature_namespace_id != feature.feature_namespace_id

    # add a new feature list with different feature list namespace ID
    flist_dict = feature_list.dict(by_alias=True)
    flist_dict["_id"] = ObjectId()
    flist_dict["name"] = "random_feature_list_name"
    flist_dict["version"] = {"name": "V220917"}
    flist_dict["feature_ids"] = [new_feat.id]
    flist_dict["feature_list_namespace_id"] = ObjectId()
    new_flist = await feature_list_service.create_document(data=FeatureListCreate(**flist_dict))

    # check updated namespace
    namespace = await feature_list_namespace_service.get_document(
        document_id=new_flist.feature_list_namespace_id
    )
    assert namespace.feature_list_ids == [new_flist.id]


@pytest.mark.asyncio
async def test_update_document__status(feature_list_namespace_service, feature_list):
    """Test update status"""
    original_doc = await feature_list_namespace_service.get_document(
        document_id=feature_list.feature_list_namespace_id
    )
    assert original_doc.status == FeatureListStatus.DRAFT
    updated_doc = await feature_list_namespace_service.update_document(
        document_id=feature_list.feature_list_namespace_id,
        data=FeatureListNamespaceServiceUpdate(status=FeatureListStatus.TEMPLATE),
    )
    assert updated_doc.status == FeatureListStatus.TEMPLATE
    assert updated_doc.readiness_distribution == original_doc.readiness_distribution
    assert updated_doc.feature_list_ids == original_doc.feature_list_ids
    assert updated_doc.default_version_mode == original_doc.default_version_mode
