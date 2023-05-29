"""
Test FeatureNamespaceService
"""
import pytest
from bson.objectid import ObjectId

from featurebyte.schema.feature import FeatureServiceCreate


@pytest.mark.asyncio
async def test_update_document(feature_namespace_service, feature_service, feature):
    """Test update document"""
    namespace = await feature_namespace_service.get_document(
        document_id=feature.feature_namespace_id
    )
    assert namespace.feature_ids == [feature.id]

    # add new feature with the same feature namespace ID
    feat_dict = feature.dict(by_alias=True)
    feat_dict["_id"] = ObjectId()
    feat_dict["version"] = {"name": "V220917"}
    new_feat = await feature_service.create_document(data=FeatureServiceCreate(**feat_dict))

    # check updated namespace
    namespace = await feature_namespace_service.get_document(
        document_id=feature.feature_namespace_id
    )
    assert namespace.feature_ids == sorted([feature.id, new_feat.id])

    # add a new feature with different feature namespace ID
    feat_dict = feature.dict(by_alias=True)
    feat_dict["_id"] = ObjectId()
    feat_dict["name"] = "random_name"
    feat_dict["feature_namespace_id"] = ObjectId()
    new_feat = await feature_service.create_document(data=FeatureServiceCreate(**feat_dict))
    assert new_feat.feature_namespace_id != feature.feature_namespace_id

    # check updated namespace
    namespace = await feature_namespace_service.get_document(
        document_id=new_feat.feature_namespace_id
    )
    assert namespace.feature_ids == [new_feat.id]
