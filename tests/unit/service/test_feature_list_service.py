"""
Test feature list service class
"""
import pytest
from bson.objectid import ObjectId

from featurebyte.exception import DocumentError, DocumentInconsistencyError
from featurebyte.schema.feature import FeatureCreate
from featurebyte.schema.feature_list import FeatureListCreate


@pytest.mark.asyncio
async def test_update_document__duplicated_feature_error(feature_list_service, feature_list):
    """Test feature creation - document inconsistency error"""
    data_dict = feature_list.dict(by_alias=True)
    data_dict["_id"] = ObjectId()
    data_dict["version"] = "V220917"
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
    flist_data_dict["version"] = "V220917"
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
