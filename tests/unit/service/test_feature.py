"""
Test FeatureService
"""
import pytest

from featurebyte.models.feature import FeatureReadiness
from featurebyte.schema.feature import FeatureServiceUpdate
from featurebyte.schema.feature_list import FeatureListCreate
from featurebyte.schema.feature_list_namespace import FeatureListNamespaceServiceUpdate
from featurebyte.schema.feature_namespace import FeatureNamespaceServiceUpdate
from tests.unit.service.test_feature_namespace_service import insert_feature_into_persistent


async def check_states_readiness_change(
    feature_service,
    feature_namespace_service,
    feature_list_service,
    feature_list_namespace_service,
    new_feature_id,
    new_flist,
    new_feature_next_readiness,
    expected_default_feature_id,
    expected_default_readiness,
    expected_feature_list_readiness_distribution,
    expected_default_feature_list_id,
    expected_default_feature_list_readiness_distribution,
):
    new_feat = await feature_service.update_document(
        document_id=new_feature_id,
        data=FeatureServiceUpdate(readiness=new_feature_next_readiness),
    )
    assert new_feat.feature_list_ids == [new_flist.id]

    # check feature namespace get updated (new feature become the default one)
    updated_feat_namespace = await feature_namespace_service.get_document(
        document_id=new_feat.feature_namespace_id
    )
    assert updated_feat_namespace.default_feature_id == expected_default_feature_id
    assert updated_feat_namespace.readiness == expected_default_readiness

    # check feature list version get updated (new feature list readiness distribution get updated)
    updated_flist = await feature_list_service.get_document(document_id=new_flist.id)
    assert (
        updated_flist.readiness_distribution.__root__
        == expected_feature_list_readiness_distribution
    )

    # check feature list namespace (new feature list becomes the default one)
    updated_flist_namespace = await feature_list_namespace_service.get_document(
        document_id=new_flist.feature_list_namespace_id,
    )
    assert updated_flist_namespace.default_feature_list_id == expected_default_feature_list_id
    assert (
        updated_flist_namespace.readiness_distribution.__root__
        == expected_default_feature_list_readiness_distribution
    )


@pytest.mark.asyncio
async def test_update_document__auto_default_version_mode(
    feature_service,
    feature_namespace_service,
    feature_list_service,
    feature_list_namespace_service,
    feature,
    feature_list,
    user,
):
    """Test update document (auto default version mode)"""
    persistent = feature_service.persistent

    # add a deprecated feature version first
    new_feature_id = await insert_feature_into_persistent(
        user=user,
        persistent=persistent,
        version_suffix="_1",
        readiness=FeatureReadiness.DEPRECATED.value,
    )
    feat_namespace = await feature_namespace_service.update_document(
        document_id=feature.feature_namespace_id,
        data=FeatureNamespaceServiceUpdate(feature_id=new_feature_id),
    )
    assert len(feat_namespace.feature_ids) == 2
    assert new_feature_id in feat_namespace.feature_ids
    assert feat_namespace.default_version_mode == "AUTO"
    assert feat_namespace.default_feature_id == feature.id
    assert feat_namespace.readiness == "DRAFT"

    # create another feature list version
    new_flist = await feature_list_service.create_document(
        data=FeatureListCreate(
            feature_ids=[new_feature_id],
            feature_list_namespace_id=feature_list.feature_list_namespace_id,
            name="sf_feature_list",
            version="V220914",
        )
    )
    flist_namespace = await feature_list_namespace_service.get_document(
        document_id=feature_list.feature_list_namespace_id,
    )
    assert len(flist_namespace.feature_list_ids) == 2
    assert new_flist.id in flist_namespace.feature_list_ids
    assert new_flist.feature_list_namespace_id == feature_list.feature_list_namespace_id
    assert flist_namespace.default_version_mode == "AUTO"
    assert flist_namespace.default_feature_list_id == feature_list.id
    assert flist_namespace.readiness_distribution.__root__ == [{"readiness": "DRAFT", "count": 1}]

    # upgrade new feature's readiness level to production
    await check_states_readiness_change(
        feature_service=feature_service,
        feature_namespace_service=feature_namespace_service,
        feature_list_service=feature_list_service,
        feature_list_namespace_service=feature_list_namespace_service,
        new_feature_id=new_feature_id,
        new_flist=new_flist,
        new_feature_next_readiness=FeatureReadiness.PRODUCTION_READY,
        expected_default_feature_id=new_feature_id,
        expected_default_readiness=FeatureReadiness.PRODUCTION_READY,
        expected_feature_list_readiness_distribution=[
            {"readiness": "PRODUCTION_READY", "count": 1}
        ],
        expected_default_feature_list_id=new_flist.id,
        expected_default_feature_list_readiness_distribution=[
            {"readiness": "PRODUCTION_READY", "count": 1}
        ],
    )

    # downgrade new feature's readiness from production ready to deprecated
    await check_states_readiness_change(
        feature_service=feature_service,
        feature_namespace_service=feature_namespace_service,
        feature_list_service=feature_list_service,
        feature_list_namespace_service=feature_list_namespace_service,
        new_feature_id=new_feature_id,
        new_flist=new_flist,
        new_feature_next_readiness=FeatureReadiness.DEPRECATED,
        expected_default_feature_id=feature.id,
        expected_default_readiness=FeatureReadiness.DRAFT,
        expected_feature_list_readiness_distribution=[{"readiness": "DEPRECATED", "count": 1}],
        expected_default_feature_list_id=feature_list.id,
        expected_default_feature_list_readiness_distribution=[{"readiness": "DRAFT", "count": 1}],
    )


@pytest.mark.asyncio
async def test_update_document__manual_default_version_mode(
    feature_service,
    feature_namespace_service,
    feature_list_service,
    feature_list_namespace_service,
    feature,
    feature_list,
    user,
):
    """Test update document (manual default version mode)"""
    persistent = feature_service.persistent

    # update feature namespace default version mode to manual first
    feat_namespace = await feature_namespace_service.update_document(
        document_id=feature.feature_namespace_id,
        data=FeatureNamespaceServiceUpdate(default_version_mode="MANUAL"),
    )
    assert feat_namespace.default_version_mode == "MANUAL"
    flist_namespace = await feature_list_namespace_service.update_document(
        document_id=feature_list.feature_list_namespace_id,
        data=FeatureListNamespaceServiceUpdate(default_version_mode="MANUAL"),
    )
    assert flist_namespace.default_version_mode == "MANUAL"

    # add a production ready feature version first
    new_feature_id = await insert_feature_into_persistent(
        user=user,
        persistent=persistent,
        version_suffix="_1",
        readiness=FeatureReadiness.PRODUCTION_READY.value,
    )
    feat_namespace = await feature_namespace_service.update_document(
        document_id=feature.feature_namespace_id,
        data=FeatureNamespaceServiceUpdate(feature_id=new_feature_id),
    )
    assert feat_namespace.default_feature_id == feature.id
    assert feat_namespace.readiness == "DRAFT"

    # create another new feature list version
    new_flist = await feature_list_service.create_document(
        data=FeatureListCreate(
            feature_ids=[new_feature_id],
            feature_list_namespace_id=feature_list.feature_list_namespace_id,
            name="sf_feature_list",
            version="V220914",
        )
    )
    flist_namespace = await feature_list_namespace_service.get_document(
        document_id=new_flist.feature_list_namespace_id,
    )
    assert flist_namespace.default_feature_list_id == feature_list.id
    assert flist_namespace.readiness_distribution.__root__ == [{"readiness": "DRAFT", "count": 1}]

    # update feature namespace default version mode to auto first
    feat_namespace = await feature_namespace_service.update_document(
        document_id=feature.feature_namespace_id,
        data=FeatureNamespaceServiceUpdate(default_version_mode="AUTO"),
    )
    assert feat_namespace.default_feature_id == new_feature_id
    assert feat_namespace.readiness == "PRODUCTION_READY"

    # check feature list namespace not affected
    flist_namespace = await feature_list_namespace_service.get_document(
        document_id=new_flist.feature_list_namespace_id,
    )
    assert flist_namespace.default_feature_list_id == feature_list.id
    assert flist_namespace.readiness_distribution.__root__ == [{"readiness": "DRAFT", "count": 1}]

    # update feature list namespace default version to auto
    flist_namespace = await feature_list_namespace_service.update_document(
        document_id=feature_list.feature_list_namespace_id,
        data=FeatureListNamespaceServiceUpdate(default_version_mode="AUTO"),
    )
    assert flist_namespace.default_feature_list_id == new_flist.id
    assert flist_namespace.readiness_distribution.__root__ == [
        {"readiness": "PRODUCTION_READY", "count": 1}
    ]
