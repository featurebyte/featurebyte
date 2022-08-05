"""
Test for FeatureStore route
"""
from bson.objectid import ObjectId

from tests.unit.routes.base import BaseApiTestSuite


class TestFeatureStoreApi(BaseApiTestSuite):
    """
    TestFeatureStoreApi
    """

    class_name = "FeatureStore"
    base_route = "/feature_store"
    payload_filename = "tests/fixtures/request_payloads/feature_store.json"
    payload = BaseApiTestSuite.load_payload(payload_filename)
    create_conflict_payload_expected_detail_pairs = [
        (
            payload,
            f'FeatureStore (id: "{payload["_id"]}") already exists. '
            f'Get the existing object by `FeatureStore.get(name="sf_featurestore")`.',
        ),
        (
            {**payload, "_id": str(ObjectId())},
            f'FeatureStore (name: "sf_featurestore") already exists. '
            f'Get the existing object by `FeatureStore.get(name="sf_featurestore")`.',
        ),
    ]
    create_unprocessable_payload_expected_detail_pairs = [
        (
            {key: val for key, val in payload.items() if key != "_id"},
            [
                {
                    "loc": ["body", "_id"],
                    "msg": "field required",
                    "type": "value_error.missing",
                }
            ],
        )
    ]
