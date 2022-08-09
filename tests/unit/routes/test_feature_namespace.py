"""
Test for FeatureNamespace route
"""
from bson import ObjectId

from featurebyte.models.feature import FeatureNamespaceModel, FeatureReadiness
from tests.unit.routes.base import BaseApiTestSuite


class TestFeatureNamespaceApi(BaseApiTestSuite):
    """
    TestFeatureNamespaceApi
    """

    class_name = "FeatureNamespace"
    base_route = "/feature_namespace"
    feature_payload = BaseApiTestSuite.load_payload(
        "tests/fixtures/request_payloads/feature_sum_30m.json"
    )
    payload = FeatureNamespaceModel(
        _id=ObjectId(),
        name="feature_namespace",
        description=None,
        version_ids=[feature_payload["_id"]],
        versions=["V220807"],
        readiness=FeatureReadiness.DRAFT,
        default_version_id=ObjectId(feature_payload["_id"]),
    ).json_dict()
    create_conflict_payload_expected_detail_pairs = [
        (payload, f'FeatureNamespace (id: "{payload["_id"]}") already exists.'),
        (
            {**payload, "_id": str(ObjectId())},
            f'FeatureNamespace (name: "feature_namespace") already exists.',
        ),
    ]
    create_unprocessable_payload_expected_detail_pairs = [
        (
            {key: value for key, value in payload.items() if key != "name"},
            [{"loc": ["body", "name"], "msg": "field required", "type": "value_error.missing"}],
        )
    ]
    not_found_save_suggestion = False

    def multiple_success_payload_generator(self, api_client):
        """Create multiple payload for setting up create_multiple_success_responses fixture"""
        for i in range(3):
            payload = self.payload.copy()
            payload["_id"] = str(ObjectId())
            yield payload
