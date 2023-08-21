"""
Test worker module
"""
from featurebyte.worker import format_mongo_uri_for_scheduler


def test_format_mongo_uri():
    """
    Test format_mongo_uri
    """
    assert (
        format_mongo_uri_for_scheduler("mongodb://localhost:27017/") == "mongodb://localhost:27017/"
    )
    assert (
        format_mongo_uri_for_scheduler("mongodb://localhost:27017/featurebyte")
        == "mongodb://localhost:27017/featurebyte"
    )
    assert (
        format_mongo_uri_for_scheduler("mongodb://localhost:27017/admin")
        == "mongodb://localhost:27017/?authSource=admin"
    )
    assert (
        format_mongo_uri_for_scheduler("mongodb://localhost:27017/admin?")
        == "mongodb://localhost:27017/?authSource=admin"
    )
    assert (
        format_mongo_uri_for_scheduler(
            "mongodb://localhost:27017,localhost:27018/admin?replicaSet=rs&ssl=false"
        )
        == "mongodb://localhost:27017,localhost:27018/?authSource=admin&replicaSet=rs&ssl=false"
    )
