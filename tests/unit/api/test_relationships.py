"""
Test relationships module
"""
from featurebyte.api.relationships import Relationships


def test_relationships_list__empty_response():
    """
    Test relationships list
    """
    relationships = Relationships.list()
    assert relationships.shape[0] == 0
