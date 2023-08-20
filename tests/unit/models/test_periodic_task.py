"""
Test periodic task model
"""
from pymongo.operations import IndexModel

from featurebyte.models.periodic_task import PeriodicTask


def test_name_not_in_indexes():
    """
    Test name is not in indexes
    """
    # name is indexed by celerybeatmongo package and should be excluded to avoid conflict
    for index in PeriodicTask.Settings.indexes:
        if isinstance(index, IndexModel):
            assert "name" not in index.document["name"]
