"""
Test model indexes
"""
import importlib
import inspect
import os
import pkgutil
from collections import defaultdict

from pymongo.operations import IndexModel

from featurebyte.models import base
from featurebyte.models.task import Task


def test_indexes():
    """
    Test model indexes
    """

    def get_verified_indexes(model):
        """
        Get indexes for model
        """
        if hasattr(model.Settings, "indexes"):
            indexed_fields = set()
            # validate field exists
            indexes = model.Settings.indexes
            for index in indexes:
                if isinstance(index, IndexModel):
                    for key in index.document["key"]:
                        indexed_fields.add(key)
                        assert key in model.__fields__, f"{key} not in {model}"
                else:
                    for (key, _) in index:
                        indexed_fields.add(key)
                        assert key in model.__fields__, f"{key} not in {model}"

            if model != Task:
                # check that basic fields are indexed for all models except Task
                for field in ["user_id", "name", "created_at", "updated_at"]:
                    assert field in indexed_fields, f"{field} not indexed for {resource}"

            # check catalog id is indexed for catalog-specific models
            if issubclass(resource, base.FeatureByteCatalogBaseDocumentModel):
                assert "catalog_id" in indexed_fields, f"catalog_id not indexed for {resource}"
            else:
                assert "catalog_id" not in indexed_fields, f"catalog_id indexed for {resource}"

            return indexes
        return []

    # load all document model classes
    models_path = os.path.dirname(base.__file__)
    table_indexes = defaultdict(list)
    for (_, name, _) in pkgutil.iter_modules([models_path]):
        module = importlib.import_module(f"featurebyte.models.{name}")
        for attr in dir(module):
            resource = getattr(module, attr)
            if (
                inspect.isclass(resource)
                and issubclass(resource, base.FeatureByteBaseDocumentModel)
                and hasattr(resource, "Settings")
            ):
                # only include classes with collection name
                if (
                    resource != base.FeatureByteBaseDocumentModel
                    and resource != base.FeatureByteCatalogBaseDocumentModel
                    and hasattr(resource.Settings, "collection_name")
                ):
                    table_indexes[resource.Settings.collection_name].extend(
                        get_verified_indexes(resource)
                    )

    for collection_name, indexes in table_indexes.items():
        combined_indexes = {}
        for index in indexes:
            if isinstance(index, IndexModel):
                combined_indexes[index.document["name"]] = index
            else:
                combined_indexes[str(index)] = index

        if not combined_indexes:
            raise ValueError(f"No indexes defined for {collection_name}")
