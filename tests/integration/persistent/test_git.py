"""
Test MongoDB persistent backend
"""
# pylint: disable=protected-access
from __future__ import annotations

from typing import Any, Dict

import os

import pytest
from bson import ObjectId

from featurebyte.persistent.git import GitDB


@pytest.fixture(name="test_document")
def test_document_fixture() -> Dict[str, Any]:
    """
    Test document to be used for testing

    Returns
    -------
    Dict[str, Any]
        Document for testing
    """
    return {
        "id": ObjectId(),
        "name": "Generic Document",
        "value": [
            {
                "key1": "value1",
                "key2": "value2",
            }
        ],
    }


def test_persistence(test_document):
    """
    Test inserting one document
    """
    # create a GitDB instance and save a document
    branch = f"integration-test-{str(ObjectId())}"
    remote_url = "git@github.com:featurebyte/playground.git"
    ssh_key_path = os.environ["GIT_SSH_KEY_PATH"]
    persistent = GitDB(
        branch=branch,
        remote_url=remote_url,
        key_path=ssh_key_path,
    )
    try:
        # use a dynamic name for the document
        doc_name = test_document["name"] = str(ObjectId())
        persistent.insert_one(collection_name="data", document=test_document)

        # create another GitDB instance and try to read the saved document
        persistent2 = GitDB(
            branch=branch,
            remote_url=remote_url,
            key_path=ssh_key_path,
        )
        doc = persistent2.find_one(collection_name="data", query_filter={"name": doc_name})
        assert doc == test_document
    finally:
        # cleanup local and remote repo
        repo, origin, ssh_cmd, branch = (
            persistent._repo,
            persistent._origin,
            persistent._ssh_cmd,
            persistent._branch,
        )
        if origin:
            with repo.git.custom_environment(GIT_SSH_COMMAND=ssh_cmd):
                origin.push(refspec=(f":{branch}"))
