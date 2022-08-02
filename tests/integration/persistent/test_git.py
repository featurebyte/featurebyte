"""
Test GitDB persistent backend
"""
# pylint: disable=protected-access
from __future__ import annotations

from typing import Any, Dict

import os
from unittest.mock import patch

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
        "_id": ObjectId(),
        "name": "Generic Document",
        "value": [
            {
                "key1": "value1",
                "key2": "value2",
            }
        ],
    }


@pytest.fixture(name="persistent_data")
def persistent_data_fixture():
    """
    GitDB persistent fixture
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
    persistent.insert_doc_name_func("data", lambda doc: doc["name"])
    persistent.insert_doc_name_func("data1", lambda doc: doc["name"])
    persistent.insert_doc_name_func("data2", lambda doc: doc["name"])
    persistent.insert_doc_name_func("__audit__data", lambda doc: doc["name"])
    persistent.insert_doc_name_func("__audit__data1", lambda doc: doc["name"])
    persistent.insert_doc_name_func("__audit__data2", lambda doc: doc["name"])
    yield persistent, branch, remote_url, ssh_key_path

    # cleanup local and remote repo
    repo, ssh_cmd, branch = (
        persistent.repo,
        persistent.ssh_cmd,
        persistent.branch,
    )
    origin = repo.remotes.origin
    if origin:
        with repo.git.custom_environment(GIT_SSH_COMMAND=ssh_cmd):
            origin.push(refspec=(f":{branch}"))


def _get_commit_messages(repo, branch, max_count=5):
    """
    Extract commit messages
    """
    return [commit.message for commit in repo.iter_commits(branch, max_count=max_count)][::-1]


@pytest.mark.asyncio
async def test_persistence(test_document, persistent_data):
    """
    Test inserting one document
    """

    # pylint: disable=too-many-locals
    persistent, branch, remote_url, ssh_key_path = persistent_data

    # use a dynamic name for the document
    doc_name = test_document["name"] = str(ObjectId())
    await persistent.insert_one(collection_name="data", document=test_document)

    # create another GitDB instance and try to read the saved document
    persistent2 = GitDB(
        branch=branch,
        remote_url=remote_url,
        key_path=ssh_key_path,
    )
    persistent2.insert_doc_name_func("data", lambda doc: doc["name"])
    doc = await persistent2.find_one(collection_name="data", query_filter={"name": doc_name})
    assert doc == test_document

    # test transaction (normal case)
    messages_first = _get_commit_messages(persistent.repo, branch)
    expected_clean_status = f"On branch {branch}\nnothing to commit, working tree clean"
    async with persistent.start_transaction() as session:
        doc["_id"] = ObjectId()
        doc["name"] = str(doc["_id"])
        doc1_id = await session.insert_one(collection_name="data1", document=doc)
        doc["_id"] = ObjectId()
        doc["name"] = str(doc["_id"])
        doc2_id = await session.insert_one(collection_name="data2", document=doc)

    # When start a transaction, it did a shallow fetch first. Therefore, not all commits are kept.
    messages_second = _get_commit_messages(persistent.repo, branch)
    assert persistent.repo.git.status() == expected_clean_status
    assert messages_second == (
        messages_first[-1:]
        + [
            (
                f"Create document: data1/{doc1_id}\n"
                f'Create document: __audit__data1/insert: "{doc1_id}"\n'
                f"Create document: data2/{doc2_id}\n"
                f'Create document: __audit__data2/insert: "{doc2_id}"\n'
            )
        ]
    )

    # test transaction failure within the context
    with pytest.raises(AssertionError):
        async with persistent.start_transaction() as session:
            await session.insert_one(collection_name="data3", document=doc)
            await session.insert_one(collection_name="data4", document=doc)
            assert False

    # check no commit is written
    assert _get_commit_messages(persistent.repo, branch) == messages_second[-1:]
    assert persistent.repo.git.status() == expected_clean_status

    # test push failure
    with pytest.raises(AssertionError):
        with patch("featurebyte.persistent.git.GitDB._push") as mock_push:
            # each insert calls a _sync_push, only the third time raises an exception
            # to simulate the push failure after context
            mock_push.side_effect = [None, None, AssertionError]
            async with persistent.start_transaction() as session:
                await session.insert_one(collection_name="data3", document=doc)
                await session.insert_one(collection_name="data4", document=doc)

    # check no commit is written
    assert _get_commit_messages(persistent.repo, branch) == messages_second[-1:]
    assert persistent.repo.git.status() == expected_clean_status
