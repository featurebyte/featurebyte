"""
Test MongoDB persistent backend
"""
from __future__ import annotations

from typing import Tuple

import os.path

import pytest
from git import Repo

from featurebyte.persistent.git import GitDB


@pytest.fixture(name="git_persistent")
def git_persistent_fixture() -> Tuple[GitDB, Repo]:
    """
    Patched MongoDB fixture for testing

    Returns
    -------
    Tuple[GitDB, Repo]
        Local GitDB object and local git repo
    """
    persistent = GitDB(branch="test")
    yield persistent, persistent._repo  # pylint: disable=protected-access


def test_insert_one(git_persistent, test_document):
    """
    Test inserting one document
    """
    persistent, repo = git_persistent
    persistent.insert_one(collection_name="data", document=test_document)

    # check document is inserted
    expected_doc_path = os.path.join(repo.working_tree_dir, "data", test_document["name"] + ".json")
    assert os.path.exists(expected_doc_path)

    # check commit messages
    messages = [commit.message for commit in repo.iter_commits("test", max_count=5)][-1::-1]
    assert messages == [
        "Initial commit",
        "Create document: data/Generic Document",
    ]


def test_insert_many(git_persistent, test_documents):
    """
    Test inserting many documents
    """
    persistent, repo = git_persistent
    persistent.insert_many(collection_name="data", documents=test_documents)

    # check documents are inserted
    for test_document in test_documents:
        expected_doc_path = os.path.join(
            repo.working_tree_dir, "data", test_document["name"] + ".json"
        )
        assert os.path.exists(expected_doc_path)

    # check commit messages
    messages = [commit.message for commit in repo.iter_commits("test", max_count=5)][-1::-1]
    assert messages == [
        "Initial commit",
        "Create document: data/Object 0",
        "Create document: data/Object 1",
        "Create document: data/Object 2",
    ]


def test_find_one(git_persistent, test_documents):
    """
    Test finding one document
    """
    persistent, _ = git_persistent
    persistent.insert_many(collection_name="data", documents=test_documents)
    doc = persistent.find_one(collection_name="data", filter_query={})
    assert doc == test_documents[0]


def test_find_many(git_persistent, test_documents):
    """
    Test finding many documents
    """
    persistent, _ = git_persistent
    persistent.insert_many(collection_name="data", documents=test_documents)
    docs, total = persistent.find(collection_name="data", filter_query={})
    assert list(docs) == test_documents
    assert total == 3

    # test sort
    docs, total = persistent.find(
        collection_name="data", filter_query={}, sort_by="id", sort_dir="desc"
    )
    assert list(docs) == test_documents[-1::-1]
    assert total == 3

    # test search
    docs, total = persistent.find(
        collection_name="data", filter_query={"name": "Object 1"}, sort_by="id", sort_dir="desc"
    )
    assert list(docs) == [test_documents[1]]
    assert total == 1

    docs, total = persistent.find(
        collection_name="data",
        filter_query={"id": test_documents[2]["id"]},
        sort_by="id",
        sort_dir="desc",
    )
    assert list(docs) == [test_documents[2]]
    assert total == 1

    # test pagination
    docs, total = persistent.find(collection_name="data", filter_query={}, page_size=2, page=1)
    assert list(docs) == test_documents[:2]
    assert total == 3
    docs, total = persistent.find(collection_name="data", filter_query={}, page_size=2, page=2)
    assert list(docs) == test_documents[2:]
    assert total == 3
    docs, total = persistent.find(collection_name="data", filter_query={}, page_size=0, page=2)
    assert list(docs) == test_documents
    assert total == 3


def test_update_one(git_persistent, test_documents):
    """
    Test updating one document
    """
    persistent, repo = git_persistent
    persistent.insert_many(collection_name="data", documents=test_documents)
    result = persistent.update_one(
        collection_name="data", filter_query={}, update={"$set": {"name": "apple"}}
    )

    assert result == 1
    results, total = persistent.find(collection_name="data", filter_query={})

    # only first document should be updated
    assert total == 3
    assert results[0]["name"] == "apple"
    assert results[1]["name"] == test_documents[1]["name"]
    assert results[2]["name"] == test_documents[2]["name"]

    # check commit messages
    messages = [commit.message for commit in repo.iter_commits("test", max_count=10)][-1::-1]
    assert messages == [
        "Initial commit",
        "Create document: data/Object 0",
        "Create document: data/Object 1",
        "Create document: data/Object 2",
        "Renaming document data/Object 0 -> data/apple",
        "Update document: data/apple",
    ]


def test_update_many(git_persistent, test_documents):
    """
    Test updating one document
    """
    persistent, repo = git_persistent
    persistent.insert_many(collection_name="data", documents=test_documents)
    result = persistent.update_many(
        collection_name="data", filter_query={}, update={"$set": {"value": 1}}
    )
    # expect all documents to be updated
    assert result == 3
    results, _ = persistent.find(collection_name="data", filter_query={})
    for result in results:
        assert result["value"] == 1

    # check commit messages
    messages = [commit.message for commit in repo.iter_commits("test", max_count=10)][-1::-1]
    assert messages == [
        "Initial commit",
        "Create document: data/Object 0",
        "Create document: data/Object 1",
        "Create document: data/Object 2",
        "Update document: data/Object 0",
        "Update document: data/Object 1",
        "Update document: data/Object 2",
    ]


def test_delete_one(git_persistent, test_documents):
    """
    Test deleting one document
    """
    persistent, repo = git_persistent
    persistent.insert_many(collection_name="data", documents=test_documents)
    result = persistent.delete_one(collection_name="data", filter_query={})
    # expect only one document to be deleted
    assert result == 1
    results, _ = persistent.find(collection_name="data", filter_query={})
    assert len(results) == 2

    # check commit messages
    messages = [commit.message for commit in repo.iter_commits("test", max_count=10)][-1::-1]
    assert messages == [
        "Initial commit",
        "Create document: data/Object 0",
        "Create document: data/Object 1",
        "Create document: data/Object 2",
        "Remove document: data/Object 0",
    ]


def test_delete_many(git_persistent, test_documents):
    """
    Test deleting many documents
    """
    persistent, repo = git_persistent
    persistent.insert_many(collection_name="data", documents=test_documents)
    result = persistent.delete_many(collection_name="data", filter_query={})
    # expect all documents to be deleted
    assert result == 3
    results, _ = persistent.find(collection_name="data", filter_query={})
    assert len(results) == 0

    # check commit messages
    messages = [commit.message for commit in repo.iter_commits("test", max_count=10)][-1::-1]
    assert messages == [
        "Initial commit",
        "Create document: data/Object 0",
        "Create document: data/Object 1",
        "Create document: data/Object 2",
        "Remove document: data/Object 0",
        "Remove document: data/Object 1",
        "Remove document: data/Object 2",
    ]
