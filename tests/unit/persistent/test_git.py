"""
Test MongoDB persistent backend
"""
import os.path

import pytest

from featurebyte.persistent import DuplicateDocumentError


def _get_commit_messages(repo, max_count=5):
    """
    Extract commit messages
    """
    return [commit.message for commit in repo.iter_commits("test", max_count=max_count)][::-1]


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
    assert _get_commit_messages(repo) == [
        "Initial commit\n",
        "Create document: data/Generic Document\n",
    ]


def test_insert_one__no_id(git_persistent, test_document):
    """
    Test inserting one document without id works, and id is added
    """
    persistent, repo = git_persistent
    persistent.insert_one(collection_name="data", document=test_document)
    assert "_id" in test_document

    # check document is inserted
    expected_doc_path = os.path.join(repo.working_tree_dir, "data", test_document["name"] + ".json")
    assert os.path.exists(expected_doc_path)

    # check commit messages
    assert _get_commit_messages(repo) == [
        "Initial commit\n",
        "Create document: data/Generic Document\n",
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
    assert _get_commit_messages(repo) == [
        "Initial commit\n",
        "Create document: data/Object 0\n",
        "Create document: data/Object 1\n",
        "Create document: data/Object 2\n",
    ]


def test_find_one(git_persistent, test_documents):
    """
    Test finding one document
    """
    persistent, _ = git_persistent
    persistent.insert_many(collection_name="data", documents=test_documents)
    doc = persistent.find_one(collection_name="data", query_filter={})
    assert doc == test_documents[0]


def test_find_many(git_persistent, test_documents):
    """
    Test finding many documents
    """
    persistent, _ = git_persistent
    persistent.insert_many(collection_name="data", documents=test_documents)
    docs, total = persistent.find(collection_name="data", query_filter={})
    assert list(docs) == test_documents
    assert total == 3

    # test sort
    docs, total = persistent.find(
        collection_name="data", query_filter={}, sort_by="_id", sort_dir="desc"
    )
    assert list(docs) == test_documents[-1::-1]
    assert total == 3

    # test search
    docs, total = persistent.find(
        collection_name="data", query_filter={"name": "Object 1"}, sort_by="_id", sort_dir="desc"
    )
    assert list(docs) == [test_documents[1]]
    assert total == 1

    docs, total = persistent.find(
        collection_name="data",
        query_filter={"_id": test_documents[2]["_id"]},
        sort_by="_id",
        sort_dir="desc",
    )
    assert list(docs) == [test_documents[2]]
    assert total == 1

    # test pagination
    docs, total = persistent.find(collection_name="data", query_filter={}, page_size=2, page=1)
    assert list(docs) == test_documents[:2]
    assert total == 3
    docs, total = persistent.find(collection_name="data", query_filter={}, page_size=2, page=2)
    assert list(docs) == test_documents[2:]
    assert total == 3
    docs, total = persistent.find(collection_name="data", query_filter={}, page_size=0, page=2)
    assert list(docs) == test_documents
    assert total == 3


@pytest.mark.parametrize(
    "query_filter,valid",
    [
        ({"key1": "Object 1", "key2": None}, True),
        ({"key": {"key1": "Object 1", "key2": None}}, True),
        ({"key": {"key": {"key1": "Object 1", "key2": None}}}, True),
        ({"key": "Object 1", "key with $": None}, False),
        ({"key": {"key": "Object 1", "key with $": None}}, False),
        ({"key": {"key": {"key": "Object 1", "key with $": None}}}, False),
        ({"key": "Object 1", "key.with.period": None}, False),
        ({"key": {"key": "Object 1", "key.with.period": None}}, False),
        ({"key": {"key": {"key": "Object 1", "key.with.period": None}}}, False),
    ],
)
def test_filter_values(git_persistent, test_document, query_filter, valid):
    """
    Test find filter values validation
    """
    persistent, _ = git_persistent
    persistent.insert_one(collection_name="data", document=test_document)

    def run_find():
        persistent.find_one(collection_name="data", query_filter=query_filter)

    if valid:
        run_find()
    else:
        with pytest.raises(NotImplementedError):
            run_find()


def test_update_one(git_persistent, test_documents):
    """
    Test updating one document
    """
    persistent, repo = git_persistent
    persistent.insert_many(collection_name="data", documents=test_documents)
    result = persistent.update_one(
        collection_name="data", query_filter={}, update={"$set": {"name": "apple"}}
    )

    assert result == 1
    results, total = persistent.find(collection_name="data", query_filter={})

    # only first document should be updated
    assert total == 3
    assert results[0]["name"] == "apple"
    assert results[1]["name"] == test_documents[1]["name"]
    assert results[2]["name"] == test_documents[2]["name"]

    # check commit messages
    assert _get_commit_messages(repo, max_count=10) == [
        "Initial commit\n",
        "Create document: data/Object 0\n",
        "Create document: data/Object 1\n",
        "Create document: data/Object 2\n",
        "Rename document: data/Object 0 -> data/apple\n",
        "Update document: data/apple\n",
    ]


def test_update_many(git_persistent, test_documents):
    """
    Test updating one document
    """
    persistent, repo = git_persistent
    persistent.insert_many(collection_name="data", documents=test_documents)
    result = persistent.update_many(
        collection_name="data", query_filter={}, update={"$set": {"value": 1}}
    )
    # expect all documents to be updated
    assert result == 3
    results, _ = persistent.find(collection_name="data", query_filter={})
    for result in results:
        assert result["value"] == 1

    # check commit messages
    assert _get_commit_messages(repo, max_count=10) == [
        "Initial commit\n",
        "Create document: data/Object 0\n",
        "Create document: data/Object 1\n",
        "Create document: data/Object 2\n",
        "Update document: data/Object 0\n",
        "Update document: data/Object 1\n",
        "Update document: data/Object 2\n",
    ]


def test_update_name_to_existing(git_persistent, test_documents):
    """
    Test updating one document with name that already exists
    """
    persistent, _ = git_persistent
    persistent.insert_many(collection_name="data", documents=test_documents)
    with pytest.raises(DuplicateDocumentError) as exc:
        persistent.update_one(
            collection_name="data", query_filter={}, update={"$set": {"name": "Object 1"}}
        )
    assert str(exc.value) == "Document data/Object 1 already exists"


@pytest.mark.parametrize(
    "update,valid",
    [
        ({}, False),
        ({"$set": {"name": "Object 1"}}, True),
        ({"$set": "not a dict"}, False),
        ({"key is not $set": {"name": "Object 1"}}, False),
        ({"$set": {"name": "Object 1"}, "more than 1 key in top level": True}, False),
        ({"$set": {"name": "Object 1", "$ ok in key": None}}, True),
        ({"$set": {"name": "Object 1", "_id": "Update ID not allowed"}}, False),
        ({"$set": {"name": "Object 1", "key.with.period": None}}, False),
        ({"$set": {"name": {"key": "Object 1", "another.key.with.period": None}}}, False),
    ],
)
def test_update_values(git_persistent, test_document, update, valid):
    """
    Test update values validation
    """
    persistent, _ = git_persistent
    persistent.insert_one(collection_name="data", document=test_document)

    def run_update():
        persistent.update_one(collection_name="data", query_filter={}, update=update)

    if valid:
        run_update()
    else:
        with pytest.raises(NotImplementedError):
            run_update()


def test_delete_one(git_persistent, test_documents):
    """
    Test deleting one document
    """
    persistent, repo = git_persistent
    persistent.insert_many(collection_name="data", documents=test_documents)
    result = persistent.delete_one(collection_name="data", query_filter={})
    # expect only one document to be deleted
    assert result == 1
    results, _ = persistent.find(collection_name="data", query_filter={})
    assert len(results) == 2

    # check commit messages
    assert _get_commit_messages(repo, max_count=10) == [
        "Initial commit\n",
        "Create document: data/Object 0\n",
        "Create document: data/Object 1\n",
        "Create document: data/Object 2\n",
        "Delete document: data/Object 0\n",
    ]


def test_delete_many(git_persistent, test_documents):
    """
    Test deleting many documents
    """
    persistent, repo = git_persistent
    persistent.insert_many(collection_name="data", documents=test_documents)
    result = persistent.delete_many(collection_name="data", query_filter={})
    # expect all documents to be deleted
    assert result == 3
    results, _ = persistent.find(collection_name="data", query_filter={})
    assert len(results) == 0

    # check commit messages
    assert _get_commit_messages(repo, max_count=10) == [
        "Initial commit\n",
        "Create document: data/Object 0\n",
        "Create document: data/Object 1\n",
        "Create document: data/Object 2\n",
        "Delete document: data/Object 0\n",
        "Delete document: data/Object 1\n",
        "Delete document: data/Object 2\n",
    ]


def test_delete_one__collection_not_exist(git_persistent):
    """
    Test document from non-existent collection should work with no effect
    """
    persistent, repo = git_persistent
    result = persistent.delete_one(collection_name="no_such_collection", query_filter={})
    assert result == 0

    # check commit messages, expect no message after initial commit
    assert _get_commit_messages(repo) == ["Initial commit\n"]


def test_start_transaction__success(git_persistent):
    """
    Test start_transaction context manager
    """
    persistent, repo = git_persistent
    col = "test_col"

    with persistent.start_transaction() as session:
        session.insert_one(collection_name=col, document={"_id": "1234", "key1": "value1"})
        session.update_one(
            collection_name=col,
            query_filter={"_id": "1234"},
            update={"$set": {"key1": "value2"}},
        )

    # check commit messages
    assert _get_commit_messages(repo) == [
        "Initial commit\n",
        "Create document: test_col/1234\nUpdate document: test_col/1234\n",
    ]
    assert repo.git.status() == "On branch test\nnothing to commit, working tree clean"


def test_start_transaction__exception_within_transaction(git_persistent):
    """
    Test start_transaction context manager (exception happens within context)
    """
    persistent, repo = git_persistent
    col = "test_col"

    with pytest.raises(AssertionError):
        # set up an exception happens within the context
        with persistent.start_transaction() as session:
            session.insert_one(collection_name=col, document={"_id": "1234", "key1": "value1"})
            session.update_one(
                collection_name=col,
                query_filter={"_id": "1234"},
                update={"$set": {"key1": "value2"}},
            )
            assert False

    # check commit messages & status
    assert _get_commit_messages(repo) == ["Initial commit\n"]
    assert repo.git.status() == "On branch test\nnothing to commit, working tree clean"
