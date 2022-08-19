"""
Test MongoDB persistent backend
"""
import os.path
from datetime import datetime
from unittest.mock import patch

import pytest
from bson.objectid import ObjectId
from freezegun import freeze_time

from featurebyte.models.persistent import AuditActionType
from featurebyte.persistent import DuplicateDocumentError


def _get_commit_messages(repo, max_count=5):
    """
    Extract commit messages
    """
    return [commit.message for commit in repo.iter_commits("test", max_count=max_count)][::-1]


@pytest.mark.asyncio
async def test_insert_one(git_persistent, test_document):
    """
    Test inserting one document
    """
    persistent, repo = git_persistent
    with patch(
        "featurebyte.persistent.git.GitDB._reset_branch", wraps=persistent._reset_branch
    ) as mock_reset:
        inserted_id = await persistent.insert_one(collection_name="data", document=test_document)
    assert mock_reset.call_args[1] == {"rate_limit_fetch": False}

    # check document is inserted
    expected_doc_path = os.path.join(repo.working_tree_dir, "data", test_document["name"] + ".json")
    assert os.path.exists(expected_doc_path)

    # check commit messages
    messages = _get_commit_messages(repo)
    assert messages == [
        "Initial commit\n",
        (
            "Create document: data/Generic Document\n"
            'Create document: __audit__data/insert: "Generic Document"\n'
        ),
    ]

    # check audit record is inserted
    results, _ = await persistent.find(
        collection_name="__audit__data", query_filter={"document_id": inserted_id}
    )
    assert len(results) == 1
    assert isinstance(results[0]["action_at"], datetime)
    assert results[0]["action_type"] == AuditActionType.INSERT
    assert results[0]["previous_values"] == {}


@pytest.mark.asyncio
async def test_insert_one__no_id(git_persistent, test_document):
    """
    Test inserting one document without id works, and id is added
    """
    persistent, repo = git_persistent
    inserted_id = await persistent.insert_one(collection_name="data", document=test_document)
    assert "_id" in test_document

    # check document is inserted
    expected_doc_path = os.path.join(repo.working_tree_dir, "data", test_document["name"] + ".json")
    assert os.path.exists(expected_doc_path)

    # check commit messages
    assert _get_commit_messages(repo) == [
        "Initial commit\n",
        (
            "Create document: data/Generic Document\n"
            'Create document: __audit__data/insert: "Generic Document"\n'
        ),
    ]

    # check audit record is inserted
    results, _ = await persistent.find(
        collection_name="__audit__data", query_filter={"document_id": inserted_id}
    )
    assert len(results) == 1
    assert isinstance(results[0]["action_at"], datetime)
    assert results[0]["action_type"] == AuditActionType.INSERT
    assert results[0]["previous_values"] == {}


@pytest.mark.asyncio
async def test_insert_many(git_persistent, test_documents):
    """
    Test inserting many documents
    """
    persistent, repo = git_persistent
    with patch(
        "featurebyte.persistent.git.GitDB._reset_branch", wraps=persistent._reset_branch
    ) as mock_reset:
        inserted_ids = await persistent.insert_many(
            collection_name="data", documents=test_documents
        )
    assert mock_reset.call_args[1] == {"rate_limit_fetch": False}

    # check documents are inserted
    assert [doc["_id"] for doc in test_documents] == inserted_ids
    for test_document in test_documents:
        expected_doc_path = os.path.join(
            repo.working_tree_dir, "data", test_document["name"] + ".json"
        )
        assert os.path.exists(expected_doc_path)

    # check commit messages
    assert _get_commit_messages(repo, max_count=10) == [
        "Initial commit\n",
        (
            "Create document: data/Object 0\n"
            "Create document: data/Object 1\n"
            "Create document: data/Object 2\n"
            'Create document: __audit__data/insert: "Object 0"\n'
            'Create document: __audit__data/insert: "Object 1"\n'
            'Create document: __audit__data/insert: "Object 2"\n'
        ),
    ]

    # check audit record is inserted
    for doc in test_documents:
        results, _ = await persistent.find(
            collection_name="__audit__data", query_filter={"document_id": doc["_id"]}
        )
        assert len(results) == 1
        assert isinstance(results[0]["action_at"], datetime)
        assert results[0]["action_type"] == AuditActionType.INSERT
        assert results[0]["previous_values"] == {}


@pytest.mark.asyncio
async def test_find_one(git_persistent, test_documents):
    """
    Test finding one document
    """
    persistent, repo = git_persistent
    await persistent.insert_many(collection_name="data", documents=test_documents)
    with patch(
        "featurebyte.persistent.git.GitDB._reset_branch", wraps=persistent._reset_branch
    ) as mock_reset:
        doc = await persistent.find_one(collection_name="data", query_filter={})
    assert mock_reset.call_args[1] == {}
    assert doc == test_documents[0]

    # check no new audit record is inserted for find
    for doc in test_documents:
        results, _ = await persistent.find(
            collection_name="__audit__data", query_filter={"document_id": doc["_id"]}
        )
        assert len(results) == 1
        assert isinstance(results[0]["action_at"], datetime)
        assert results[0]["action_type"] == AuditActionType.INSERT
        assert results[0]["previous_values"] == {}

    assert _get_commit_messages(repo, max_count=10) == [
        "Initial commit\n",
        (
            "Create document: data/Object 0\n"
            "Create document: data/Object 1\n"
            "Create document: data/Object 2\n"
            'Create document: __audit__data/insert: "Object 0"\n'
            'Create document: __audit__data/insert: "Object 1"\n'
            'Create document: __audit__data/insert: "Object 2"\n'
        ),
    ]


@pytest.mark.asyncio
async def test_find_many(git_persistent, test_documents):
    """
    Test finding many documents
    """
    persistent, _ = git_persistent
    await persistent.insert_many(collection_name="data", documents=test_documents)
    docs, total = await persistent.find(collection_name="data", query_filter={})
    assert list(docs) == test_documents
    assert total == 3

    # test sort
    with patch(
        "featurebyte.persistent.git.GitDB._reset_branch", wraps=persistent._reset_branch
    ) as mock_reset:
        docs, total = await persistent.find(
            collection_name="data", query_filter={}, sort_by="_id", sort_dir="desc"
        )
    assert mock_reset.call_args[1] == {}
    assert list(docs) == test_documents[-1::-1]
    assert total == 3

    # test search
    docs, total = await persistent.find(
        collection_name="data", query_filter={"name": "Object 1"}, sort_by="_id", sort_dir="desc"
    )
    assert list(docs) == [test_documents[1]]
    assert total == 1

    docs, total = await persistent.find(
        collection_name="data",
        query_filter={"_id": test_documents[2]["_id"]},
        sort_by="_id",
        sort_dir="desc",
    )
    assert list(docs) == [test_documents[2]]
    assert total == 1

    # test pagination
    docs, total = await persistent.find(
        collection_name="data", query_filter={}, page_size=2, page=1
    )
    assert list(docs) == test_documents[:2]
    assert total == 3
    docs, total = await persistent.find(
        collection_name="data", query_filter={}, page_size=2, page=2
    )
    assert list(docs) == test_documents[2:]
    assert total == 3
    docs, total = await persistent.find(
        collection_name="data", query_filter={}, page_size=0, page=2
    )
    assert list(docs) == test_documents
    assert total == 3

    # check no new audit record is inserted for find
    for doc in test_documents:
        results, _ = await persistent.find(
            collection_name="__audit__data", query_filter={"document_id": doc["_id"]}
        )
        assert len(results) == 1
        assert isinstance(results[0]["action_at"], datetime)
        assert results[0]["action_type"] == AuditActionType.INSERT
        assert results[0]["previous_values"] == {}


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
@pytest.mark.asyncio
async def test_filter_values(git_persistent, test_document, query_filter, valid):
    """
    Test find filter values validation
    """
    persistent, _ = git_persistent
    await persistent.insert_one(collection_name="data", document=test_document)

    async def run_find():
        await persistent.find_one(collection_name="data", query_filter=query_filter)

    if valid:
        await run_find()
    else:
        with pytest.raises(NotImplementedError):
            await run_find()


@pytest.mark.asyncio
@freeze_time("2022-08-01")
async def test_update_one(git_persistent, test_documents):
    """
    Test updating one document
    """
    persistent, repo = git_persistent
    await persistent.insert_many(collection_name="data", documents=test_documents)

    with patch(
        "featurebyte.persistent.git.GitDB._reset_branch", wraps=persistent._reset_branch
    ) as mock_reset:
        result = await persistent.update_one(
            collection_name="data", query_filter={}, update={"$set": {"name": "apple"}}
        )
    assert mock_reset.call_args[1] == {"rate_limit_fetch": False}

    assert result == 1
    results, total = await persistent.find(collection_name="data", query_filter={})

    # only first document should be updated
    assert total == 3
    assert results[0]["name"] == "apple"
    assert results[1] == test_documents[1]
    assert results[2] == test_documents[2]

    # check commit messages
    assert _get_commit_messages(repo, max_count=10) == [
        "Initial commit\n",
        (
            "Create document: data/Object 0\n"
            "Create document: data/Object 1\n"
            "Create document: data/Object 2\n"
            'Create document: __audit__data/insert: "Object 0"\n'
            'Create document: __audit__data/insert: "Object 1"\n'
            'Create document: __audit__data/insert: "Object 2"\n'
        ),
        (
            "Rename document: data/Object 0 -> data/apple\n"
            "Update document: data/apple\n"
            'Create document: __audit__data/update: "Object 0"\n'
        ),
    ]

    # check audit record is inserted
    updated_doc = test_documents[0]
    results, _ = await persistent.find(
        collection_name="__audit__data", query_filter={"document_id": updated_doc["_id"]}
    )
    assert len(results) == 2
    assert isinstance(results[0].pop("_id"), ObjectId)
    assert isinstance(results[0].pop("action_at"), datetime)
    assert isinstance(results[1].pop("_id"), ObjectId)
    assert isinstance(results[1].pop("action_at"), datetime)
    assert results == [
        {
            "name": 'insert: "Object 0"',
            "document_id": updated_doc["_id"],
            "action_type": AuditActionType.INSERT,
            "previous_values": {},
            "current_values": {
                "name": "Object 0",
                "value": [{"key1": "value1", "key2": "value2"}],
                "created_at": datetime(2022, 8, 1),
            },
            "user_id": None,
        },
        {
            "name": 'update: "Object 0"',
            "document_id": updated_doc["_id"],
            "action_type": AuditActionType.UPDATE,
            "previous_values": {"name": "Object 0"},
            "current_values": {
                "name": "apple",
                "updated_at": datetime(2022, 8, 1),
            },
            "user_id": None,
        },
    ]


@pytest.mark.asyncio
@freeze_time("2022-08-01")
async def test_update_many(git_persistent, test_documents):
    """
    Test updating one document
    """
    persistent, repo = git_persistent
    await persistent.insert_many(collection_name="data", documents=test_documents)
    with patch(
        "featurebyte.persistent.git.GitDB._reset_branch", wraps=persistent._reset_branch
    ) as mock_reset:
        result = await persistent.update_many(
            collection_name="data", query_filter={}, update={"$set": {"value": 1}}
        )
    assert mock_reset.call_args[1] == {"rate_limit_fetch": False}

    # expect all documents to be updated
    assert result == 3
    results, _ = await persistent.find(collection_name="data", query_filter={})
    for result in results:
        assert result["value"] == 1

    # check commit messages
    assert _get_commit_messages(repo, max_count=15) == [
        "Initial commit\n",
        (
            "Create document: data/Object 0\n"
            "Create document: data/Object 1\n"
            "Create document: data/Object 2\n"
            'Create document: __audit__data/insert: "Object 0"\n'
            'Create document: __audit__data/insert: "Object 1"\n'
            'Create document: __audit__data/insert: "Object 2"\n'
        ),
        (
            "Update document: data/Object 0\n"
            "Update document: data/Object 1\n"
            "Update document: data/Object 2\n"
            'Create document: __audit__data/update: "Object 0"\n'
            'Create document: __audit__data/update: "Object 1"\n'
            'Create document: __audit__data/update: "Object 2"\n'
        ),
    ]

    # check audit record is inserted
    for i, updated_doc in enumerate(test_documents):
        results, _ = await persistent.find(
            collection_name="__audit__data", query_filter={"document_id": updated_doc["_id"]}
        )
        assert len(results) == 2
        assert isinstance(results[0].pop("_id"), ObjectId)
        assert isinstance(results[0].pop("action_at"), datetime)
        assert isinstance(results[1].pop("_id"), ObjectId)
        assert isinstance(results[1].pop("action_at"), datetime)
        assert results == [
            {
                "name": f'insert: "Object {i}"',
                "document_id": updated_doc["_id"],
                "action_type": AuditActionType.INSERT,
                "previous_values": {},
                "current_values": {
                    "name": f"Object {i}",
                    "value": [{"key1": "value1", "key2": "value2"}],
                    "created_at": datetime(2022, 8, 1),
                },
                "user_id": None,
            },
            {
                "name": f'update: "Object {i}"',
                "document_id": updated_doc["_id"],
                "action_type": AuditActionType.UPDATE,
                "previous_values": {"value": [{"key1": "value1", "key2": "value2"}]},
                "current_values": {
                    "value": 1,
                    "updated_at": datetime(2022, 8, 1),
                },
                "user_id": None,
            },
        ]


@pytest.mark.asyncio
@freeze_time("2022-08-01")
async def test_replace_one(git_persistent, test_documents):
    """
    Test replacing one document
    """
    persistent, repo = git_persistent
    await persistent.insert_many(collection_name="data", documents=test_documents)

    before, _ = await persistent.find(collection_name="data", query_filter={})

    with patch(
        "featurebyte.persistent.git.GitDB._reset_branch", wraps=persistent._reset_branch
    ) as mock_reset:
        result = await persistent.replace_one(
            collection_name="data", query_filter={}, replacement={"name": "apple"}
        )
    assert mock_reset.call_args[1] == {"rate_limit_fetch": False}
    assert result == 1
    after, total = await persistent.find(collection_name="data", query_filter={})

    # only first document should be updated
    assert total == 3
    assert after[0]["name"] == "apple"
    assert after[1] == before[1]
    assert after[2] == before[2]

    # check commit messages
    assert _get_commit_messages(repo, max_count=10) == [
        "Initial commit\n",
        (
            "Create document: data/Object 0\n"
            "Create document: data/Object 1\n"
            "Create document: data/Object 2\n"
            'Create document: __audit__data/insert: "Object 0"\n'
            'Create document: __audit__data/insert: "Object 1"\n'
            'Create document: __audit__data/insert: "Object 2"\n'
        ),
        (
            "Rename document: data/Object 0 -> data/apple\n"
            "Update document: data/apple\n"
            'Create document: __audit__data/replace: "Object 0"\n'
        ),
    ]

    # check audit record is inserted
    updated_doc = test_documents[0]
    results, _ = await persistent.find(
        collection_name="__audit__data", query_filter={"document_id": updated_doc["_id"]}
    )
    assert len(results) == 2
    assert isinstance(results[0].pop("_id"), ObjectId)
    assert isinstance(results[0].pop("action_at"), datetime)
    assert isinstance(results[1].pop("_id"), ObjectId)
    assert isinstance(results[1].pop("action_at"), datetime)

    # id should remain unchanged, so it won't be captured in old values
    before[0].pop("_id")
    before[0].pop("created_at")  # due to freeze_time, this value is the same too

    assert results == [
        {
            "name": 'insert: "Object 0"',
            "document_id": updated_doc["_id"],
            "action_type": AuditActionType.INSERT,
            "previous_values": {},
            "current_values": {
                "name": f"Object 0",
                "value": [{"key1": "value1", "key2": "value2"}],
                "created_at": datetime(2022, 8, 1),
            },
            "user_id": None,
        },
        {
            "name": 'replace: "Object 0"',
            "document_id": updated_doc["_id"],
            "action_type": AuditActionType.REPLACE,
            "previous_values": before[0],
            "current_values": {
                "name": "apple",
                "updated_at": datetime(2022, 8, 1),
            },
            "user_id": None,
        },
    ]


@pytest.mark.asyncio
async def test_update_name_to_existing(git_persistent, test_documents):
    """
    Test updating one document with name that already exists
    """
    persistent, _ = git_persistent
    await persistent.insert_many(collection_name="data", documents=test_documents)
    with pytest.raises(DuplicateDocumentError) as exc:
        await persistent.update_one(
            collection_name="data", query_filter={}, update={"$set": {"name": "Object 1"}}
        )
    assert str(exc.value) == "Document data/Object 1 already exists"


@pytest.mark.parametrize(
    "update,valid",
    [
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
@pytest.mark.asyncio
async def test_update_values(git_persistent, test_document, update, valid):
    """
    Test update values validation
    """
    persistent, _ = git_persistent
    await persistent.insert_one(collection_name="data", document=test_document)

    async def run_update():
        await persistent.update_one(collection_name="data", query_filter={}, update=update)

    if valid:
        await run_update()
    else:
        with pytest.raises(NotImplementedError):
            await run_update()


@pytest.mark.asyncio
@freeze_time("2022-08-01")
async def test_delete_one(git_persistent, test_documents):
    """
    Test deleting one document
    """
    persistent, repo = git_persistent
    await persistent.insert_many(collection_name="data", documents=test_documents)
    with patch(
        "featurebyte.persistent.git.GitDB._reset_branch", wraps=persistent._reset_branch
    ) as mock_reset:
        result = await persistent.delete_one(collection_name="data", query_filter={})
    assert mock_reset.call_args[1] == {"rate_limit_fetch": False}

    # expect only one document to be deleted
    assert result == 1
    results, _ = await persistent.find(collection_name="data", query_filter={})
    assert len(results) == 2

    # check commit messages
    assert _get_commit_messages(repo, max_count=10) == [
        "Initial commit\n",
        (
            "Create document: data/Object 0\n"
            "Create document: data/Object 1\n"
            "Create document: data/Object 2\n"
            'Create document: __audit__data/insert: "Object 0"\n'
            'Create document: __audit__data/insert: "Object 1"\n'
            'Create document: __audit__data/insert: "Object 2"\n'
        ),
        ("Delete document: data/Object 0\n" 'Create document: __audit__data/delete: "Object 0"\n'),
    ]

    # check audit record is inserted
    deleted_doc = test_documents[0]
    results, _ = await persistent.find(
        collection_name="__audit__data", query_filter={"document_id": deleted_doc["_id"]}
    )
    assert len(results) == 2
    assert isinstance(results[0].pop("_id"), ObjectId)
    assert isinstance(results[0].pop("action_at"), datetime)
    assert isinstance(results[1].pop("_id"), ObjectId)
    assert isinstance(results[1].pop("action_at"), datetime)
    assert results == [
        {
            "name": 'insert: "Object 0"',
            "document_id": deleted_doc["_id"],
            "action_type": AuditActionType.INSERT,
            "previous_values": {},
            "current_values": {
                "name": f"Object 0",
                "value": [{"key1": "value1", "key2": "value2"}],
                "created_at": datetime(2022, 8, 1),
            },
            "user_id": None,
        },
        {
            "name": 'delete: "Object 0"',
            "document_id": deleted_doc["_id"],
            "action_type": AuditActionType.DELETE,
            "previous_values": test_documents[0],
            "current_values": {},
            "user_id": None,
        },
    ]


@pytest.mark.asyncio
async def test_delete_many(git_persistent, test_documents):
    """
    Test deleting many documents
    """
    persistent, repo = git_persistent
    await persistent.insert_many(collection_name="data", documents=test_documents)
    with patch(
        "featurebyte.persistent.git.GitDB._reset_branch", wraps=persistent._reset_branch
    ) as mock_reset:
        result = await persistent.delete_many(collection_name="data", query_filter={})
    assert mock_reset.call_args[1] == {"rate_limit_fetch": False}
    # expect all documents to be deleted
    assert result == 3
    results, _ = await persistent.find(collection_name="data", query_filter={})
    assert len(results) == 0

    # check commit messages
    assert _get_commit_messages(repo, max_count=15) == [
        "Initial commit\n",
        (
            "Create document: data/Object 0\n"
            "Create document: data/Object 1\n"
            "Create document: data/Object 2\n"
            'Create document: __audit__data/insert: "Object 0"\n'
            'Create document: __audit__data/insert: "Object 1"\n'
            'Create document: __audit__data/insert: "Object 2"\n'
        ),
        (
            "Delete document: data/Object 0\n"
            "Delete document: data/Object 1\n"
            "Delete document: data/Object 2\n"
            'Create document: __audit__data/delete: "Object 0"\n'
            'Create document: __audit__data/delete: "Object 1"\n'
            'Create document: __audit__data/delete: "Object 2"\n'
        ),
    ]


@pytest.mark.asyncio
async def test_delete_one__collection_not_exist(git_persistent):
    """
    Test document from non-existent collection should work with no effect
    """
    persistent, repo = git_persistent
    result = await persistent.delete_one(collection_name="no_such_collection", query_filter={})
    assert result == 0

    # check commit messages, expect no message after initial commit
    assert _get_commit_messages(repo) == ["Initial commit\n"]


@pytest.mark.asyncio
async def test_start_transaction__success(git_persistent):
    """
    Test start_transaction context manager
    """
    persistent, repo = git_persistent
    col = "test_col"

    async with persistent.start_transaction() as session:
        await session.insert_one(collection_name=col, document={"_id": "1234", "key1": "value1"})
        await session.update_one(
            collection_name=col,
            query_filter={"_id": "1234"},
            update={"$set": {"key1": "value2"}},
        )

    # check commit messages
    assert _get_commit_messages(repo) == [
        "Initial commit\n",
        (
            "Create document: test_col/1234\n"
            "Create document: __audit__test_col/insert: None\n"
            "Update document: test_col/1234\n"
            "Create document: __audit__test_col/update: None\n"
        ),
    ]
    assert repo.git.status() == "On branch test\nnothing to commit, working tree clean"


@pytest.mark.asyncio
async def test_start_transaction__exception_within_transaction(git_persistent):
    """
    Test start_transaction context manager (exception happens within context)
    """
    persistent, repo = git_persistent
    col = "test_col"

    # expect consistent behavior in 2 consecutive attempts
    for _ in range(2):
        with pytest.raises(AssertionError):
            # set up an exception happens within the context
            async with persistent.start_transaction() as session:
                await session.insert_one(
                    collection_name=col, document={"_id": "1234", "key1": "value1"}
                )
                await session.update_one(
                    collection_name=col,
                    query_filter={"_id": "1234"},
                    update={"$set": {"key1": "value2"}},
                )
                assert False

        # check commit messages & status
        assert _get_commit_messages(repo) == ["Initial commit\n"]
        assert repo.git.status() == "On branch test\nnothing to commit, working tree clean"


@pytest.mark.asyncio
async def test_get_audit_logs(git_persistent, test_document):
    """
    Test retrieving audit logs
    """
    persistent, repo = git_persistent

    # insert a doc
    inserted_id = await persistent.insert_one(collection_name="data", document=test_document)

    # update the doc a few times
    for i in range(5):
        num_updated = await persistent.update_one(
            collection_name="data", query_filter={"_id": inserted_id}, update={"$set": {"value": i}}
        )
        assert num_updated == 1

    # delete the doc
    num_deleted = await persistent.delete_one(
        collection_name="data", query_filter={"_id": inserted_id}
    )
    assert num_deleted == 1

    # check retrieve audit logs work as expected
    audit_logs, _ = await persistent.get_audit_logs(collection_name="data", document_id=inserted_id)
    assert [log["action_type"] for log in audit_logs] == ["DELETE"] + ["UPDATE"] * 5 + ["INSERT"]

    # check update logs only
    audit_logs, _ = await persistent.get_audit_logs(
        collection_name="data", document_id=inserted_id, query_filter={"action_type": "UPDATE"}
    )
    assert [log["action_type"] for log in audit_logs] == ["UPDATE"] * 5
    assert [log["previous_values"]["value"] for log in audit_logs] == [
        3,
        2,
        1,
        0,
        [{"key1": "value1", "key2": "value2"}],
    ]
