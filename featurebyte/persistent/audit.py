"""
Audit logging for persistent operations
"""

from __future__ import annotations

from functools import wraps
from typing import Any, List, Optional, Tuple

import numpy as np

from featurebyte.models.persistent import (
    AuditActionType,
    AuditDocument,
    AuditTransactionMode,
    Document,
    QueryFilter,
)


def get_audit_collection_name(collection_name: str) -> str:
    """
    Get name of audit collection

    Parameters
    ----------
    collection_name: str
        Name of collection to be audited

    Returns
    -------
    str
        Audit collection name
    """
    return f"__audit__{collection_name}"


def get_audit_doc_name(
    action_type: AuditActionType, original_doc: Document, updated_doc: Document
) -> str:
    """
    Retrieve document name for audit log record names

    Parameters
    ----------
    action_type: AuditActionType
        Audit action type
    original_doc: Document
        Original document object
    updated_doc: Document
        Updated document object

    Returns
    -------
    str
        Document name
    """
    if action_type == AuditActionType.INSERT:
        doc_name = updated_doc.get("name")
    else:
        doc_name = original_doc.get("name")

    if doc_name is not None:
        doc_name = f'"{doc_name}"'
    return f"{action_type.lower()}: {doc_name}"


def get_previous_and_current_values(
    original_doc: Document, updated_doc: Document
) -> tuple[Document, Document]:
    """
    Get values in original document that has been changed in updated document

    Parameters
    ----------
    original_doc: Document
        Original document
    updated_doc: Document
        Updated document

    Returns
    -------
    tuple[Document, Document]
        Previous values documents (document with values in original document that has been updated)
        and current values documents (document with values in current document that has been updated)
    """
    previous_values = {
        key: value for key, value in original_doc.items() if value != updated_doc.get(key, np.nan)
    }
    current_values = {
        key: value for key, value in updated_doc.items() if value != original_doc.get(key, np.nan)
    }
    return previous_values, current_values


def audit_transaction(mode: AuditTransactionMode, action_type: AuditActionType) -> Any:
    """
    Decorator to create audit records for persistent transactions

    Parameters
    ----------
    mode: AuditTransactionMode
        Transaction mode
    action_type: AuditActionType
        Action type

    Returns
    -------
    Any
        Return from function
    """

    async def _execute_transaction(
        persistent: Any,
        collection_name: str,
        async_execution: Any,
        query_filter: Optional[QueryFilter] = None,
        **kwargs: Any,
    ) -> Tuple[Any, int, List[Document]]:
        """
        Execute persistent transaction and return affected records prior to transaction

        Parameters
        ----------
        persistent: Any
            Persistent object to execute transaction
        collection_name: str
            Collection name
        async_execution: Any
            Async executed coroutine
        query_filter: Optional[QueryFilter]
            Conditions to filter on
        kwargs: Any
            Other keyword arguments

        Returns
        -------
        Tuple[Any, int, List[Document]]
            Return value from transaction,
            number of records updated,
            list of affected documents prior to transaction
        """
        if action_type == AuditActionType.INSERT:
            # insertion of new document(s)
            return_value = await async_execution
            if mode == AuditTransactionMode.SINGLE:
                original_docs = [{"_id": return_value}]
                num_updated = 1
            else:
                original_docs = [{"_id": _id} for _id in return_value]
                num_updated = len(return_value)
        else:
            # retrieve original document(s)
            if mode == AuditTransactionMode.SINGLE:
                original_doc = await persistent._find_one(
                    collection_name=collection_name,
                    query_filter=query_filter,
                )
                if not original_doc:
                    original_docs = []
                else:
                    original_docs = [original_doc]
            else:
                original_docs, _ = await persistent._find(
                    collection_name=collection_name,
                    query_filter=query_filter,
                )
            return_value = num_updated = await async_execution

        return return_value, num_updated, original_docs

    async def _create_audit_docs(
        persistent: Any,
        collection_name: str,
        action_type: AuditActionType,
        original_docs: List[Document],
        **kwargs: str,
    ) -> None:
        """
        Create audit documents to track transactions

        Parameters
        ----------
        persistent: Any
            Persistent object to execute transaction
        collection_name: str
            Collection name
        action_type: AuditActionType
            Transaction auction type
        original_docs: List[Document]
            list of affected documents prior to transaction
        kwargs: Any
            Other keyword arguments
        """

        # retrieve updated document(s)
        if action_type == AuditActionType.DELETE:
            updated_docs = original_docs
        else:
            (
                updated_docs,
                num_updated_docs,
            ) = await persistent._find(
                collection_name=collection_name,
                query_filter={"_id": {"$in": [doc["_id"] for doc in original_docs]}},
            )
            assert len(original_docs) == num_updated_docs

            # for insertion, original docs is empty, so we set it to updated_docs
            # so that previous_values will be empty
            if action_type == AuditActionType.INSERT:
                original_docs = [{"_id": doc.get("_id")} for doc in updated_docs]

        # create audit docs to track changes
        audit_docs = []
        user_id = kwargs.get("user_id")
        for original_doc, updated_doc in zip(
            sorted(original_docs, key=lambda item: str(item["_id"])),
            sorted(updated_docs, key=lambda item: str(item["_id"])),
        ):
            if action_type != AuditActionType.DELETE:
                assert updated_doc["_id"] == original_doc["_id"]
            else:
                updated_doc = {}

            previous_values, current_values = get_previous_and_current_values(
                original_doc, updated_doc
            )
            audit_docs.append(
                AuditDocument(
                    user_id=user_id,
                    name=get_audit_doc_name(action_type, original_doc, updated_doc),
                    document_id=original_doc["_id"],
                    action_type=action_type,
                    previous_values=previous_values,
                    current_values=current_values,
                ).model_dump(by_alias=True)
            )

        await persistent._insert_many(
            collection_name=get_audit_collection_name(collection_name), documents=audit_docs
        )

    def inner(func: Any) -> Any:
        """
        Inner decorator wrapper

        Parameters
        ----------
        func: Any
            Function to wrap

        Returns
        -------
        Any
            Wrapped function
        """

        @wraps(func)
        async def wrapper(persistent: Any, collection_name: str, *args: Any, **kwargs: Any) -> Any:
            """
            Wrapped function to perform audit logging of the persistent function

            Parameters
            ----------
            persistent: Any
                Persistent object from function to be wrapped
            collection_name: str
                Collection name to use
            args: Any
                Positional arguments for the function
            kwargs: Any
                Keyword arguments for the function

            Returns
            -------
            Any
                Return value from execution of the function
            """
            if kwargs.pop("disable_audit", False):
                return await func(persistent, collection_name=collection_name, *args, **kwargs)

            async with persistent.start_transaction() as session:
                return_value, num_updated, original_docs = await _execute_transaction(
                    persistent=session,
                    collection_name=collection_name,
                    async_execution=func(session, collection_name=collection_name, *args, **kwargs),
                    **kwargs,
                )

                if num_updated and original_docs:
                    await _create_audit_docs(
                        persistent=session,
                        collection_name=collection_name,
                        action_type=action_type,
                        original_docs=original_docs,
                        **kwargs,
                    )

                return return_value

        return wrapper

    return inner
