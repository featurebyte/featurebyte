"""
Audit logging for persistent operations
"""
from typing import Any, List, Optional, Tuple

from featurebyte.models.persistent import (
    AuditActionType,
    AuditDocument,
    AuditTransactionMode,
    Document,
    QueryFilter,
)


def get_doc_name(doc: Document) -> str:
    """
    Retrieve document name for audit log record names

    Parameters
    ----------
    doc: Document
        Document object

    Returns
    -------
    str
        Document name
    """
    name = doc.get("name")
    if name:
        return f'"{name}"'
    return "None"


def get_previous_values(original_doc: Document, updated_doc: Document) -> Document:
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
    Document
        Document with values in original document that has been updated
    """
    return {key: value for key, value in original_doc.items() if value != updated_doc.get(key)}


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
        async_execution: Any,
        collection_name: Optional[str] = None,
        query_filter: Optional[QueryFilter] = None,
        **kwargs: Any,
    ) -> Tuple[Any, int, List[Document]]:
        """
        Execute persistent transaction and return affected records prior to transaction

        Parameters
        ----------
        persistent: Any
            Persistent object to execute transaction
        async_execution: Any
            Async executed coroutine
        collection_name: Optional[str]
            Collection name
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

        Raises
        ------
        ValueError
            Transaction did not execute
        """
        if query_filter is None:
            # insertion of new document(s)
            return_value = await async_execution
            if mode == AuditTransactionMode.SINGLE:
                original_docs = [kwargs["document"]]
                num_updated = 1
            else:
                original_docs = kwargs["documents"]
                num_updated = len(return_value)
        else:
            # retrieve original document(s)
            if mode == AuditTransactionMode.SINGLE:
                original_doc = await persistent._find_one(  # pylint: disable=protected-access
                    collection_name=collection_name,
                    query_filter=query_filter,
                )
                if not original_doc:
                    raise ValueError("No document found")
                original_docs = [original_doc]
            else:
                original_docs, _ = await persistent._find(  # pylint: disable=protected-access
                    collection_name=collection_name,
                    query_filter=query_filter,
                )
                original_docs = list(original_docs)
                if not original_docs:
                    raise ValueError("No document found")
            return_value = num_updated = await async_execution

        return return_value, num_updated, original_docs

    async def _create_audit_docs(
        persistent: Any,
        action_type: AuditActionType,
        original_docs: List[Document],
        collection_name: Optional[str] = None,
        **kwargs: str,
    ) -> None:
        """
        Create audit documents to track transactions

        Parameters
        ----------
        persistent: Any
            Persistent object to execute transaction
        action_type: AuditActionType
            Transaction auction type
        original_docs: List[Document]
            list of affected documents prior to transaction
        collection_name: Optional[str]
            Collection name
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
            ) = await persistent._find(  # pylint: disable=protected-access
                collection_name=collection_name,
                query_filter={"_id": {"$in": [doc["_id"] for doc in original_docs]}},
            )
            assert len(original_docs) == num_updated_docs

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

            audit_docs.append(
                AuditDocument(
                    user_id=user_id,
                    name=f"{action_type.lower()}: {get_doc_name(original_doc)}",
                    document_id=original_doc["_id"],
                    action_type=action_type,
                    previous_values=get_previous_values(original_doc, updated_doc),
                ).dict(by_alias=True)
            )

        await persistent._insert_many(  # pylint: disable=protected-access
            collection_name=f"__audit__{collection_name}", documents=audit_docs
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

        async def wrapper(persistent: Any, *args: Any, **kwargs: Any) -> Any:
            """
            Wrapped function to perform audit logging of the persistent function

            Parameters
            ----------
            persistent: Any
                Persistent object from function to be wrapped
            args: Any
                Positional arguments for the function
            kwargs: Any
                Keyword arguments for the function

            Returns
            -------
            Any
                Return value from execution of the function
            """
            async with persistent.start_transaction() as session:

                try:
                    return_value, num_updated, original_docs = await _execute_transaction(
                        persistent=session,
                        async_execution=func(session, *args, **kwargs),
                        **kwargs,
                    )
                except ValueError:
                    return 0

                if num_updated:
                    await _create_audit_docs(
                        persistent=session,
                        action_type=action_type,
                        original_docs=original_docs,
                        **kwargs,
                    )

                return return_value

        return wrapper

    return inner
