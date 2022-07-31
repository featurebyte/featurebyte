"""
Audit logging for persistent operations
"""
from featurebyte.models.persistent import Document


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
