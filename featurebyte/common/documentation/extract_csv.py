"""
Extract documentation into a CSV file.
"""

import csv
from dataclasses import dataclass
from typing import Dict, List, Literal

from featurebyte.common.documentation.doc_types import DocItems, ResourceDetails
from featurebyte.common.documentation.documentation_layout import get_overall_layout
from featurebyte.common.documentation.gen_ref_pages_docs_builder import (
    generate_documentation_for_docs,
    get_doc_groups,
)
from featurebyte.common.documentation.resource_extractor import get_resource_details
from featurebyte.logging import get_logger

logger = get_logger(__name__)


@dataclass
class DocItemToRender:
    """
    DocItemToRender is a dataclass to capture what we're going to be writing to the CSV.

    Each field is a column in the CSV.
    """

    menu_item: str
    # eg. class, method, property
    item_type: Literal["class", "property", "method", "missing"]
    # eg. FeatureStore, FeatureStore.list
    class_method_or_attribute: str
    # link to docs, eg: http://127.0.0.1:8000/reference/featurebyte.api.feature_store.FeatureStore/
    link: str
    # the current docstring
    docstring_description: str

    parameters: str
    returns: str
    raises: str
    examples: str
    see_also: str


def get_resource_details_for_path(path: str, is_pure_method: bool) -> ResourceDetails:
    """
    Get the resource details for a given path.

    This is typically only used for documentation items that have an explicit file override.

    Parameters
    ----------
    path: str
        The path to get the resource details for
    is_pure_method: bool
        Whether the path is a pure method or not

    Returns
    -------
    ResourceDetails
        The resource details for the given path
    """
    split_path = path.split(".")
    # this is for instances where the override is a class
    # eg. "api.groupby.GroupBy"
    if split_path[-1][0].isupper():
        return get_resource_details(path)

    class_description = ".".join(split_path[:-1])
    if is_pure_method:
        return get_resource_details(f"{class_description}!!{split_path[-1]}")
    return get_resource_details(f"{class_description}::{split_path[-1]}")


def extract_details_for_rendering(resource_details: ResourceDetails) -> Dict[str, str]:
    """
    Extract the details for rendering.

    Parameters
    ----------
    resource_details: ResourceDetails
        The resource details to extract the details for

    Returns
    -------
    Dict[str, str]
        The extracted details
    """
    return {
        "docstring_description": resource_details.description_string,
        "parameters": resource_details.parameters_string,
        "returns": resource_details.returns_string,
        "raises": resource_details.raises_string,
        "examples": resource_details.examples_string,
        "see_also": resource_details.see_also_string,
    }


def _write_items_to_csv(file_name: str, all_doc_items_to_generate: List[DocItemToRender]) -> None:
    """
    Write the items to a CSV file.

    Parameters
    ----------
    file_name: str
        The file name to write to
    all_doc_items_to_generate: List[DocItemToRender]
        The items to write to the CSV
    """
    logger.info("Writing to CSV")
    with open(file_name, "w") as f:
        writer = csv.writer(f)
        writer.writerow([
            "menu_item",
            "class_method_or_attribute",
            "link",
            "docstring_description",
            "parameters",
            "returns",
            "raises",
            "examples",
            "see_also",
        ])
        for doc_item in all_doc_items_to_generate:
            writer.writerow([
                doc_item.menu_item,
                doc_item.class_method_or_attribute,
                doc_item.link,
                doc_item.docstring_description,
                doc_item.parameters,
                doc_item.returns,
                doc_item.raises,
                doc_item.examples,
                doc_item.see_also,
            ])
        logger.info(f"Success - done writing rows to {file_name}")


def _generate_items_to_render(doc_items: DocItems) -> List[DocItemToRender]:
    """
    Generate the items to render.

    Parameters
    ----------
    doc_items: DocItems
        The doc items to generate the items to render for.

    Returns
    -------
    List[DocItemToRender]
        The items to render
    """
    logger.info("Building items to generate")
    all_doc_items_to_generate: List[DocItemToRender] = []
    for layout_item in get_overall_layout():
        doc_path_override = layout_item.get_doc_path_override()
        if doc_path_override is not None:
            # handle those with explicit overrides
            link_without_md = doc_path_override.replace(".md", "")
            resource_details = get_resource_details_for_path(
                link_without_md, bool(layout_item.is_pure_method)
            )
            all_doc_items_to_generate.append(
                DocItemToRender(
                    menu_item=" > ".join(layout_item.menu_header),
                    item_type=resource_details.type,
                    class_method_or_attribute=link_without_md,
                    link=f"http://127.0.0.1:8000/reference/{link_without_md}",
                    **extract_details_for_rendering(resource_details),
                )
            )
            continue
        doc_item = doc_items.get(layout_item.get_api_path_override())
        if doc_item is not None:
            all_doc_items_to_generate.append(
                DocItemToRender(
                    menu_item=" > ".join(layout_item.menu_header),
                    item_type=doc_item.resource_details.type,
                    class_method_or_attribute=doc_item.class_method_or_attribute,
                    link=doc_item.link,
                    **extract_details_for_rendering(doc_item.resource_details),
                )
            )
            continue

        # If we don't find a doc item, add a placeholder.
        all_doc_items_to_generate.append(
            DocItemToRender(
                menu_item=" > ".join(layout_item.menu_header),
                item_type="missing",
                class_method_or_attribute="missing",
                link="missing",
                docstring_description="missing",
                parameters="missing",
                returns="missing",
                raises="missing",
                examples="missing",
                see_also="missing",
            )
        )
    return all_doc_items_to_generate


def dump_to_csv(doc_items: DocItems) -> None:
    """
    Dump the documentation to a CSV file.

    Parameters
    ----------
    doc_items: DocItems
        The documentation items to dump to a CSV file
    """
    file_name = "debug/documentation.csv"
    logger.info("############################################")
    logger.info(f"# Generating CSV - {file_name} #")
    logger.info("############################################")
    all_doc_items_to_generate = _generate_items_to_render(doc_items)
    _write_items_to_csv(file_name, all_doc_items_to_generate)


if __name__ == "__main__":
    doc_groups_to_use = get_doc_groups()
    _, doc_items = generate_documentation_for_docs(doc_groups_to_use)
    dump_to_csv(doc_items)
