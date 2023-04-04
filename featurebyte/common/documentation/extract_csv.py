"""
Extract documentation into a CSV file.
"""
import csv
from dataclasses import dataclass
from typing import Dict, Optional, List

from featurebyte.common.documentation.documentation_layout import get_overall_layout
from featurebyte.common.documentation.resource_extractor import ResourceDetails, get_resource_details


@dataclass
class DocItem:
    # eg. FeatureStore, FeatureStore.list
    class_method_or_attribute: str
    # link to docs, eg: http://127.0.0.1:8000/reference/featurebyte.api.feature_store.FeatureStore/
    link: str
    # the resource details for this doc item
    resource_details: ResourceDetails


class DocItems:
    """
    DocItems is a singleton class that is used to store all of the documentation items
    that are generated from the code.
    """

    def __init__(self):
        self.doc_items: Dict[str, DocItem] = {}

    def add(self, key: str, value: DocItem) -> None:
        if not key.startswith("featurebyte."):
            key = f"featurebyte.{key}"
        self.doc_items[key] = value

    def get(self, key: str) -> Optional[DocItem]:
        if key not in self.doc_items:
            return None
        return self.doc_items.get(key)

    def keys(self) -> List[str]:
        return list(self.doc_items.keys())


DOC_ITEMS = DocItems()


@dataclass
class DocItemToRender:
    menu_item: str
    # eg. FeatureStore, FeatureStore.list
    class_method_or_attribute: str
    # ilnk to docs, eg: http://127.0.0.1:8000/reference/featurebyte.api.feature_store.FeatureStore/
    link: str
    # the current docstring
    docstring_description: str

    parameters: str
    returns: str
    raises: str
    examples: str
    see_also: str


def get_resource_details_for_path(path: str) -> ResourceDetails:
    split_path = path.split(".")
    # this is for instances where the override is a class
    # eg. "api.groupby.GroupBy"
    if split_path[-1][0].isupper():
        return get_resource_details(path)

    class_description = ".".join(split_path[:-1])
    return get_resource_details(f"{class_description}::{split_path[-1]}")


def dump_to_csv():
    file_name = "debug/test.csv"
    with open(file_name, "w") as f:
        all_doc_items_to_generate = []
        for layout_item in get_overall_layout():
            doc_path_override = layout_item.get_doc_path_override()
            if doc_path_override is not None:
                # handle those with explicit overrides
                link_without_md = doc_path_override.replace(".md", "")
                resource_details = get_resource_details_for_path(link_without_md)
                all_doc_items_to_generate.append(
                    DocItemToRender(
                        menu_item=" > ".join(layout_item.menu_header),
                        class_method_or_attribute=link_without_md,
                        link=f"http://127.0.0.1:8000/reference/{link_without_md}",
                        docstring_description=resource_details.description_string,
                        parameters=resource_details.parameters_string,
                        returns=resource_details.returns_string,
                        raises=resource_details.raises_string,
                        examples=resource_details.examples_string,
                        see_also=resource_details.see_also_string,
                    )
                )
                continue

            if layout_item.get_api_path_override() in DOC_ITEMS.doc_items:
                doc_item = DOC_ITEMS.get(layout_item.get_api_path_override())
                resource_details = doc_item.resource_details
                all_doc_items_to_generate.append(
                    DocItemToRender(
                        menu_item=" > ".join(layout_item.menu_header),
                        class_method_or_attribute=doc_item.class_method_or_attribute,
                        link=doc_item.link,
                        docstring_description=resource_details.description_string,
                        parameters=resource_details.parameters_string,
                        returns=resource_details.returns_string,
                        raises=resource_details.raises_string,
                        examples=resource_details.examples_string,
                        see_also=resource_details.see_also_string,
                    )
                )
            elif layout_item.get_api_path_override().lower() in DOC_ITEMS.doc_items:
                doc_item = DOC_ITEMS.get(layout_item.get_api_path_override().lower())
                resource_details = doc_item.resource_details
                all_doc_items_to_generate.append(
                    DocItemToRender(
                        menu_item=" > ".join(layout_item.menu_header),
                        class_method_or_attribute=doc_item.class_method_or_attribute,
                        link=doc_item.link,
                        docstring_description=resource_details.description_string,
                        parameters=resource_details.parameters_string,
                        returns=resource_details.returns_string,
                        raises=resource_details.raises_string,
                        examples=resource_details.examples_string,
                        see_also=resource_details.see_also_string,
                    )
                )
            else:
                all_doc_items_to_generate.append(
                    DocItemToRender(
                        menu_item=" > ".join(layout_item.menu_header),
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
        writer = csv.writer(f)
        writer.writerow(
            [
                "menu_item",
                "class_method_or_attribute",
                "link",
                "docstring_description",
                "parameters",
                "returns",
                "raises",
                "examples",
                "see_also",
            ]
        )
        for doc_item in all_doc_items_to_generate:
            writer.writerow(
                [
                    doc_item.menu_item,
                    doc_item.class_method_or_attribute,
                    doc_item.link,
                    doc_item.docstring_description,
                    doc_item.parameters,
                    doc_item.returns,
                    doc_item.raises,
                    doc_item.examples,
                    doc_item.see_also,
                ]
            )
        print(f"done writing rows to {file_name}")
