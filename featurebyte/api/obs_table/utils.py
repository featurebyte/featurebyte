"""
Observation table utils
"""
from typing import Any, Dict, List, Optional, Tuple

from featurebyte.query_graph.model.graph import QueryGraphModel
from featurebyte.query_graph.node import Node
from featurebyte.query_graph.node.metadata.sdk_code import ExpressionStr
from featurebyte.query_graph.transform.sdk_code import SDKCodeExtractor


def get_definition_for_obs_table_creation_from_view(
    graph: QueryGraphModel,
    node: Node,
    name: str,
    sample_rows: Optional[int] = None,
    columns: Optional[List[str]] = None,
    columns_rename_mapping: Optional[Dict[str, str]] = None,
    context_name: Optional[str] = None,
    skip_entity_validation_checks: Optional[bool] = False,
    primary_entities: Optional[List[str]] = None,
) -> str:
    """
    Helper method to get the definition for creating an observation table from a view.

    Parameters
    ----------
    graph: QueryGraphModel
        The query graph
    node: Node
        The node that represents the view
    name: str
        The name of the observation table
    sample_rows: Optional[int]
        The number of rows to sample from the view
    columns: Optional[List[str]]
        The columns to include in the observation table
    columns_rename_mapping: Optional[Dict[str, str]]
        The columns to rename in the observation table
    context_name: Optional[str]
        The name of the context to associate the observation table with
    skip_entity_validation_checks: Optional[bool]
        Whether to skip entity validation checks
    primary_entities: Optional[List[str]]
        The primary entities to associate the observation table with

    Returns
    -------
    str
    """

    def last_statement_callback(output_var: Any, var_name: Any) -> List[Tuple[Any, ExpressionStr]]:
        param_rows = [f'name="{name}"']
        if sample_rows is not None:
            param_rows.append(f"sample_rows={sample_rows}")
        if columns is not None:
            param_rows.append(f"columns={columns}")
        if columns_rename_mapping is not None:
            param_rows.append(f"columns_rename_mapping={columns_rename_mapping}")
        if context_name is not None:
            param_rows.append(f'context_name="{context_name}"')
        if skip_entity_validation_checks:
            param_rows.append(f"skip_entity_validation_checks={skip_entity_validation_checks}")
        if primary_entities is not None:
            param_rows.append(f"primary_entities={primary_entities}")
        params = ",\n\t".join(param_rows)
        return [
            (
                output_var,
                ExpressionStr(
                    f"""{var_name}.create_observation_table(
                        {params}
                    )
                    """
                ),
            )
        ]

    state = SDKCodeExtractor(graph=graph).extract(
        node=node, to_use_saved_data=True, last_statement_callback=last_statement_callback
    )
    return state.code_generator.generate(to_format=True)
