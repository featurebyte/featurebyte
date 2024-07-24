"""
ViewConstructionService class
"""

from __future__ import annotations

from typing import Any, cast

from bson import ObjectId

from featurebyte import ColumnCleaningOperation, TableCleaningOperation
from featurebyte.exception import GraphInconsistencyError
from featurebyte.query_graph.enum import GraphNodeType
from featurebyte.query_graph.graph import QueryGraph
from featurebyte.query_graph.model.column_info import ColumnInfo
from featurebyte.query_graph.model.graph import GraphNodeNameMap, QueryGraphModel
from featurebyte.query_graph.node import Node
from featurebyte.query_graph.node.input import InputNode
from featurebyte.query_graph.node.nested import (
    BaseGraphNode,
    BaseViewGraphNodeParameters,
    ViewMetadata,
)
from featurebyte.service.table import TableService


class ViewConstructionService:
    """
    ViewConstructionService class is responsible for constructing view graph nodes inside a query graph.
    This service will retrieve the table from the table service and construct the view graph nodes.
    """

    def __init__(self, table_service: TableService):
        self.table_service = table_service

    @staticmethod
    def _get_additional_keyword_parameters_pairs(
        graph_node: BaseGraphNode,
        input_node_names: list[str],
        view_node_name_to_table_info: dict[str, tuple[Node, list[ColumnInfo], InputNode]],
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        """
        Get additional parameters pair to construct view graph node and update metadata.

        Parameters
        ----------
        graph_node: BaseGraphNode
            Graph node
        input_node_names: list[str]
            Input node names
        view_node_name_to_table_info: dict[str, tuple[Node, List[ColumnInfo], InputNode]]
            View node name to table info mapping

        Returns
        -------
        tuple[dict[str, Any], dict[str, Any]]

        Raises
        ------
        GraphInconsistencyError
            If the graph has unexpected structure
        """
        view_parameters: dict[str, Any] = {}
        metadata_parameters: dict[str, Any] = {}
        if graph_node.parameters.type == GraphNodeType.ITEM_VIEW:
            # prepare the event view graph node and its columns info for item view graph node to use
            # handle the following assumption so that it won't throw internal server error
            if len(input_node_names) != 2:
                raise GraphInconsistencyError("Item view graph node must have 2 input nodes.")

            event_view_graph_node_name = input_node_names[1]
            if event_view_graph_node_name not in view_node_name_to_table_info:
                raise GraphInconsistencyError(
                    "Event view graph node must be processed before item view graph node."
                )

            # retrieve the processed graph node and its columns info
            (
                event_view_node,
                event_view_columns_info,
                table_input_node,
            ) = view_node_name_to_table_info[event_view_graph_node_name]

            # prepare additional parameters for view construction parameters
            view_parameters["event_view_node"] = event_view_node
            view_parameters["event_view_columns_info"] = event_view_columns_info
            view_parameters["event_view_event_id_column"] = table_input_node.parameters.id_column  # type: ignore

            # prepare additional parameters for metadata update
            event_metadata = cast(ViewMetadata, event_view_node.parameters.metadata)  # type: ignore
            metadata_parameters["event_drop_column_names"] = event_metadata.drop_column_names
            metadata_parameters["event_column_cleaning_operations"] = (
                event_metadata.column_cleaning_operations
            )

        return view_parameters, metadata_parameters

    @staticmethod
    def _prepare_target_columns_and_input_nodes(
        query_graph: QueryGraphModel,
        view_node_name_to_table_info: dict[str, tuple[Node, list[ColumnInfo], InputNode]],
        input_node_names: list[str],
        table_id_to_source_column_names: dict[ObjectId, set[str]],
    ) -> tuple[list[str], list[Node]]:
        input_nodes = []
        for input_node_name in input_node_names:
            if input_node_name in view_node_name_to_table_info:
                view_node, _, _ = view_node_name_to_table_info[input_node_name]
                input_nodes.append(view_node)
            else:
                input_nodes.append(query_graph.get_node_by_name(input_node_name))

        # the first input node to the view graph node is the source table input node
        source_table_input_node = input_nodes[0]
        assert isinstance(source_table_input_node, InputNode)
        target_columns = table_id_to_source_column_names[source_table_input_node.parameters.id]  # type: ignore
        return list(target_columns), input_nodes

    async def _construct_view_graph_node(
        self,
        graph_node: BaseGraphNode,
        input_table_node: InputNode,
        graph_input_nodes: list[Node],
        target_columns: list[str],
        table_name_to_column_cleaning_operations: dict[str, list[ColumnCleaningOperation]],
        create_view_kwargs: dict[str, Any],
        metadata_kwargs: dict[str, Any],
        use_source_settings: bool,
    ) -> tuple[BaseGraphNode, list[ColumnInfo]]:
        # get the table document based on the table ID in input node
        table_id = input_table_node.parameters.id
        assert table_id is not None
        table_document = await self.table_service.get_document(document_id=table_id)

        # get the additional parameters for view construction and metadata update
        parameters_metadata = graph_node.parameters.metadata  # type: ignore
        assert isinstance(parameters_metadata, ViewMetadata)
        assert table_document.name is not None
        if use_source_settings:
            column_clean_ops = table_document.table_data.column_cleaning_operations
        else:
            column_clean_ops = table_name_to_column_cleaning_operations.get(
                table_document.name,
                parameters_metadata.column_cleaning_operations,
            )

        parameters_metadata = parameters_metadata.clone(
            view_mode=parameters_metadata.view_mode,
            column_cleaning_operations=column_clean_ops,
            **metadata_kwargs,
        )

        # create a temporary view graph node & use it to prune the view graph node parameters
        temp_view_graph_node, _ = table_document.create_view_graph_node(
            input_node=input_table_node,
            metadata=parameters_metadata,
            **create_view_kwargs,
        )

        assert isinstance(temp_view_graph_node.parameters, BaseViewGraphNodeParameters)
        new_view_graph_node, new_view_columns_info = table_document.create_view_graph_node(
            input_node=input_table_node,
            metadata=temp_view_graph_node.parameters.prune_metadata(
                target_columns=target_columns, input_nodes=graph_input_nodes
            ),
            **create_view_kwargs,
        )
        return new_view_graph_node, new_view_columns_info

    async def prepare_view_node_name_to_replacement_node(
        self,
        query_graph: QueryGraphModel,
        target_node: Node,
        table_cleaning_operations: list[TableCleaningOperation],
        use_source_settings: bool,
    ) -> dict[str, Node]:
        """
        Prepare view graph node name to replacement node mapping by using the provided table cleaning and
        the view graph node metadata.

        Parameters
        ----------
        query_graph: QueryGraphModel
            Feature model graph
        target_node: Node
            Target node of the feature model graph
        table_cleaning_operations: List[TableCleaningOperation]
            Table cleaning operations
        use_source_settings: bool
            Whether to use source table's table cleaning operations

        Returns
        -------
        dict[str, Node]

        Raises
        ------
        GraphInconsistencyError
            If the graph has unexpected structure
        """
        query_graph = QueryGraph(**query_graph.model_dump(by_alias=True))
        node_name_to_replacement_node: dict[str, Node] = {}
        table_name_to_column_cleaning_operations: dict[str, list[ColumnCleaningOperation]] = {
            table_clean_op.table_name: table_clean_op.column_cleaning_operations
            for table_clean_op in table_cleaning_operations
        }
        # view node name to (view graph node, view columns_info & table input node)
        view_node_name_to_table_info: dict[str, tuple[Node, list[ColumnInfo], InputNode]] = {}

        # prepare table ID to required source column names mapping
        table_id_to_source_column_names = query_graph.extract_table_id_to_table_column_names(
            node=target_node
        )

        # since the graph node is topologically sorted, input to the view graph node will always be
        # processed before the view graph node itself
        for graph_node in query_graph.iterate_sorted_graph_nodes(
            graph_node_types=GraphNodeType.view_graph_node_types()
        ):
            # the first graph node of view graph node is always the table input node
            input_node_names = query_graph.get_input_node_names(node=graph_node)
            view_graph_input_node = query_graph.get_node_by_name(node_name=input_node_names[0])

            if not isinstance(view_graph_input_node, InputNode):
                raise GraphInconsistencyError(
                    "First input node of view graph node is not an input node."
                )

            # additional parameters used to construct view graph node
            (create_view_kwargs, metadata_kwargs) = self._get_additional_keyword_parameters_pairs(
                graph_node=graph_node,
                input_node_names=input_node_names,
                view_node_name_to_table_info=view_node_name_to_table_info,
            )

            # prepare required output columns & input nodes for the view graph node
            target_columns, input_nodes = self._prepare_target_columns_and_input_nodes(
                query_graph=query_graph,
                view_node_name_to_table_info=view_node_name_to_table_info,
                input_node_names=input_node_names,
                table_id_to_source_column_names=table_id_to_source_column_names,
            )

            # create a new view graph node
            new_view_graph_node, new_view_columns_info = await self._construct_view_graph_node(
                graph_node=graph_node,
                input_table_node=view_graph_input_node,
                graph_input_nodes=input_nodes,
                target_columns=target_columns,
                table_name_to_column_cleaning_operations=table_name_to_column_cleaning_operations,
                create_view_kwargs=create_view_kwargs,
                metadata_kwargs=metadata_kwargs,
                use_source_settings=use_source_settings,
            )

            # store the new view graph node, view columns info and table input node
            view_node_name_to_table_info[graph_node.name] = (
                new_view_graph_node,
                new_view_columns_info,
                view_graph_input_node,
            )

            # check whether there is any changes on the new view graph node
            if new_view_graph_node != graph_node:
                node_name_to_replacement_node[graph_node.name] = new_view_graph_node

        return node_name_to_replacement_node

    async def construct_graph(
        self,
        query_graph: QueryGraphModel,
        target_node: Node,
        table_cleaning_operations: list[TableCleaningOperation],
    ) -> GraphNodeNameMap:
        """
        Constructs the query graph by reconstructing the view graph nodes in the query graph.
        During the reconstruction, the view graph node will remove unused column cleaning operations
        and update the metadata of the view graph node.

        Parameters
        ----------
        query_graph: QueryGraphModel
            Query graph model
        target_node: Node
            Target node of the query graph
        table_cleaning_operations: list[TableCleaningOperation]
            List of table cleaning operations to be applied to the table used by the query graph

        Returns
        -------
        GraphNodeNameMap
        """
        node_name_to_replacement_node = await self.prepare_view_node_name_to_replacement_node(
            query_graph=query_graph,
            target_node=target_node,
            table_cleaning_operations=table_cleaning_operations,
            use_source_settings=False,
        )
        updated_query_graph, node_name_map = QueryGraph(**query_graph.model_dump()).reconstruct(
            node_name_to_replacement_node=node_name_to_replacement_node,
            regenerate_groupby_hash=True,
        )
        return updated_query_graph, node_name_map
