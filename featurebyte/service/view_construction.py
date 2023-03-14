"""
ViewConstructionService class
"""
from __future__ import annotations

from typing import Any, cast

from bson import ObjectId

from featurebyte import ColumnCleaningOperation, DataCleaningOperation
from featurebyte.exception import GraphInconsistencyError
from featurebyte.persistent import Persistent
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
from featurebyte.service.base_service import BaseService
from featurebyte.service.tabular_data import DataService


class ViewConstructionService(BaseService):
    """
    ViewConstructionService class is responsible for constructing view graph nodes inside a query graph.
    This service will retrieve the data from the data service and construct the view graph nodes.
    """

    def __init__(self, user: Any, persistent: Persistent, catalog_id: ObjectId):
        super().__init__(user, persistent, catalog_id)
        self.data_service = DataService(user=user, persistent=persistent, catalog_id=catalog_id)

    @staticmethod
    def _get_additional_keyword_parameters_pairs(
        graph_node: BaseGraphNode,
        input_node_names: list[str],
        view_node_name_to_data_info: dict[str, tuple[Node, list[ColumnInfo], InputNode]],
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        """
        Get additional parameters pair to construct view graph node and update metadata.

        Parameters
        ----------
        graph_node: BaseGraphNode
            Graph node
        input_node_names: list[str]
            Input node names
        view_node_name_to_data_info: dict[str, tuple[Node, List[ColumnInfo], InputNode]]
            View node name to data info mapping

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
            if event_view_graph_node_name not in view_node_name_to_data_info:
                raise GraphInconsistencyError(
                    "Event view graph node must be processed before item view graph node."
                )

            # retrieve the processed graph node and its columns info
            (
                event_view_node,
                event_view_columns_info,
                data_input_node,
            ) = view_node_name_to_data_info[event_view_graph_node_name]

            # prepare additional parameters for view construction parameters
            view_parameters["event_view_node"] = event_view_node
            view_parameters["event_view_columns_info"] = event_view_columns_info
            view_parameters["event_view_event_id_column"] = data_input_node.parameters.id_column  # type: ignore

            # prepare additional parameters for metadata update
            event_metadata = cast(ViewMetadata, event_view_node.parameters.metadata)  # type: ignore
            metadata_parameters["event_drop_column_names"] = event_metadata.drop_column_names
            metadata_parameters[
                "event_column_cleaning_operations"
            ] = event_metadata.column_cleaning_operations

        return view_parameters, metadata_parameters

    @staticmethod
    def _prepare_target_columns_and_input_nodes(
        query_graph: QueryGraphModel,
        view_node_name_to_data_info: dict[str, tuple[Node, list[ColumnInfo], InputNode]],
        input_node_names: list[str],
        node_name: str,
    ) -> tuple[list[str], list[Node]]:
        target_columns = query_graph.get_target_nodes_required_column_names(
            node_name=node_name, keep_target_node_names=None
        )
        input_nodes = []
        for input_node_name in input_node_names:
            if input_node_name in view_node_name_to_data_info:
                view_node, _, _ = view_node_name_to_data_info[input_node_name]
                input_nodes.append(view_node)
            else:
                input_nodes.append(query_graph.get_node_by_name(input_node_name))
        return target_columns, input_nodes

    async def _construct_view_graph_node(
        self,
        query_graph: QueryGraphModel,
        graph_node: BaseGraphNode,
        input_node: InputNode,
        input_node_names: list[str],
        data_name_to_column_cleaning_operations: dict[str, list[ColumnCleaningOperation]],
        view_node_name_to_data_info: dict[str, tuple[Node, list[ColumnInfo], InputNode]],
        create_view_kwargs: dict[str, Any],
        metadata_kwargs: dict[str, Any],
        use_source_settings: bool,
    ) -> tuple[BaseGraphNode, list[ColumnInfo]]:
        # get the data document based on the data ID in input node
        data_id = input_node.parameters.id
        assert data_id is not None
        data_document = await self.data_service.get_document(document_id=data_id)

        # get the additional parameters for view construction and metadata update
        parameters_metadata = graph_node.parameters.metadata  # type: ignore
        assert isinstance(parameters_metadata, ViewMetadata)
        assert data_document.name is not None
        if use_source_settings:
            column_clean_ops = data_document.table_data.column_cleaning_operations
        else:
            column_clean_ops = data_name_to_column_cleaning_operations.get(
                data_document.name,
                parameters_metadata.column_cleaning_operations,
            )

        parameters_metadata = parameters_metadata.clone(
            view_mode=parameters_metadata.view_mode,
            column_cleaning_operations=column_clean_ops,
            **metadata_kwargs,
        )

        # create a temporary view graph node & use it to prune the view graph node parameters
        temp_view_graph_node, _ = data_document.create_view_graph_node(
            input_node=input_node,
            metadata=parameters_metadata,
            **create_view_kwargs,
        )

        # prune the view graph node metadata and reconstruct a new view graph node
        target_columns, input_nodes = self._prepare_target_columns_and_input_nodes(
            query_graph=query_graph,
            view_node_name_to_data_info=view_node_name_to_data_info,
            input_node_names=input_node_names,
            node_name=graph_node.name,
        )

        assert isinstance(temp_view_graph_node.parameters, BaseViewGraphNodeParameters)
        new_view_graph_node, new_view_columns_info = data_document.create_view_graph_node(
            input_node=input_node,
            metadata=temp_view_graph_node.parameters.prune_metadata(
                target_columns=target_columns, input_nodes=input_nodes
            ),
            **create_view_kwargs,
        )
        return new_view_graph_node, new_view_columns_info

    async def prepare_view_node_name_to_replacement_node(
        self,
        query_graph: QueryGraphModel,
        data_cleaning_operations: list[DataCleaningOperation],
        use_source_settings: bool,
    ) -> dict[str, Node]:
        """
        Prepare view graph node name to replacement node mapping by using the provided data cleaning and
        the view graph node metadata.

        Parameters
        ----------
        query_graph: QueryGraphModel
            Feature model
        data_cleaning_operations: Optional[List[DataCleaningOperation]]
            Data cleaning operations
        use_source_settings: bool
            Whether to use source data's data cleaning operations

        Returns
        -------
        dict[str, Node]

        Raises
        ------
        GraphInconsistencyError
            If the graph has unexpected structure
        """
        node_name_to_replacement_node: dict[str, Node] = {}
        data_name_to_column_cleaning_operations: dict[str, list[ColumnCleaningOperation]] = {
            data_clean_op.data_name: data_clean_op.column_cleaning_operations
            for data_clean_op in data_cleaning_operations
        }
        # view node name to (view graph node, view columns_info & data input node)
        view_node_name_to_data_info: dict[str, tuple[Node, list[ColumnInfo], InputNode]] = {}

        # since the graph node is topologically sorted, input to the view graph node will always be
        # processed before the view graph node itself
        for graph_node in query_graph.iterate_sorted_graph_nodes(
            graph_node_types=GraphNodeType.view_graph_node_types()
        ):
            # the first graph node of view graph node is always the data input node
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
                view_node_name_to_data_info=view_node_name_to_data_info,
            )

            # create a new view graph node
            new_view_graph_node, new_view_columns_info = await self._construct_view_graph_node(
                query_graph=query_graph,
                graph_node=graph_node,
                input_node=view_graph_input_node,
                input_node_names=input_node_names,
                data_name_to_column_cleaning_operations=data_name_to_column_cleaning_operations,
                view_node_name_to_data_info=view_node_name_to_data_info,
                create_view_kwargs=create_view_kwargs,
                metadata_kwargs=metadata_kwargs,
                use_source_settings=use_source_settings,
            )

            # store the new view graph node, view columns info and data input node
            view_node_name_to_data_info[graph_node.name] = (
                new_view_graph_node,
                new_view_columns_info,
                view_graph_input_node,
            )

            # check whether there is any changes on the new view graph node
            if new_view_graph_node != graph_node:
                node_name_to_replacement_node[graph_node.name] = new_view_graph_node

        return node_name_to_replacement_node

    async def construct_graph(
        self, query_graph: QueryGraphModel, data_cleaning_operations: list[DataCleaningOperation]
    ) -> GraphNodeNameMap:
        """
        Constructs the query graph by reconstructing the view graph nodes in the query graph.
        During the reconstruction, the view graph node will remove unused column cleaning operations
        and update the metadata of the view graph node.

        Parameters
        ----------
        query_graph: QueryGraphModel
            Query graph model
        data_cleaning_operations: list[DataCleaningOperation]
            List of data cleaning operations to be applied to the data used by the query graph

        Returns
        -------
        GraphNodeNameMap
        """
        node_name_to_replacement_node = await self.prepare_view_node_name_to_replacement_node(
            query_graph=query_graph,
            data_cleaning_operations=data_cleaning_operations,
            use_source_settings=False,
        )
        updated_query_graph, node_name_map = QueryGraph(**query_graph.dict()).reconstruct(
            node_name_to_replacement_node=node_name_to_replacement_node,
            regenerate_groupby_hash=True,
        )
        return updated_query_graph, node_name_map
