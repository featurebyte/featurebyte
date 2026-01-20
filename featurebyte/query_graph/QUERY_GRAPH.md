# Query Graph System

This document provides a comprehensive guide to the FeatureByte Query Graph system - the core engine that powers declarative feature engineering.

## Overview

### What is the Query Graph?

The Query Graph is a **directed acyclic graph (DAG)** that represents feature definitions as a series of transformations. Instead of writing SQL directly, users define features through Python SDK operations, which build a graph of nodes representing operations (aggregations, joins, filters, etc.).

### Why It Exists

1. **Declarative Feature Engineering** - Users describe *what* they want, not *how* to compute it
2. **Multi-Warehouse Support** - Single graph definition → SQL for Snowflake, Spark, Databricks, BigQuery
3. **Optimization** - Graph can be pruned, merged, and optimized before SQL generation
4. **Reproducibility** - Immutable graphs ensure feature definitions are versioned and reproducible
5. **Lineage Tracking** - Track data flow from source tables through transformations to final features

### Core Principles

- **Immutability** - Operations create new nodes/graphs, never modify existing ones
- **Lazy Evaluation** - Graph is built but not executed; optimization happens at execution time
- **Type Safety** - Strong typing throughout with Pydantic models
- **Platform Agnostic** - Graph representation is independent of target data warehouse

---

## Directory Structure

```
featurebyte/query_graph/
├── graph.py                 # Main QueryGraph and GlobalQueryGraph classes
├── enum.py                  # NodeType, NodeOutputType, GraphNodeType enums
├── algorithm.py             # Graph traversal (DFS, topological sort)
├── util.py                  # Hashing, identifiers, lineage utilities
├── pruning_util.py          # Graph pruning utilities
├── ttl_handling_util.py     # TTL (time-to-live) handling
│
├── node/                    # Node type definitions (~28 files)
│   ├── base.py              # BaseNode, BaseSeriesOutputNode base classes
│   ├── generic.py           # Core SQL ops: PROJECT, FILTER, JOIN, GROUPBY
│   ├── input.py             # InputNode for table access
│   ├── nested.py            # Nested graph nodes, ProxyInputNode
│   ├── agg_func.py          # Aggregation function definitions
│   ├── binary.py            # Binary operations (arithmetic, comparison)
│   ├── unary.py             # Unary operations (math functions)
│   ├── scalar.py            # Scalar operations (CAST, IS_NULL, CONDITIONAL)
│   ├── string.py            # String operations
│   ├── date.py              # Date/time operations
│   ├── count_dict.py        # Dictionary-based aggregations
│   ├── vector.py            # Vector operations (cosine similarity)
│   ├── distance.py          # Distance calculations (haversine)
│   ├── request.py           # Request-time columns
│   ├── function.py          # Generic/user-defined functions
│   ├── cleaning_operation.py # Data cleaning operations
│   ├── mixin.py             # Shared node behaviors
│   ├── schema.py            # Node schema
│   ├── utils.py             # Node utilities
│   ├── validator.py         # Node validation logic
│   └── metadata/            # Node metadata system
│       ├── operation.py     # OperationStructure, column tracking
│       ├── column.py        # InColumnStr, OutColumnStr types
│       ├── config.py        # Code generation config
│       ├── sdk_code.py      # SDK code generation primitives
│       └── templates/       # Code generation templates
│
├── model/                   # Data models for graph components
│   ├── graph.py             # Edge, QueryGraphModel
│   ├── column_info.py       # Column specifications
│   ├── dtype.py             # DBVarTypeInfo (data types)
│   ├── window.py            # CalendarWindow, TimeInterval
│   ├── feature_job_setting.py # Feature job scheduling
│   ├── timestamp_schema.py  # Timestamp and timezone handling
│   ├── entity_relationship_info.py # Entity dependencies
│   ├── entity_lookup_plan.py # Entity lookup planning
│   ├── critical_data_info.py # Critical data validations
│   ├── common_table.py      # Common table specifications
│   ├── time_series_table.py # Time series metadata
│   ├── node_hash_util.py    # Node hashing utilities
│   └── table.py             # Table model
│
├── transform/               # Extractors (analysis), transformers (optimization), code generators
│   ├── base.py              # BaseGraphExtractor base class
│   ├── operation_structure.py # Extract column/aggregation info
│   ├── pruning.py           # Remove unused branches & nodes
│   ├── quick_pruning.py     # Fast pruning without full analysis
│   ├── reconstruction.py    # Rebuild graph with replacements
│   ├── flattening.py        # Convert nested to flat graphs
│   ├── decompose_point.py   # Identify split points for offline store ingest graphs
│   ├── definition.py        # Extract definition hash via graph normalization
│   ├── null_filling_value.py # Extract null filling value by simulating graph with nulls
│   ├── offline_store_ingest.py # Decompose graph into nested offline store ingest nodes
│   ├── on_demand_view.py    # Feast on-demand views
│   ├── on_demand_function.py # Databricks UDF functions
│   └── sdk_code.py          # SDK code generation
│
├── graph_node/              # Nested graph node handling
│   └── base.py              # Base classes for graph nodes
│
└── sql/                     # SQL generation (separate system, 40+ files)
    ├── common.py            # Shared SQL generation logic
    ├── interpreter/         # Graph to SQL interpreter
    ├── specs.py             # SQL specifications
    └── specifications/      # SQL specifications
```

---

## Core Architecture

### 1. Graph Classes

#### QueryGraphModel (`model/graph.py`)

The base immutable graph model storing nodes and edges. The definition below is a simplified representation for readability. The actual class contains additional fields for caching and reference management (e.g., `node_name_to_ref`, `ref_to_node_name`).

```python
class QueryGraphModel(FeatureByteBaseModel):
    edges: List[Edge]                    # Directed edges (parent → child)
    nodes: List[Node]                    # All nodes in the graph

    # Non-serialized indices (rebuilt on load)
    nodes_map: Dict[str, Node]           # node_name → Node
    edges_map: Dict[str, List[str]]      # node_name → child node names
    backward_edges_map: Dict[str, List[str]]  # node_name → parent node names
```

Key methods:
- `get_node_by_name(name)` - Retrieve node by name
- `iterate_nodes(target_node, node_type)` - Iterate nodes with optional filtering
- `iterate_sorted_graph_nodes(...)` - Topologically sorted iteration

#### QueryGraph (`graph.py`)

Extends QueryGraphModel with manipulation methods:

```python
class QueryGraph(QueryGraphModel):
    # Key methods for analysis
    def get_primary_input_nodes(node, ...) -> List[InputNode]
    def get_table_ids(node, ...) -> Set[ObjectId]
    def get_entity_ids(node, ...) -> Set[ObjectId]
    def extract_operation_structure(node, ...) -> OperationStructure

    # Key methods for transformation
    def prune(target_node, ...) -> Tuple[QueryGraph, Dict]
    def flatten() -> QueryGraph
    def reconstruct(node_name_map, ...) -> QueryGraph
```

#### GlobalQueryGraph (`graph.py`)

Singleton for SDK state management:

```python
class GlobalQueryGraph(QueryGraph, metaclass=SingletonMeta):
    """
    Single global instance used during SDK operations.
    State stored in GlobalGraphState to prevent copy operations.
    """
```

The SDK builds features by adding nodes to this global graph, then extracts subgraphs for specific features.

### 2. Node System

#### Node Hierarchy

```
BaseNode (abstract)
├── BaseSeriesOutputNode
│   ├── BaseSeriesOutputWithAScalarParamNode
│   │   ├── BinaryOpWithBoolOutputNode (EQ, NE, GT, etc.)
│   │   └── BinaryArithmeticOpNode (ADD, SUB, MUL, etc.)
│   └── BaseSeriesOutputWithSingleOperandNode
├── BasePrunableNode (adds pruning support)
└── Specific node implementations
```

#### Node Structure Pattern

Every node follows this pattern:

```python
class SomeNode(BaseNode):
    class Parameters(FeatureByteBaseModel):
        # Node-specific parameters
        columns: List[InColumnStr]
        value: Optional[ScalarValue]

    type: Literal[NodeType.SOME_TYPE] = NodeType.SOME_TYPE
    parameters: Parameters
    output_type: NodeOutputType  # FRAME or SERIES

    # Required method implementations
    def _get_required_input_columns(self, input_index: int, ...) -> Sequence[str]:
        """
        Returns columns this node needs from the input at input_index.
        Called during graph pruning (GraphReconstructionTransformer) to determine
        which columns to keep, enabling removal of unnecessary column selections.
        """

    def _derive_node_operation_info(
        self, inputs: List[OperationStructure], ...
    ) -> OperationStructure:
        """
        Computes output columns, aggregations, and lineage based on input structures.
        Called by OperationStructureExtractor during backward graph traversal to build
        column lineage, track data types, and understand graph structure before SQL
        generation and pruning.
        """

    def _derive_sdk_code(
        self, node_inputs: List[VarNameExpressionInfo], ...
    ) -> Tuple[List[StatementT], VarNameExpressionInfo]:
        """
        Generates Python SDK code that reproduces this node's operation.
        Called by SDKCodeGenerator to export feature definitions as readable
        Python code (e.g., for feature.definition or Feast on-demand views).
        """
```

#### Node Categories

| Category | File | Key Node Types |
|----------|------|----------------|
| **Input** | `input.py` | `InputNode` (table access) |
| **Request** | `request.py` | `RequestColumnNode` (request-time features) |
| **SQL Core** | `generic.py` | `ProjectNode`, `FilterNode`, `JoinNode`, `GroupByNode`, `AliasNode`, `LookupNode` |
| **Aggregation** | `generic.py` | `GroupByNode`, `ItemGroupbyNode`, `NonTileWindowAggregateNode`, `ForwardAggregateNode` |
| **Binary Ops** | `binary.py` | Arithmetic (`ADD`, `SUB`, `MUL`, `DIV`), Comparison (`EQ`, `NE`, `GT`, `LT`) |
| **Unary Ops** | `unary.py` | `ABS`, `SQRT`, `LOG`, `EXP`, `FLOOR`, `CEIL`, trigonometric |
| **String** | `string.py` | `CONCAT`, `SUBSTRING`, `LENGTH`, `TRIM`, `REPLACE`, `PAD` |
| **Date/Time** | `date.py` | `DT_EXTRACT`, `DATE_DIFF`, `DATE_ADD`, `TIMEDELTA` |
| **Type/Scalar** | `scalar.py` | `CAST`, `IS_NULL`, `IS_IN`, `CONDITIONAL` |
| **Dictionary** | `count_dict.py` | Dictionary-based aggregations for categorical features |
| **Vector** | `vector.py` | `COSINE_SIMILARITY` and vector operations |
| **Nested** | `nested.py` | `BaseGraphNode`, `ProxyInputNode` for nested graphs |

### 3. Node Metadata System (`node/metadata/`)

#### OperationStructure (`operation.py`)

Central concept tracking what a node produces. Built by `OperationStructureExtractor` during
backward graph traversal, used for pruning, SQL generation, and lineage tracking.

```python
@dataclass
class OperationStructure:
    output_type: NodeOutputType
    # FRAME (table-like with multiple columns) or SERIES (single column)

    output_category: NodeOutputCategory
    # VIEW: regular view output, columns contains output columns
    # FEATURE: aggregated output, columns contains input columns, aggregations contains outputs
    # TARGET: similar to FEATURE but for target computation

    columns: List[ViewDataColumn]
    # For VIEW: the output columns available after this node
    # For FEATURE/TARGET: the input columns used to compute aggregations

    aggregations: List[FeatureDataColumn]
    # Computed aggregations (AggregationColumn or PostAggregationColumn)
    # Only populated when output_category is FEATURE or TARGET

    row_index_lineage: Tuple[str, ...]
    # Tuple of node names tracking row grouping through operations
    # Changes when rows are grouped (e.g., by GroupByNode) or joined
    # Used to validate joins and track aggregation boundaries

    is_time_based: bool
    # Whether this involves time-based aggregation (affects tile computation)
```

#### Column Types (`column.py`)

Type markers used in node parameter definitions to distinguish column references:

- **`InColumnStr`** - Marks parameters referencing columns from input nodes
  - Automatically extracted to determine required input columns (`_get_required_input_columns`)
  - Used during graph normalization to remap column names for definition hashing
  - Examples: `keys`, `value_column`, `timestamp_column` in aggregation nodes

- **`OutColumnStr`** - Marks parameters representing newly created column names
  - Examples: output column names, alias names

### 4. NodeType Enum (`enum.py`)

Defines 75+ node types organized by category:

```python
class NodeType(str, Enum):
    # Logical
    AND = "and"
    OR = "or"
    NOT = "not"

    # Relational
    EQ = "eq"
    NE = "ne"
    GT = "gt"
    # ... etc

    # SQL Operations
    PROJECT = "project"
    FILTER = "filter"
    GROUPBY = "groupby"
    JOIN = "join"
    # ... etc
```

Helper methods:
- `aggregation_and_lookup_node_types()` - Returns node types that produce aggregated results
  (GROUPBY, ITEM_GROUPBY, AGGREGATE_AS_AT, LOOKUP, etc.). Used to identify aggregation
  boundaries in the graph.
- `non_aggregation_with_timestamp_node_types()` - Returns non-aggregation nodes that work with
  timestamps (JOIN, TRACK_CHANGES, DT_EXTRACT, DATE_DIFF, DATE_ADD, INPUT). Used to handle
  timestamp_schema in node hashing for backward compatibility.

---

## Graph Transformations

### Base Classes (`transform/base.py`)

Two base classes provide graph traversal frameworks:

#### BaseGraphExtractor - Backward Traversal

**Purpose**: Extract information from the graph by traversing backward from a target node.
Use when you need to analyze dependencies, compute lineage, or gather information that
flows from outputs to inputs (e.g., "what columns does this feature need?").

Traverses the graph **backward** from a target node toward inputs (reverse topological order).
Handles caching of processed nodes and manages two state types:
- `BranchStateT`: State for the current traversal branch
- `GlobalStateT`: State shared across the entire extraction

```python
class BaseGraphExtractor(Generic[OutputT, BranchStateT, GlobalStateT]):
    def _pre_compute(self, branch_state, global_state, node, input_node_names):
        """
        Called before traversing input nodes.
        Returns: (input_names_to_traverse, skip_post_compute)
        Use to filter which inputs to traverse or skip nodes entirely.
        """

    def _in_compute(self, branch_state, global_state, node, input_node):
        """
        Called for each input node during traversal.
        Returns: updated branch_state
        Use to prepare state before recursing into an input.
        """

    def _post_compute(self, branch_state, global_state, node, inputs, skip_post):
        """
        Called after all input nodes are processed.
        Receives results from all input nodes in `inputs` list.
        Use to combine input results and compute this node's output.
        """

    def extract(self, node, **kwargs) -> OutputT:
        """Entry point - override to initialize states and call _extract()."""
```

#### BaseGraphTransformer - Forward Traversal

**Purpose**: Transform or rebuild the graph by visiting nodes in dependency order.
Use when you need to process nodes after their inputs are ready (e.g., reconstructing
a graph, decomposing into subgraphs).

Traverses the graph **forward** in topological order (inputs before outputs).

```python
class BaseGraphTransformer(Generic[OutputT, GlobalStateT]):
    def _compute(self, global_state, node):
        """Called for each node in topological order."""
```

### Key Transformers

| Transformer | File | Purpose |
|-------------|------|---------|
| **OperationStructureExtractor** | `operation_structure.py` | Extract column/aggregation lineage via backward traversal |
| **PruningTransformer** | `pruning.py` | Remove unused branches, minimize node parameters |
| **QuickGraphStructurePruningTransformer** | `quick_pruning.py` | Fast pruning without full operation structure |
| **GraphReconstructionTransformer** | `reconstruction.py` | Rebuild graph with replacement nodes |
| **GraphFlatteningTransformer** | `flattening.py` | Convert nested graphs to flat structure |
| **DecomposePointExtractor** | `decompose_point.py` | Find split points for offline store ingest graph decomposition |
| **SDKCodeGenerator** | `sdk_code.py` | Generate Python SDK code from graph |

### Pruning Process

Graph pruning optimizes queries by removing unnecessary nodes and columns. It consists of
two phases, both using backward traversal from the target node:

**Phase 1: Graph Structure Pruning** (`GraphStructurePruningExtractor`)
- Traverses backward from target node to inputs
- Removes nodes that don't contribute to the final output
- For prunable nodes (`BasePrunableNode`), uses `resolve_node_pruned()` to find replacement
- Outputs: pruned graph structure + node name mapping

**Phase 2: Node Parameters Pruning** (`NodeParametersPruningExtractor`)
- Traverses the structure-pruned graph backward
- Removes unused columns from node parameters (e.g., PROJECT nodes selecting fewer columns)
- Uses `OperationStructure` to determine which columns are actually needed
- Outputs: fully pruned graph with minimal parameters

**Example**: If a feature only uses `col_a` but the source selects `[col_a, col_b, col_c]`,
pruning will reduce the PROJECT node to only select `col_a`.

---

## Key Concepts

### 1. Immutability

Graph operations never modify existing structures. Each SDK operation adds new nodes to the
graph and returns a new reference:

```python
# Filtering creates a new view with a FilterNode added to the graph
filtered_view = event_view[event_view["amount"] > 100]  # event_view unchanged

# Column operations create new nodes, original columns unchanged
doubled = event_view["amount"] * 2  # Returns a new Series, event_view["amount"] unchanged

# Assigning a column adds nodes but creates a new view reference internally
event_view["doubled_amount"] = event_view["amount"] * 2
```

This enables:
- Version control of feature definitions
- Safe concurrent access
- Reproducible feature computation

### 2. Lazy Evaluation

Graphs are built but not executed during SDK operations:

```python
# These build the graph but don't execute SQL
feature = event_view.groupby("entity").aggregate(
    method="sum",
    value_column="amount",
    ...
)

# SQL is generated only when needed (preview, save, materialize)
feature.preview(observation_set)
```

### 3. Tile-Based Aggregation

For efficient feature serving, time-based aggregations use pre-computed "tiles":

- Time windows are broken into fixed intervals (e.g., hourly tiles)
- Tiles are stored in the data warehouse
- Feature requests combine relevant tiles

Key files:
- `featurebyte/tile/` - Tile computation logic
- `util.py` - `get_tile_table_identifier_v1/v2()` for tile naming

### 4. Lineage Tracking

Lineage is tracked in `OperationStructure` and propagated through the graph during
`OperationStructureExtractor` traversal. Three types of lineage:

| Type | Where Stored | What It Tracks | Used For |
|------|--------------|----------------|----------|
| **Row Index** | `OperationStructure.row_index_lineage` | Tuple of node names defining current row grouping | Join validation (compatible row indices), tile table identification |
| **Node/Column** | `ViewDataColumn.node_names`, `AggregationColumn.node_names` | Set of all nodes contributing to a column | Graph pruning (which nodes are needed for output) |
| **Entity** | Extracted via `get_entity_ids()` | Entity IDs through joins and lookups | Feature serving, lookup planning, primary entity identification |

**Row Index Lineage Example**:
- Starts as input table node name: `("input_1",)`
- After GroupBy on entity: `("input_1", "groupby_1")` - row grouping changed
- Used to generate tile table identifiers (same row lineage = same tile structure)

### 5. Nested Graphs

Nested graphs encapsulate related operations into a single node. Used for:
- **View-specific operations**: EventView, SCDView, DimensionView, ItemView, etc.
- **Data cleaning**: Column cleaning operations grouped together
- **Offline store ingest**: Pre-computed subgraphs for feature materialization

```python
# ProxyInputNode - placeholder in nested graph that references parent graph input
class ProxyInputNode:
    parameters.input_order: int  # Which parent input this refers to (0, 1, 2...)
    # Has no actual inputs (max_input_count = 0)
    # During operation structure extraction, looks up the actual input's structure

# BaseGraphNode - contains a complete nested QueryGraph
class BaseGraphNode:
    parameters.graph: QueryGraphModel      # The nested graph
    parameters.output_node_name: str       # Which node in nested graph is the output
    parameters.type: GraphNodeType         # CLEANING, EVENT_VIEW, SCD_VIEW, etc.
```

**How it works**:
1. Parent graph has input nodes (e.g., `InputNode` for a table)
2. `BaseGraphNode` contains a nested graph with `ProxyInputNode`(s)
3. `ProxyInputNode.input_order` maps to parent's input at that position
4. `GraphFlatteningTransformer` inlines nested graphs for SQL generation

---

## Data Flow: From SDK to SQL

### 1. Graph Construction

Each SDK operation adds nodes to the global `QueryGraph`. Example:

```python
# 1. Get table and view
event_table = EventTable.get_by_id(table_id)
event_view = event_table.get_view()
# Creates: InputNode → GraphNode (containing view-specific operations)

# 2. Select and transform columns
amount = event_view["amount"]           # Creates: ProjectNode
doubled = amount * 2                     # Creates: MulNode (binary operation)

# 3. Create aggregation feature
feature = event_view.groupby(
    by_keys=["customer_id"]
).aggregate_over(
    value_column="amount",
    method="sum",
    windows=["7d"],
    feature_names=["total_amount_7d"],
)
# Creates: GroupByNode with aggregation parameters
```

**What happens internally**:
1. SDK calls `GlobalQueryGraph.add_operation()` to create nodes
2. Edges connect input nodes to new node
3. SDK object (View, Series, Feature) holds reference to its output node
4. Graph is built incrementally as user writes SDK code

### 2. Operation Structure Extraction

Before SQL generation, analyze what each node produces:

```
OperationStructureExtractor
    │
    ▼ Backward traversal from target
    │
    ├── For each node: _derive_node_operation_info()
    │   ├── Input columns required
    │   ├── Output columns produced
    │   ├── Aggregations computed
    │   └── Data types inferred
    │
    └── Build complete lineage
```

### 3. Graph Pruning

Remove unnecessary nodes:

```
Full Graph                  Pruned Graph
───────────────────────────────────────────
A → B → C → D (target)      A → B → D (target)
    ↓
    E → F (unused)          (removed)
```

### 4. SQL Generation

Convert graph to platform-specific SQL:

```
Pruned Graph → SQL Interpreter → Platform Adapter → SQL String
                    │                   │
                    │                   ├── Snowflake
                    │                   ├── Spark
                    │                   ├── Databricks
                    │                   └── BigQuery
                    │
                    └── featurebyte/query_graph/sql/
```

---

## Developer Guide: Adding New Nodes

### Step 1: Define the Node Type

Add your new node type to the `NodeType` enum in `featurebyte/query_graph/enum.py`.

```python
class NodeType(str, Enum):
    # ... existing types
    MY_NEW_OP = "my_new_op"
```

### Step 2: Create the Node Class

Create a new Python file in `featurebyte/query_graph/node/` (e.g., `my_op.py`) or add to an existing one. Define your node class, inheriting from `BaseNode` or another suitable base class.

Your node class must:
- Implement the `max_input_count` abstract property.
- Implement the `_get_required_input_columns` abstract method.
- Implement the `_derive_node_operation_info` abstract method.
- It is also recommended to implement `_derive_sdk_code` to support SDK code generation.

```python
class MyNewOpNode(BaseNode):
    class Parameters(FeatureByteBaseModel):
        some_param: str
        columns: List[InColumnStr]

    type: Literal[NodeType.MY_NEW_OP] = NodeType.MY_NEW_OP
    parameters: Parameters
    output_type: NodeOutputType = NodeOutputType.FRAME

    @property
    def max_input_count(self) -> int:
        return 1

    def _get_required_input_columns(
        self, input_index: int, available_column_names: List[str]
    ) -> Sequence[str]:
        return self.parameters.columns

    def _derive_node_operation_info(
        self,
        inputs: List[OperationStructure],
        global_state: OperationStructureInfo,
    ) -> OperationStructure:
        # Compute output columns/aggregations based on inputs
        ...

    def _derive_sdk_code(
        self,
        node_inputs: List[NodeCodeGenOutput],
        var_name_generator: VariableNameGenerator,
        operation_structure: OperationStructure,
        config: SDKCodeGenConfig,
        context: CodeGenerationContext,
    ) -> Tuple[List[StatementT], VarNameExpressionInfo]:
        # Generate SDK code that reproduces this operation
        ...
```

### Step 3: Register in Node Union (Automatic)

**No action is needed.** The node is registered automatically.

The system uses `import_submodules` in `featurebyte/query_graph/node/__init__.py` to find all `BaseNode` subclasses in the directory and add them to the `Node` union type. As long as your new node class is in a file under the `featurebyte/query_graph/node/` directory, it will be discovered.

### Step 4: Add SQL Generation

SQL generation is handled by `SQLNode` subclasses in the `featurebyte/query_graph/sql/ast/` directory.

1.  **Create an `SQLNode` Subclass**: In a new or existing file in `featurebyte/query_graph/sql/ast/`, create a class that inherits from `SQLNode` (or a more specific subclass like `ExpressionNode`).
2.  **Link to `NodeType`**: Set the `query_node_type` class variable to your new `NodeType` (e.g., `NodeType.MY_NEW_OP`). This registers your `SQLNode` to handle your `QueryGraph` node.
3.  **Implement `build`**: Implement the `build` class method. This method receives a `SQLNodeContext` object and is responsible for constructing and returning an instance of your `SQLNode` class. Inside the `build` method, you can access the `query_node` and its `parameters` from the context.
4.  **Implement `sql` property**: Implement the `sql` property, which should return a `sqlglot` expression object representing the SQL for your node.

Example for a unary string operation in `featurebyte/query_graph/sql/ast/string.py`:
```python
@dataclass
class MyNewOpSQLNode(ExpressionNode):
    query_node_type = NodeType.MY_NEW_OP

    @property
    def sql(self) -> expressions.Expression:
        # self.expr is the input expression from the parent node
        return expressions.Anonymous(this="MY_NEW_SQL_FUNC", expressions=[self.expr.sql])

    @classmethod
    def build(cls, context: SQLNodeContext) -> "MyNewOpSQLNode":
        table_node, input_expr_node, parameters = prepare_unary_input_nodes(context)
        return cls(
            context=context,
            table_node=table_node,
            expr=input_expr_node,
        )
```

### Step 5: Add Platform-Specific Handling (if needed)

If your SQL function has different names or syntax across data warehouses, use the adapter pattern.

In your `SQLNode`'s `sql` property, call a method on the adapter:
```python
# In your SQLNode subclass
@property
def sql(self) -> Expression:
    return self.context.adapter.my_new_op_sql(self.expr.sql)
```

Then, add the abstract method to `featurebyte/query_graph/sql/adapter/base.py`:
```python
class BaseAdapter(ABC):
    @abstractmethod
    def my_new_op_sql(cls, expr: Expression) -> Expression:
        ...
```

Finally, implement the method in each platform-specific adapter (e.g., `featurebyte/query_graph/sql/adapter/snowflake.py`).

### Step 6: Write Tests

- Add unit tests for your new node class in `tests/unit/query_graph/`.
- Add integration tests that cover the SQL generation for different platforms if your node has platform-specific behavior.

---

## Key File Reference

### Core Graph

| File | Purpose |
|------|---------|
| `graph.py` | QueryGraph, GlobalQueryGraph classes |
| `model/graph.py` | QueryGraphModel, Edge |
| `enum.py` | NodeType, NodeOutputType enums |
| `algorithm.py` | DFS, topological sort |
| `util.py` | Hashing, tile identifiers |

### Nodes

| File | Key Nodes |
|------|-----------|
| `node/base.py` | BaseNode, BaseSeriesOutputNode |
| `node/generic.py` | PROJECT, FILTER, JOIN, GROUPBY, LOOKUP |
| `node/input.py` | InputNode |
| `node/nested.py` | BaseGraphNode, ProxyInputNode |
| `node/agg_func.py` | Aggregation function specs |
| `node/mixin.py` | Shared behaviors (aggregation, lookup) |

### Transformations

| File | Purpose |
|------|---------|
| `transform/base.py` | BaseGraphExtractor |
| `transform/operation_structure.py` | Column/aggregation extraction |
| `transform/pruning.py` | Graph pruning |
| `transform/flattening.py` | Nested → flat conversion |
| `transform/sdk_code.py` | SDK code generation |

### Metadata

| File | Purpose |
|------|---------|
| `node/metadata/operation.py` | OperationStructure |
| `node/metadata/column.py` | InColumnStr, OutColumnStr |
| `model/dtype.py` | DBVarTypeInfo |
| `model/column_info.py` | Column specifications |

---

## Common Patterns

### Pattern: Node Parameter Validation

```python
@field_validator("columns")
@classmethod
def validate_columns(cls, value):
    if not value:
        raise ValueError("columns cannot be empty")
    return value
```

### Pattern: Deriving Output Type from Input

```python
def _derive_node_operation_info(self, inputs, global_state):
    input_op = inputs[0]
    return OperationStructure(
        columns=input_op.columns,  # Pass through
        output_type=self.output_type,
        output_category=input_op.output_category,
        row_index_lineage=input_op.row_index_lineage,
    )
```

### Pattern: Binary Operation

```python
def _derive_node_operation_info(self, inputs, global_state):
    left, right = inputs[0], inputs[1]
    # Combine lineage from both inputs
    return OperationStructure(
        columns=[...],
        output_type=NodeOutputType.SERIES,
        row_index_lineage=left.row_index_lineage,  # Typically from left
    )
```

### Pattern: Aggregation Node

```python
def _derive_node_operation_info(self, inputs, global_state):
    # Aggregation changes row_index_lineage (groups rows)
    return OperationStructure(
        aggregations=[AggregationColumn(...)],
        output_type=NodeOutputType.FRAME,
        output_category=NodeOutputCategory.FEATURE,
        row_index_lineage=(...),  # New lineage based on groupby keys
    )
```

---

## Troubleshooting

### "Column not found" errors

Check that `_get_required_input_columns()` returns all columns the node needs from its inputs.

### Pruning removes needed nodes

Ensure node properly declares its input dependencies in `_get_required_input_columns()`.

### SDK code generation fails

Verify `_derive_sdk_code()` handles all node configurations and edge cases.

### SQL generation differs by platform

Check platform-specific SQL adapters in `featurebyte/sql/{platform}/`.
