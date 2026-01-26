# User-Provided Features Implementation Plan

## Problem Statement
Allow users to specify features that are provided as part of the request (in the observation table) during feature materialization. These "user-provided features" should:
1. Be definable in a Context object (with optional cleaning operations)
2. Be first-class Feature objects that can be added to FeatureLists
3. Be usable as inputs for creating derived computed features
4. Work in both historical materialization and online serving
5. **Only be accessible as Features through the Context** - users must use `context.get_user_provided_feature()` to ensure only predefined columns are used (no raw RequestColumn access)
6. **Store context_id** on Features and FeatureLists that use user-provided columns, enabling filtering during listing based on Context or UseCase
7. **Support column_cleaning_operations** - cleaning operations can be defined per user-provided column and are applied during materialization

## Key Design Decisions

1. **Context provides Features, not RequestColumns**: Users access user-provided columns only as Feature objects through `context.get_user_provided_feature(column_name)`. There is no `get_request_column()` method - users cannot get raw RequestColumn objects for user-provided columns.

2. **Cleaning operations support**: User-provided columns can have `column_cleaning_operations` defined, just like regular table columns. These are applied when the feature is materialized.

3. **context_id tracking**: Features and FeatureLists that use user-provided columns store a `context_id` field, enabling:
   - Filtering features/feature lists by context during listing
   - Validation that all features in a FeatureList use the same context (or no context)
   - Traceability of which context defined the user-provided columns

4. **Automatic context_id derivation for FeatureLists**: When a FeatureList is created, its `context_id` is automatically derived from its constituent features.

---

## Current State Analysis

### Existing Infrastructure
- **RequestColumn** (`featurebyte/api/request_column.py`): Exists but restricted to `POINT_IN_TIME` only via `NotImplementedError`
- **RequestColumnNode** (`featurebyte/query_graph/node/request.py`): Already supports arbitrary column names and dtypes
- **Context** (`featurebyte/models/context.py`): Defines primary entities, links to observation tables
- **Feature** (`featurebyte/api/feature.py`): Requires `graph`, `node_name`, and `tabular_source`

### Key Challenge
Features require a `tabular_source` (table details from a data warehouse). User-provided features don't come from a registered table - they come from the request/observation data. We need a way to represent this.

---

## Proposed Implementation

### Phase 1: Extend Context Model with User-Provided Column Definitions

**Files to modify:**
- `featurebyte/models/context.py`
- `featurebyte/schema/context.py`
- `featurebyte/api/context.py`

**Changes:**

```python
# featurebyte/models/context.py
from featurebyte.query_graph.node.cleaning_operation import ColumnCleaningOperation

class UserProvidedColumnDefinition(FeatureByteBaseModel):
    """Definition of a user-provided column in observation/request data"""
    column_name: str
    dtype: DBVarType
    description: Optional[str] = None
    # Cleaning operations to apply to the column during materialization
    column_cleaning_operations: List[ColumnCleaningOperation] = Field(default_factory=list)

class ContextModel(FeatureByteCatalogBaseDocumentModel):
    # ... existing fields ...
    user_provided_columns: List[UserProvidedColumnDefinition] = Field(default_factory=list)
```

```python
# featurebyte/api/context.py
@classmethod
def create(
    cls,
    name: str,
    primary_entity: List[str],
    description: Optional[str] = None,
    treatment_name: Optional[str] = None,
    user_provided_columns: Optional[List[Dict[str, Any]]] = None,  # New parameter
) -> "Context":
    ...
```

---

### Phase 2: Internal RequestColumn Support

**Key Design:** The `RequestColumn` class is modified internally to support arbitrary columns, but this is NOT exposed publicly. Users can only access user-provided columns as Feature objects through `context.get_user_provided_feature()`.

**Files to modify:**
- `featurebyte/api/request_column.py`
- `featurebyte/query_graph/node/request.py`

**Changes:**

```python
# featurebyte/api/request_column.py
# Make create_request_column internal (remove NotImplementedError restriction)
# This is used internally by Context.get_user_provided_feature()
@classmethod
def _create_request_column(cls, column_name: str, column_dtype: DBVarType) -> RequestColumn:
    """
    Internal method to create a RequestColumn.

    This is not exposed publicly - users should use Context.get_user_provided_feature() instead.
    """
    node = GlobalQueryGraph().add_operation(
        node_type=NodeType.REQUEST_COLUMN,
        node_params={"column_name": column_name, "dtype": column_dtype},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[],
    )
    return cls(
        feature_store=None,
        tabular_source=None,
        node_name=node.name,
        name=column_name,
        dtype=column_dtype,
    )
```

```python
# featurebyte/query_graph/node/request.py - Update SDK code generation
def _derive_sdk_code(self, ...):
    if self.parameters.column_name == SpecialColumnName.POINT_IN_TIME:
        obj = ClassEnum.REQUEST_COLUMN(_method_name="point_in_time")
    else:
        # Generate code showing context-based feature access for user-provided columns
        obj = ClassEnum.CONTEXT(
            _method_name="get_user_provided_feature",
            column_name=ValueStr.create(self.parameters.column_name),
        )
```

---

### Phase 3: Context ID Tracking for Features and FeatureLists

**Key Design:** Features and FeatureLists that use user-provided columns must store `context_id` to enable filtering during listing.

**Files to modify:**
- `featurebyte/models/feature.py` (add `context_id` field)
- `featurebyte/models/feature_list.py` (add `context_id` field)
- `featurebyte/api/context.py` (add method to get features)
- `featurebyte/query_graph/model/common_table.py` (add request source type)

**Changes:**

```python
# featurebyte/query_graph/model/common_table.py
class TableSourceType(str, Enum):
    """Types of table sources"""
    WAREHOUSE = "warehouse"       # Normal tables from data warehouse
    REQUEST_DATA = "request_data"  # User-provided data from observation/request

class TabularSource(FeatureByteBaseModel):
    # ... existing fields ...
    source_type: TableSourceType = Field(default=TableSourceType.WAREHOUSE)
```

```python
# featurebyte/models/feature.py
class FeatureModel(BaseFeatureModel):
    # ... existing fields ...

    # Context ID for features that use user-provided columns
    # This enables filtering features by context during listing
    context_id: Optional[PydanticObjectId] = Field(default=None)

    @model_validator(mode="after")
    def _add_tile_derived_attributes(self) -> "FeatureModel":
        # Skip tile derivation for features that only use user-provided columns
        if self._is_pure_user_provided_feature():
            self.__dict__["aggregation_ids"] = []
            return self
        # ... existing logic ...

    def _is_pure_user_provided_feature(self) -> bool:
        """Check if feature only uses user-provided columns (no warehouse data)"""
        # A feature is pure user-provided if it has no input nodes from tables
        return not self.table_ids
```

```python
# featurebyte/models/feature_list.py
class FeatureListModel(FeatureByteCatalogBaseDocumentModel):
    # ... existing fields ...

    # Context ID for feature lists that contain features using user-provided columns
    # Derived from the features in the list
    context_id: Optional[PydanticObjectId] = Field(default=None)
```

```python
# featurebyte/api/context.py
class Context:
    def get_user_provided_feature(
        self,
        column_name: str,
        feature_name: Optional[str] = None,
    ) -> Feature:
        """
        Get a Feature object for a user-provided column defined in this context.

        The returned feature will have context_id set, linking it to this context.
        Any column_cleaning_operations defined for this column will be applied
        during feature materialization by adding cleaning nodes to the query graph.

        Parameters
        ----------
        column_name : str
            Name of the user-provided column (must be defined in context)
        feature_name : Optional[str]
            Name for the feature (defaults to column_name)

        Returns
        -------
        Feature
            A Feature object representing the user-provided column

        Examples
        --------
        >>> context = fb.Context.get("loan_approval_context")
        >>> income_feature = context.get_user_provided_feature("annual_income")
        >>> income_feature.save()
        """
        col_def = self._get_user_provided_column_definition(column_name)
        if col_def is None:
            raise ValueError(f"Column '{column_name}' not defined in context user_provided_columns")

        # Create RequestColumn internally
        request_col = RequestColumn._create_request_column(column_name, col_def.dtype)

        # Apply cleaning operations by adding graph nodes (similar to how table columns work)
        # Each CleaningOperation.add_cleaning_operation() adds nodes like CONDITIONAL, CAST, etc.
        if col_def.column_cleaning_operations:
            for cleaning_op in col_def.column_cleaning_operations:
                request_col = cleaning_op.add_cleaning_operation(
                    graph_node=request_col.graph,
                    input_node=request_col.node,
                    dtype=col_def.dtype,
                )

        # Convert to Feature with context_id set
        feature = Feature.from_request_column(
            request_col,
            name=feature_name or column_name,
            context_id=self.id,
        )
        return feature
```

**Note on Cleaning Operations Implementation:**

Cleaning operations work by adding nodes to the query graph:
- `MissingValueImputation` → adds CONDITIONAL node to replace NULL with imputed_value
- `ValueBeyondEndpointImputation` → adds comparison + CONDITIONAL nodes
- `CastToNumeric` → adds TRY_CAST node

The existing `CleaningOperation.add_cleaning_operation()` method in `featurebyte/query_graph/node/cleaning_operation.py` handles this. For user-provided features, we reuse this same mechanism to build cleaning nodes on top of the RequestColumn graph node.

---

### Phase 4: Feature/FeatureList Service Updates and Listing Filters

**Files to modify:**
- `featurebyte/service/feature.py`
- `featurebyte/service/feature_list.py`
- `featurebyte/routes/feature/api.py`
- `featurebyte/routes/feature_list/api.py`
- `featurebyte/api/feature.py`
- `featurebyte/api/feature_list.py`

**Key Changes:**

1. **Propagate context_id** when saving features that use user-provided columns
2. **Derive context_id for FeatureLists** from their constituent features
3. **Add context_id filter to listing APIs** so users can filter features/feature lists by context
4. **Add use_case filter** (since UseCase links to Context, can filter transitively)

```python
# featurebyte/service/feature.py
class FeatureService:
    async def create_document(self, data: FeatureCreate) -> FeatureModel:
        # ... existing logic ...

        # If feature uses user-provided columns, ensure context_id is set
        if self._uses_user_provided_columns(data.graph, data.node_name):
            if data.context_id is None:
                raise ValueError(
                    "Features using user-provided columns must specify a context_id"
                )
        # ... save feature ...
```

```python
# featurebyte/service/feature_list.py
class FeatureListService:
    async def create_document(self, data: FeatureListCreate) -> FeatureListModel:
        # ... existing logic ...

        # Derive context_id from features
        context_ids = set()
        for feature in features:
            if feature.context_id:
                context_ids.add(feature.context_id)

        if len(context_ids) > 1:
            raise ValueError(
                "All features in a FeatureList must use the same context or no context"
            )

        data.context_id = context_ids.pop() if context_ids else None
        # ... save feature list ...
```

```python
# featurebyte/api/feature.py - Add context filter to listing
@classmethod
def list(
    cls,
    include_id: Optional[bool] = False,
    primary_entity: Optional[Union[str, List[str]]] = None,
    primary_table: Optional[Union[str, List[str]]] = None,
    context_name: Optional[str] = None,  # NEW: Filter by context
) -> pd.DataFrame:
    """
    List saved features

    Parameters
    ----------
    context_name: Optional[str]
        Name of context to filter features. Only features associated with
        this context (via user-provided columns) will be returned.
    """
    ...
```

```python
# featurebyte/api/feature_list.py - Add context filter to listing
@classmethod
def list(
    cls,
    include_id: Optional[bool] = False,
    context_name: Optional[str] = None,  # NEW: Filter by context
    use_case_name: Optional[str] = None,  # NEW: Filter by use case
) -> pd.DataFrame:
    """
    List saved feature lists

    Parameters
    ----------
    context_name: Optional[str]
        Name of context to filter feature lists.
    use_case_name: Optional[str]
        Name of use case to filter feature lists (filters by associated context).
    """
    ...
```

---

### Phase 5: Validation During Materialization

**Files to modify:**
- `featurebyte/service/historical_features_and_target.py`
- `featurebyte/query_graph/sql/feature_historical.py`
- `featurebyte/exception.py`

**Changes:**

```python
# featurebyte/exception.py
class MissingUserProvidedColumnsError(FeatureByteException):
    """Raised when required user-provided columns are missing from observation table"""

# featurebyte/query_graph/graph.py
def get_user_provided_column_requirements(
    self, node: Node
) -> List[Tuple[str, DBVarType]]:
    """
    Extract all user-provided column requirements from the graph.
    Returns list of (column_name, dtype) tuples for non-POINT_IN_TIME request columns.
    """
    request_columns = []
    for req_node in self.iterate_nodes(node, NodeType.REQUEST_COLUMN):
        if req_node.parameters.column_name != SpecialColumnName.POINT_IN_TIME:
            request_columns.append(
                (req_node.parameters.column_name, req_node.parameters.dtype)
            )
    return request_columns
```

```python
# featurebyte/service/historical_features_and_target.py
async def get_historical_features(...):
    # Extract required user-provided columns from all feature graphs
    required_columns = set()
    for feature in features:
        required_columns.update(
            feature.graph.get_user_provided_column_requirements(feature.node)
        )

    # Validate observation table has all required columns
    observation_columns = set(col.name for col in observation_set.columns_info)
    missing = [col for col, _ in required_columns if col not in observation_columns]
    if missing:
        raise MissingUserProvidedColumnsError(
            f"Observation table missing required user-provided columns: {missing}"
        )
```

---

### Phase 6: Online Serving Support

**Files to modify:**
- `featurebyte/service/online_serving.py`
- `featurebyte/service/entity_validation.py`

**Changes:**

```python
# featurebyte/service/online_serving.py
async def get_online_features_from_feature_list(
    self, feature_list, request_data, ...
):
    # Extract required user-provided columns from feature list
    required_user_columns = self._get_required_user_columns(feature_list)

    # Validate request_data has all required columns
    request_columns = set(request_data.keys())
    missing = [col for col in required_user_columns if col not in request_columns]
    if missing:
        raise MissingUserProvidedColumnsError(
            f"Request data missing required user-provided columns: {missing}"
        )

    # Include user-provided columns in SQL generation
    # ... existing logic with user_provided_columns passed through ...
```

---

### Phase 7: SQL Generation Updates

**Files to modify:**
- `featurebyte/query_graph/sql/feature_historical.py`
- `featurebyte/query_graph/sql/online_serving.py`

User-provided features are simple SELECT statements from the request/observation table. The SQL generation needs to:
1. For passthrough features: `SELECT user_col FROM request_table`
2. For derived features: existing logic handles this since the graph includes the RequestColumnNode

---

## Usage Example

```python
import featurebyte as fb

# 1. Create context with user-provided column definitions (including cleaning operations)
context = fb.Context.create(
    name="loan_approval_context",
    primary_entity=["customer"],
    user_provided_columns=[
        {
            "column_name": "annual_income",
            "dtype": fb.DBVarType.FLOAT,
            "description": "Customer's annual income",
            "column_cleaning_operations": [
                fb.MissingValueImputation(imputed_value=0.0),
                fb.ValueBeyondEndpointImputation(
                    type="less_than", end_point=0, imputed_value=0.0
                ),
            ],
        },
        {
            "column_name": "credit_score",
            "dtype": fb.DBVarType.INT,
            "description": "Customer's credit score",
        },
    ]
)

# 2. Get user-provided features from context
# These features will have context_id set automatically
# Cleaning operations are applied during materialization
income_feature = context.get_user_provided_feature("annual_income")
income_feature.save()

credit_score_feature = context.get_user_provided_feature("credit_score")
credit_score_feature.save()

# 3. Use user-provided features in derived feature expressions
total_debt = catalog.get_feature("customer_total_debt")
debt_to_income = total_debt / income_feature
debt_to_income.name = "debt_to_income_ratio"
debt_to_income.save()

# 4. Create feature list with both types
# context_id is automatically derived from constituent features
feature_list = fb.FeatureList(
    [income_feature, credit_score_feature, debt_to_income, other_computed_features],
    name="loan_features"
)
feature_list.save()

# 5. List features/feature lists filtered by context
# Only features associated with this context will be shown
context_features = fb.Feature.list(context_name="loan_approval_context")
context_feature_lists = fb.FeatureList.list(context_name="loan_approval_context")

# 6. Historical materialization
# Observation table must have: POINT_IN_TIME, customer_id, annual_income, credit_score
observation_table = fb.ObservationTable.upload(df, context_id=context.id)
historical_table = feature_list.compute_historical_feature_table(
    observation_table,
    historical_feature_table_name="loan_training_data"
)

# 7. Online serving - request must include user-provided column values
deployment = feature_list.deploy(name="loan_deployment", context_id=context.id)
deployment.enable()
# Online request: {"customer_id": "123", "annual_income": 75000, "credit_score": 720}
```

---

## Critical Files Summary

| Phase | File | Changes |
|-------|------|---------|
| 1 | `featurebyte/models/context.py` | Add `UserProvidedColumnDefinition` with `column_cleaning_operations`, `user_provided_columns` field |
| 1 | `featurebyte/schema/context.py` | Update create/update schemas |
| 1 | `featurebyte/api/context.py` | Add `user_provided_columns` param to `create()` |
| 2 | `featurebyte/api/request_column.py` | Make `_create_request_column()` internal (remove public restriction) |
| 2 | `featurebyte/query_graph/node/request.py` | Update SDK code generation for context-based feature access |
| 3 | `featurebyte/models/feature.py` | Add `context_id` field |
| 3 | `featurebyte/models/feature_list.py` | Add `context_id` field |
| 3 | `featurebyte/api/context.py` | Add `get_user_provided_feature()` method with cleaning ops support |
| 3 | `featurebyte/query_graph/model/common_table.py` | Add `REQUEST_DATA` source type |
| 4 | `featurebyte/service/feature.py` | Propagate `context_id`, validate user-provided column usage |
| 4 | `featurebyte/service/feature_list.py` | Derive `context_id` from features, validate consistency |
| 4 | `featurebyte/routes/feature/api.py` | Add `context_id` filter parameter |
| 4 | `featurebyte/routes/feature_list/api.py` | Add `context_id` and `use_case_id` filter parameters |
| 4 | `featurebyte/api/feature.py` | Add `context_name` to `list()`, add `from_request_column()` |
| 4 | `featurebyte/api/feature_list.py` | Add `context_name`, `use_case_name` to `list()` |
| 5 | `featurebyte/exception.py` | Add `MissingUserProvidedColumnsError` |
| 5 | `featurebyte/query_graph/graph.py` | Add `get_user_provided_column_requirements()` |
| 5 | `featurebyte/service/historical_features_and_target.py` | Add validation for required columns |
| 6 | `featurebyte/service/online_serving.py` | Validate user-provided columns in request |

---

## Backward Compatibility

- Existing Contexts without `user_provided_columns` have empty list (default)
- Existing features using only `POINT_IN_TIME` work unchanged
- New `context_id` field defaults to `None` for existing features and feature lists
- No changes to stored feature graphs - `RequestColumnNode` already supports arbitrary columns
- Listing APIs continue to work without context filter (returns all features/feature lists)

---

## Verification Plan

1. **Unit Tests:**
   - Context creation with `user_provided_columns` (including `column_cleaning_operations`)
   - `context.get_user_provided_feature()` returns Feature with `context_id` set
   - `context.get_user_provided_feature()` raises error for undefined columns
   - `context.get_user_provided_feature()` applies cleaning operations to the feature
   - Feature creation from user-provided column sets `context_id`
   - FeatureList derives `context_id` from constituent features
   - Validation error when features in FeatureList have different `context_id`
   - Validation error when observation table missing columns

2. **Cleaning Operations Tests:**
   - User-provided feature with `MissingValueImputation` applies correctly
   - User-provided feature with `ValueBeyondEndpointImputation` applies correctly
   - Multiple cleaning operations chain correctly

3. **Listing Filter Tests:**
   - `Feature.list(context_name=...)` filters correctly
   - `FeatureList.list(context_name=...)` filters correctly
   - `FeatureList.list(use_case_name=...)` filters by associated context

4. **Integration Tests:**
   - End-to-end: define context → get user-provided feature → create derived features → materialize historical
   - Online serving with user-provided columns in request
   - Mixed feature list (computed + user-provided from same context)
   - Cleaning operations applied during historical materialization

5. **Manual Testing:**
   - Run `task test-unit` after changes
   - Test with local feature store if available
