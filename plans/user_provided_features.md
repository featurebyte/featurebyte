# User-Provided Features Implementation Plan

## Problem Statement
Allow users to specify features that are provided as part of the request (in the observation table) during feature materialization. These "user-provided features" should:
1. Be definable in a Context object
2. Be first-class Feature objects that can be added to FeatureLists
3. Be usable as inputs for creating derived computed features
4. Work in both historical materialization and online serving

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
class UserProvidedColumnDefinition(FeatureByteBaseModel):
    """Definition of a user-provided column in observation/request data"""
    column_name: str
    dtype: DBVarType
    description: Optional[str] = None

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

### Phase 2: Extend RequestColumn to Support Arbitrary Columns

**Files to modify:**
- `featurebyte/api/request_column.py`
- `featurebyte/query_graph/node/request.py`

**Changes:**

```python
# featurebyte/api/request_column.py
@classmethod
def column(cls, name: str, dtype: DBVarType) -> "RequestColumn":
    """
    Get a RequestColumn representing a user-provided column in the request data.

    Parameters
    ----------
    name : str
        Name of the column in the observation table / request data
    dtype : DBVarType
        Data type of the column

    Returns
    -------
    RequestColumn
        A RequestColumn object that can be used in feature expressions

    Examples
    --------
    >>> income = fb.RequestColumn.column("annual_income", fb.DBVarType.FLOAT)
    >>> debt_to_income = total_debt_feature / income
    >>> debt_to_income.name = "debt_to_income_ratio"
    """
    # Remove the NotImplementedError restriction in create_request_column
    return cls.create_request_column(name, dtype)

# Update create_request_column to remove the restriction
@classmethod
def create_request_column(cls, column_name: str, column_dtype: DBVarType) -> RequestColumn:
    # Remove the NotImplementedError check - allow any column
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
        # Generate code for user-provided columns
        obj = ClassEnum.REQUEST_COLUMN(
            _method_name="column",
            name=ValueStr.create(self.parameters.column_name),
            dtype=self.parameters.dtype,
        )
```

---

### Phase 3: Create UserProvidedFeature API for First-Class Features

**New file:** `featurebyte/api/user_provided_feature.py`

**Files to modify:**
- `featurebyte/api/__init__.py` (export new class)
- `featurebyte/api/context.py` (add method to get features)
- `featurebyte/models/feature.py` (add flag for user-provided features)
- `featurebyte/query_graph/model/common_table.py` (add request source type)

**Key Design:**

User-provided features need a `tabular_source`. Options:
1. **Create a special "REQUEST_DATA" source type** - cleanest approach
2. Use None and special-case throughout - messy

**Recommended approach: Add a new source type**

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
    is_user_provided: bool = Field(default=False, frozen=True)

    @model_validator(mode="after")
    def _add_tile_derived_attributes(self) -> "FeatureModel":
        # Skip tile derivation for user-provided features
        if self.is_user_provided:
            return self
        # ... existing logic ...
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
        """
        # Validate column is defined in context
        col_def = self._get_user_provided_column_definition(column_name)
        if col_def is None:
            raise ValueError(f"Column '{column_name}' not defined in context user_provided_columns")

        # Create feature using RequestColumn
        request_col = RequestColumn.column(column_name, col_def.dtype)

        # Convert to Feature with special handling
        feature = Feature.from_request_column(
            request_col,
            name=feature_name or column_name,
            context_id=self.id,
        )
        return feature
```

---

### Phase 4: Feature Model and Service Updates

**Files to modify:**
- `featurebyte/models/feature.py`
- `featurebyte/service/feature.py`
- `featurebyte/service/feature_list.py`
- `featurebyte/schema/feature.py`

**Key Changes:**

1. **Add `is_user_provided` flag** to distinguish user-provided features
2. **Skip tile derivation** for user-provided features (no aggregations)
3. **Update validation** to allow empty `aggregation_ids` for user-provided features
4. **Update FeatureList cluster derivation** to handle features without complex graphs

```python
# featurebyte/models/feature.py
class FeatureModel(BaseFeatureModel):
    is_user_provided: bool = Field(default=False, frozen=True)
    context_id: Optional[PydanticObjectId] = Field(default=None)  # Link to defining context

    @model_validator(mode="after")
    def _add_tile_derived_attributes(self) -> "FeatureModel":
        if self.is_user_provided:
            # User-provided features have no tiles
            self.__dict__["aggregation_ids"] = []
            return self
        # ... existing logic ...
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

# 1. Create context with user-provided column definitions
context = fb.Context.create(
    name="loan_approval_context",
    primary_entity=["customer"],
    user_provided_columns=[
        {"column_name": "annual_income", "dtype": fb.DBVarType.FLOAT,
         "description": "Customer's annual income"},
        {"column_name": "credit_score", "dtype": fb.DBVarType.INT,
         "description": "Customer's credit score"},
    ]
)

# 2a. Get user-provided features directly from context (passthrough)
income_feature = context.get_user_provided_feature("annual_income")
income_feature.save()

# 2b. Use user-provided columns in derived feature expressions
annual_income = fb.RequestColumn.column("annual_income", fb.DBVarType.FLOAT)
total_debt = catalog.get_feature("customer_total_debt")
debt_to_income = total_debt / annual_income
debt_to_income.name = "debt_to_income_ratio"
debt_to_income.save()

# 3. Create feature list with both types
feature_list = fb.FeatureList(
    [income_feature, debt_to_income, other_computed_features...],
    name="loan_features"
)
feature_list.save()

# 4. Historical materialization
# Observation table must have: POINT_IN_TIME, customer_id, annual_income, credit_score
observation_table = fb.ObservationTable.upload(df, context_id=context.id)
historical_table = feature_list.compute_historical_feature_table(
    observation_table,
    historical_feature_table_name="loan_training_data"
)

# 5. Online serving - request must include user-provided column values
deployment = feature_list.deploy(name="loan_deployment", context_id=context.id)
deployment.enable()
# Online request: {"customer_id": "123", "annual_income": 75000, "credit_score": 720}
```

---

## Critical Files Summary

| Phase | File | Changes |
|-------|------|---------|
| 1 | `featurebyte/models/context.py` | Add `UserProvidedColumnDefinition`, `user_provided_columns` field |
| 1 | `featurebyte/schema/context.py` | Update create/update schemas |
| 1 | `featurebyte/api/context.py` | Add `user_provided_columns` param, `get_user_provided_feature()` method |
| 2 | `featurebyte/api/request_column.py` | Remove restriction, add `column()` method |
| 2 | `featurebyte/query_graph/node/request.py` | Update SDK code generation |
| 3 | `featurebyte/query_graph/model/common_table.py` | Add `REQUEST_DATA` source type |
| 3 | `featurebyte/api/feature.py` | Add `from_request_column()` class method |
| 4 | `featurebyte/models/feature.py` | Add `is_user_provided`, `context_id` fields, skip tile derivation |
| 4 | `featurebyte/service/feature.py` | Handle user-provided feature creation |
| 4 | `featurebyte/service/feature_list.py` | Handle mixed feature types in clusters |
| 5 | `featurebyte/exception.py` | Add `MissingUserProvidedColumnsError` |
| 5 | `featurebyte/query_graph/graph.py` | Add `get_user_provided_column_requirements()` |
| 5 | `featurebyte/service/historical_features_and_target.py` | Add validation for required columns |
| 6 | `featurebyte/service/online_serving.py` | Validate user-provided columns in request |

---

## Backward Compatibility

- Existing Contexts without `user_provided_columns` have empty list (default)
- Existing features using only `POINT_IN_TIME` work unchanged
- New `is_user_provided` flag defaults to `False` for existing features
- No changes to stored feature graphs - `RequestColumnNode` already supports arbitrary columns

---

## Verification Plan

1. **Unit Tests:**
   - Context creation with user_provided_columns
   - RequestColumn.column() with various dtypes
   - Feature creation from user-provided column
   - Validation error when observation table missing columns

2. **Integration Tests:**
   - End-to-end: define context → create features → materialize historical
   - Online serving with user-provided columns in request
   - Mixed feature list (computed + user-provided)

3. **Manual Testing:**
   - Run `task test-unit` after changes
   - Test with local feature store if available
