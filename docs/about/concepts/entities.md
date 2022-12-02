Entities represent an important concept in FeatureByte as they are used to define, serve and organize features.

An Entity is an object of interest that is uniquely identifiable. It exists either physically or logically.

Examples of physical entities are a customer, a house or a car. Examples of logical entities are a merchant, an account, a credit card, or a business event such as a transaction or order.

### Entity tagging
To establish the connection of an entity with the data, users tag columns representing the entity in the registered data sources. While columns tagged for a given entity may have different names (for example: ‘custID’, ‘customerID’), an entity has one unique serving name used during the feature requests.

Even if no feature is associated with an entity, its tagging is encouraged because it can help recommend joins and features. In this case, one of the columns tagged for the entity is typically a primary (or natural) key of a data source.

### Entities Relationship
FeatureByte automatically establishes a Child-parent Relationship between Entities.

Child-parent Relationships are used to simplify feature serving, to recommend features from parent entities for a use case, and to suggest similarity features that compares the child and parent entities.

The entity is automatically set as the child of other entities when its primary key (or natural key) refers to a table in which columns are tagged as belonging to other entities.

Users can also establish Subtype-supertype Relationships. An entity subtype inherits attributes and relationships of the supertype.

### Entities of a Feature
The entity of the feature defined by an aggregate is the entity tagged to the aggregate’s GroupBy key. When more than one key is used in GroupBy, a tuple of entities is associated with a feature.

When a Feature is defined via a column of a table or a view, the Feature’s Entity is the table’s primary key (or natural key).

When a Feature is derived from multiple Features, the entity of the feature is the lowest-level child entity.

### Event Entities and Feature Leakage
Entities related to business events such as complaints or transactions are identified in FeatureByte as an "Event Entity".

For use cases related to an Event Entity, features are served using windows that exclude the event of the request.

For example, in a transaction fraud detection, FeatureByte’s windowed aggregation implementation ensures the feature windows exclude the current transaction and avoids leaks when comparing the current transaction to previous transactions.

### Entities to Serve a Feature
Serving a feature can be done either with the serving name and instances of the feature entity or one of its children.

When the entity is an Event entity, sometimes at inference time some of the information may not yet be recorded in the Data Warehouse. This can occur because a Data Warehouse is not updated in real-time. In this case, the user has to provide the missing information as part of the feature materialization request.
