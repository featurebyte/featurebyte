Although a Feature List can be seen as a simple list of features, it is a key element in a Machine Learning solution. Building a feature list usually demands time and domain knowledge. Ideally, a Feature List should help achieve the best accuracy, allow interpretability, be operationally efficient and contain features that don’t present any major training/serving inconsistency risk.

In FeatureByte, users can easily create new FeatureLists, share them with others or reuse existing FeatureLists.

### Feature List Main Entities
A feature list can be composed of features extracted for multiple entities, which may make its serving more complex. In FeatureByte, the serving of a feature list is simplified by the identification of its main entities.

The main entities of a feature list are automatically defined thanks to the entity relationship. Every entity of the feature list that has a child entity in the list, is represented by its child.

This usually results in a single main entity for most use cases.

### Serving Names
To support the fact that users may need to change the name of columns (“serving names”) of the data when a feature list is served, the original name of the features can be mapped to new serving names. By default, the serving names are equal to the name of the features.
