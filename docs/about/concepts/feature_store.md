An online Feature Store and an offline Feature Store are automatically managed to reduce the latency of the feature serving at both training and inference time.

FeatureByte's Feature Stores reside in the data warehouse of the data sources. There is no bulk outbound data transfer which minimizes security risks.

The orchestration of the feature materialization in the stores is automatically triggered once a feature is deployed based on the Feature Job setting of the feature.

The computation of features is performed in the data warehouse, leveraging the scalability, stability and efficiency they provide.

### Resource Optimization
To reduce resource usage needed to serve historical and online requests, FeatureByte stores partial aggregations in the form of tiles. Thanks to this, computation is made on incremental tiles and not the entire time window.

FeatureByte manages two types of tiles: offline tiles and online tiles.

If a feature is not deployed yet, offline tiles are cached after a historical feature request to reduce the latency of subsequent requests. Once a feature is deployed, offline tiles are computed and stored at the same schedule as the online tiles.

### Storage Optimization
The tiling approach adopted by FeatureByte significantly reduces storage compared to storing offline features, because:

* tiles are more sparse than features and
* tiles can be shared by features using the same input columns and aggregation functions but several time windows or post aggregations transforms.

### Offline / Online Consistency
Because most recent tiles are more exposed to the risk of incomplete data, older online tiles are recomputed and inconsistencies are automatically fixed. Offline tiles are computed only when the risk of incomplete data is considered as negligible.

### Feature Job Setting
To mitigate the risk that online requests are served with incomplete data (due to the underlying data latency), the feature computation excludes the most recent data. The time between the feature computation and the latest event timestamp considered is called a Blind Spot in FeatureByte.

Information of both the feature job scheduling and the Blind Spot is stored in a Feature Job Setting metadata that is attached to every feature.

### Default Feature Job Setting
FeatureByte automatically analyzes the records creation of event data sources and recommends a default setting for the Feature Job scheduling and the Blind Spot at the source level, abstracting the complexity of setting Feature Jobs.

A different Feature Job Setting can be set during the feature declaration if users desire a more conversative Blind Spot parameter or a less frequent Feature Job schedule.
