A feature list has to be deployed before online requests are supported.

A feature list is deployed without creating separate pipelines or using different tools. The deployment complexity is abstracted away from users and the orchestration of the feature materialization into the online feature store is automatically triggered. With no effort, low latency feature serving is available for model inference via a REST API service.

### Online Request data
Request data to serve a feature list online consists of the instance of the feature list (main) entities for which inference is needed. Users may also provide the instance of the parent entities, and the context or use case for which the request is made.

For the inference of contexts with an event entity, request data may also include the instance of entities attributes that are not available yet at inference time.

If the feature list served includes On Demand features, the request data should also include the information needed to compute the On Demand features.

### Disabling deployment
Deployment can be disabled any time if online serving of the feature list is not needed anymore. Unlike for the log and wait approach adopted by some feature stores, disabling the deployment of a feature doesn’t affect the serving of the historical requests.
