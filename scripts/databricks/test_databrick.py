import copy
import json

from databricks_cli.clusters.api import ClusterApi
from databricks_cli.jobs.api import JobsApi
from databricks_cli.sdk.api_client import ApiClient

api_client = ApiClient(
    host="https://2085793316075774.4.gcp.databricks.com",
    token="dapi04004b8a8c85e210bf5b57783d8720cd",
)

clusters_api = ClusterApi(api_client)

print("clusters: ", clusters_api.list_clusters())
