from databricks_cli.jobs.api import JobsApi
from databricks_cli.sdk.api_client import ApiClient

api_client = ApiClient(
    host="https://2085793316075774.4.gcp.databricks.com",
    token="dapi04004b8a8c85e210bf5b57783d8720cd",
)

jobs_api = JobsApi(api_client)

jobs_api.run_now(
    job_id=884089431435841,
    notebook_params={"TILE_ID": "1236"},
    jar_params=None,
    python_params=None,
    spark_submit_params=None,
)
