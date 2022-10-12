import copy
import json

from databricks_cli.clusters.api import ClusterApi
from databricks_cli.jobs.api import JobsApi
from databricks_cli.sdk.api_client import ApiClient

api_client = ApiClient(
    host="https://2085793316075774.4.gcp.databricks.com",
    token="dapi04004b8a8c85e210bf5b57783d8720cd",
)

# clusters_api = ClusterApi(api_client)
#
# print("clusters: ", clusters_api.list_clusters())
#
# 'cluster_id': '1010-041133-p07dh5j6'

jobs_api = JobsApi(api_client)

# job_setting = jobs_api.get_job(job_id="692026930850269")
#
# print("**get_job 1: ", json.dumps(job_setting, indent=4))
#
#
# job_setting["settings"]['schedule'] = {'quartz_cron_expression': '30 * * * * ?', 'timezone_id': 'UTC', 'pause_status': 'UNPAUSED'}
#
# request_json = {
#     "job_id": "692026930850269",
#     "new_settings": job_setting
# }
#
#
# result = jobs_api.reset_job(json=request_json)
#
# print("**reset result: ", result)
#
# job_setting = jobs_api.get_job(job_id="692026930850269")
# print("**get_job 2: ", job_setting)

job_spec = {
    "name": "test_job_2",
    "tasks": [
        {
            "task_key": "test_job_2_task_1",
            "existing_cluster_id": "1010-041133-p07dh5j6",
            "notebook_task": {
                "notebook_path": "/Users/yanhui@featurebyte.com/test_schedule_1",
                "source": "WORKSPACE",
            },
        }
    ],
    "schedule": {
        "quartz_cron_expression": "30 * * * * ?",
        "timezone_id": "UTC",
        "pause_status": "UNPAUSED",
    },
}

# result = jobs_api.create_job(json=job_spec)
#
# print("**reset result: ", result)

job_spec2 = copy.deepcopy(job_spec)
job_spec2["schedule"]["quartz_cron_expression"] = "15 * * * * ?"
job_spec2["schedule"]["pause_status"] = "PAUSED"

update_job_spec = {"job_id": 379919337018178, "new_settings": job_spec2}

result = jobs_api.reset_job(json=update_job_spec)

print("**reset result: ", result)
