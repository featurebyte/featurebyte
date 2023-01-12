from databricks_cli.jobs.api import JobsApi
from databricks_cli.sdk.api_client import ApiClient

databricks_host = "https://2085793316075774.4.gcp.databricks.com"
databricks_token = "dapi79ca878eeeea4b33ed9a5ea5b907a46b"

api_client = ApiClient(
    host=databricks_host,
    token=databricks_token,
)

jobs_api = JobsApi(api_client)

# run SP_TILE_REGISTRY
# jobs_api.run_now(
#     job_id=5060465458223,
#     notebook_params={
#         "FEATUREBYTE_DATABASE": "featurebyte_github_20221028124418_559760",
#         "SQL": "SELECT * FROM TEST",
#         "TIME_MODULO_FREQUENCY_SECOND": 183,
#         "BLIND_SPOT_SECOND": 3,
#         "FREQUENCY_MINUTE": 5,
#         "ENTITY_COLUMN_NAMES": "PRODUCT_ACTION,CUST_ID",
#         "VALUE_COLUMN_NAMES": "VALUE,VALUE1,VALUE2",
#         "TILE_ID": "tile_id1",
#         "TABLE_NAME": "tile_id1",
#         "TABLE_EXIST": "Y",
#     },
#     jar_params=None,
#     python_params=None,
#     spark_submit_params=None,
# )


# run SP_TILE_MONITOR
# monitor_sql = "select * from tile_id1_input"
# jobs_api.run_now(
#     job_id=1035125421595393,
#     notebook_params={
#         "FEATUREBYTE_DATABASE": "featurebyte_github_20221028124418_559760",
#         "MONITOR_SQL": monitor_sql,
#         "TILE_START_DATE_COLUMN": "TILE_START_DATE",
#         "TIME_MODULO_FREQUENCY_SECOND": 183,
#         "BLIND_SPOT_SECOND": 3,
#         "FREQUENCY_MINUTE": 5,
#         "ENTITY_COLUMN_NAMES": "PRODUCT_ACTION,CUST_ID",
#         "VALUE_COLUMN_NAMES": "VALUE,VALUE1,VALUE2",
#         "TILE_ID": "tile_id1",
#         "TILE_TYPE": "ONLINE",
#         "DATABRICKS_HOST": databricks_host,
#         "DATABRICKS_TOKEN": databricks_token,
#     },
#     jar_params=None,
#     python_params=None,
#     spark_submit_params=None,
# )

# run SP_TILE_GENERATE
# jobs_api.run_now(
#     job_id=177372429009472,
#     notebook_params={
#         "FEATUREBYTE_DATABASE": "featurebyte_github_20221028124418_559760",
#         "SQL": "select * from tile_id1_input",
#         "TILE_START_DATE_COLUMN": "TILE_START_DATE",
#         "TILE_LAST_START_DATE_COLUMN": "LAST_TILE_START_DATE",
#         "TIME_MODULO_FREQUENCY_SECOND": 183,
#         "BLIND_SPOT_SECOND": 3,
#         "FREQUENCY_MINUTE": 5,
#         "ENTITY_COLUMN_NAMES": "PRODUCT_ACTION,CUST_ID",
#         "VALUE_COLUMN_NAMES": "VALUE,VALUE1,VALUE2",
#         "TILE_ID": "tile_id2",
#         "TILE_TYPE": "ONLINE",
#         "LAST_TILE_START_STR": "2022-10-13T23:53:00.000Z",
#         "DATABRICKS_HOST": databricks_host,
#         "DATABRICKS_TOKEN": databricks_token,
#     },
#     jar_params=None,
#     python_params=None,
#     spark_submit_params=None,
# )


# run SP_TILE_GENERATE_SCHEDULE
# jobs_api.run_now(
#     job_id=964130063109057,
#     notebook_params={
#         "FEATUREBYTE_DATABASE": "featurebyte_github_20221028124418_559760",
#         "TILE_ID": "tile_id3",
#         "TIME_MODULO_FREQUENCY_SECOND": 183,
#         "BLIND_SPOT_SECOND": 3,
#         "FREQUENCY_MINUTE": 5,
#         "SQL": "select * from tile_id1_input",
#         "OFFLINE_PERIOD_MINUTE": 1440,
#         "TILE_START_DATE_COLUMN": "TILE_START_DATE",
#         "TILE_LAST_START_DATE_COLUMN": "TILE_START_DATE",
#         "TILE_START_DATE_PLACEHOLDER": "TILE_START_DATE",
#         "TILE_END_DATE_PLACEHOLDER": "TILE_START_DATE",
#         "ENTITY_COLUMN_NAMES": "PRODUCT_ACTION,CUST_ID",
#         "VALUE_COLUMN_NAMES": "VALUE,VALUE1,VALUE2",
#         "TYPE": "ONLINE",
#         "MONITOR_PERIODS": 10,
#         "JOB_SCHEDULE_TS": "2022-10-13T23:53:00.000Z",
#         "DATABRICKS_HOST": databricks_host,
#         "DATABRICKS_TOKEN": databricks_token,
#     },
#     jar_params=None,
#     python_params=None,
#     spark_submit_params=None,
# )


# run SP_TILE_SCHEDULE_ONLINE_STORE
# jobs_api.run_now(
#     job_id=956202849191914,
#     notebook_params={
#         "FEATUREBYTE_DATABASE": "featurebyte_github_20221028124418_559760",
#         "TILE_ID": "tile_id3",
#         "JOB_SCHEDULE_TS_STR": "2022-10-13T23:53:00.000Z",
#     },
#     jar_params=None,
#     python_params=None,
#     spark_submit_params=None,
# )


# run SP_TILE_GENERATE_ENTITY_TRACKING
# jobs_api.run_now(
#     job_id=1069334282017391,
#     notebook_params={
#         "FEATUREBYTE_DATABASE": "featurebyte_github_20221028124418_559760",
#         "TILE_ID": "tile_id3",
#         "ENTITY_COLUMN_NAMES": "PRODUCT_ACTION,CUST_ID",
#         "ENTITY_TABLE": "entity_table_input",
#         "TILE_LAST_START_DATE_COLUMN": "TILE_START_DATE",
#     },
#     jar_params=None,
#     python_params=None,
#     spark_submit_params=None,
# )

# run SP_TILE_GENERATE_ENTITY_TRACKING as Python Script
# jobs_api.run_now(
#     job_id=580523640841080,
#     notebook_params=None,
#     jar_params=None,
#     python_params=["featurebyte_github_20221028124418_559760", "TILE_START_DATE", "PRODUCT_ACTION,CUST_ID", "tile_id3", "entity_table_input"],
#     spark_submit_params=None,
# )


# # run SP_TILE_GENERATE as Python Script
# jobs_api.run_now(
#     job_id=774382467675787,
#     notebook_params=None,
#     jar_params=None,
#     python_params=[
#         "featurebyte_github_20221028124418_559760",
#         "select * from tile_id1_input",
#         "TILE_START_DATE",
#         "LAST_TILE_START_DATE",
#         183,
#         3,
#         5,
#         "PRODUCT_ACTION,CUST_ID",
#         "VALUE,VALUE1,VALUE2",
#         "tile_id2",
#         "ONLINE",
#         "2022-10-13T23:53:00.000Z",
#         databricks_host,
#         databricks_token
#     ],
#     spark_submit_params=None,
# )


# run SP_TILE_MONITOR as Python Script
# monitor_sql = "select * from tile_id1_input"
# jobs_api.run_now(
#     job_id=64933796889339,
#     notebook_params=None,
#     jar_params=None,
#     python_params=[
#         "featurebyte_github_20221028124418_559760",
#         monitor_sql,
#         "TILE_START_DATE",
#         183,
#         3,
#         5,
#         "PRODUCT_ACTION,CUST_ID",
#         "VALUE,VALUE1,VALUE2",
#         "tile_id1",
#         "ONLINE",
#         databricks_host,
#         databricks_token
#     ],
#     spark_submit_params=None,
# )

# run SP_REGISTRY as Python Script
# jobs_api.run_now(
#     job_id=1037702005162715,
#     notebook_params=None,
#     jar_params=None,
#     python_params=[
#         "featurebyte_github_20221028124418_559760",
#         "SELECT * FROM TEST",
#         183,
#         3,
#         5,
#         "PRODUCT_ACTION,CUST_ID",
#         "VALUE,VALUE1,VALUE2",
#         "tile_id1",
#         "tile_id1",
#         "Y",
#     ],
#     spark_submit_params=None,
# )


# run SP_TILE_GENERATE_SCHEDULE as Python Script
jobs_api.run_now(
    job_id=441968797881317,
    notebook_params=None,
    jar_params=None,
    python_params=[
        "featurebyte_github_20221028124418_559760",
        "tile_id3",
        183,
        3,
        5,
        1440,
        "select * from tile_id1_input",
        "TILE_START_DATE",
        "LAST_TILE_START_DATE",
        "TILE_START_DATE",
        "TILE_END_DATE",
        "PRODUCT_ACTION,CUST_ID",
        "VALUE,VALUE1,VALUE2",
        "ONLINE",
        10,
        "2022-10-13T23:53:00.000Z",
    ],
    spark_submit_params=None,
)
