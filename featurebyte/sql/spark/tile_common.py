"""
Base Class for Tile Schedule Instance
"""
from pydantic.fields import PrivateAttr
from pydantic.main import BaseModel
from pyspark.sql import SparkSession


class TileCommon(BaseModel):

    featurebyte_database: str
    tile_id: str
    tile_modulo_frequency_second: int
    blind_spot_second: int
    frequency_minute: int

    sql: str
    entity_column_names: str
    value_column_names: str
    value_column_types: str

    _spark: SparkSession = PrivateAttr()

    def __init__(self, spark_session: SparkSession, **kwargs):
        """
        Initialize Tile Schedule Instance

        Parameters
        ----------
        spark_session: SparkSession
            input SparkSession
        """
        super().__init__(**kwargs)
        self._spark = spark_session
        self._spark.sql(f"USE DATABASE {self.featurebyte_database}")

    # offline_period_minute: int
    # tile_start_date_column: str
    # tile_last_start_date_column: str
    # tile_start_date_placeholder: str
    # tile_end_date_placeholder: str

    # tile_type: str
    # monitor_periods: int
    # job_schedule_ts: str

    # _spark: SparkSession = PrivateAttr()
    #
    # def __init__(self, script_path: str, **kwargs):
    #     """
    #     Initialize SparkSession
    #
    #     Parameters
    #     ----------
    #     script_path
    #
    #     Returns
    #     -------
    #     SparkSession instance
    #     """
    #     super().__init__(**kwargs)
    #
    #     spark = SparkSession.builder.appName("TileManagement").getOrCreate()
    #     spark.sparkContext.addPyFile(f"{script_path}/tile_common.py")
    #     spark.sparkContext.addPyFile(f"{script_path}/tile_registry.py")
    #     spark.sparkContext.addPyFile(f"{script_path}/tile_monitor.py")
    #     spark.sparkContext.addPyFile(f"{script_path}/tile_generate.py")
    #     spark.sparkContext.addPyFile(f"{script_path}/tile_generate_entity_tracking.py")
    #     spark.sparkContext.addPyFile(f"{script_path}/tile_schedule_online_store.py")
    #
    #     spark.sql(f"USE DATABASE {kwargs['featurebyte_database']}")
    #
    #     self._spark = spark
