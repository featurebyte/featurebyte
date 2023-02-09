"""
Tile Generate tests for Spark Session
"""
from datetime import datetime

from featurebyte.enum import InternalName
from featurebyte.sql.spark.tile_generate import TileGenerate


def test_generate_tile(spark_session):
    """
    Test normal generation of tiles
    """

    entity_col_names = "PRODUCT_ACTION,CUST_ID"
    value_col_names = "VALUE"
    value_col_types = "FLOAT"
    table_name = "TEMP_TABLE"
    tile_id = f"TEMP_TABLE_{datetime.now().strftime('%Y%m%d%H%M%S_%f')}"
    tile_sql = (
        f"SELECT {InternalName.TILE_START_DATE},{entity_col_names},{value_col_names} FROM {table_name} "
        f"WHERE date_format({InternalName.TILE_START_DATE}, 'yyyy-MM-dd HH:mm:ss') >= '2022-06-05 23:48:00' "
        f"AND date_format({InternalName.TILE_START_DATE}, 'yyyy-MM-dd HH:mm:ss') < '2022-06-05 23:58:00'"
    )

    tile_generate_ins = TileGenerate(
        spark_session=spark_session,
        featurebyte_database="TEST_DB_1",
        tile_id=tile_id,
        tile_modulo_frequency_second=183,
        blind_spot_second=3,
        frequency_minute=5,
        sql=tile_sql,
        entity_column_names=entity_col_names,
        value_column_names=value_col_names,
        value_column_types=value_col_types,
        tile_type="OFFLINE",
        tile_start_date_column=InternalName.TILE_START_DATE,
    )

    tile_generate_ins.execute()

    sql = f"SELECT COUNT(*) as TILE_COUNT FROM {tile_id}"
    result = spark_session.sql(sql).collect()
    assert result[0].TILE_COUNT == 2

    result = spark_session.sql(f"SELECT * FROM TILE_REGISTRY WHERE TILE_ID = '{tile_id}'").collect()
    assert result[0].VALUE_COLUMN_NAMES == "VALUE"
    assert result[0].VALUE_COLUMN_TYPES == "FLOAT"


def test_generate_tile_no_data(spark_session):
    """
    Test generation of tile with no tile data
    """
    entity_col_names = "PRODUCT_ACTION,CUST_ID"
    value_col_names = "VALUE"
    value_col_types = "FLOAT"
    table_name = "TEMP_TABLE"
    tile_id = f"TEMP_TABLE_{datetime.now().strftime('%Y%m%d%H%M%S_%f')}"
    tile_sql = (
        f"SELECT {InternalName.TILE_START_DATE},{entity_col_names},{value_col_names} "
        f"FROM {table_name} "
        f"WHERE date_format({InternalName.TILE_START_DATE}, 'yyyy-MM-dd HH:mm:ss') > '2022-06-05 23:58:00'"
    )

    tile_generate_ins = TileGenerate(
        spark_session=spark_session,
        featurebyte_database="TEST_DB_1",
        tile_id=tile_id,
        tile_modulo_frequency_second=183,
        blind_spot_second=3,
        frequency_minute=5,
        sql=tile_sql,
        entity_column_names=entity_col_names,
        value_column_names=value_col_names,
        value_column_types=value_col_types,
        tile_type="OFFLINE",
        tile_start_date_column=InternalName.TILE_START_DATE,
    )

    tile_generate_ins.execute()

    sql = f"SELECT COUNT(*) as TILE_COUNT FROM {tile_id}"
    result = spark_session.sql(sql).collect()
    assert result[0].TILE_COUNT == 0


def test_generate_tile_new_value_column(spark_session):
    """
    Test normal generation of tiles
    """
    entity_col_names = "PRODUCT_ACTION,CUST_ID"
    value_col_names = "VALUE"
    value_col_types = "FLOAT"
    table_name = "TEMP_TABLE"
    tile_id = f"TEMP_TABLE_{datetime.now().strftime('%Y%m%d%H%M%S_%f')}"
    tile_sql = (
        f"SELECT {InternalName.TILE_START_DATE},{entity_col_names},{value_col_names} FROM {table_name} "
        f"WHERE date_format({InternalName.TILE_START_DATE}, 'yyyy-MM-dd HH:mm:ss') >= '2022-06-05 23:48:00' "
        f"AND date_format({InternalName.TILE_START_DATE}, 'yyyy-MM-dd HH:mm:ss') < '2022-06-05 23:58:00'"
    )

    tile_generate_ins = TileGenerate(
        spark_session=spark_session,
        featurebyte_database="TEST_DB_1",
        tile_id=tile_id,
        tile_modulo_frequency_second=183,
        blind_spot_second=3,
        frequency_minute=5,
        sql=tile_sql,
        entity_column_names=entity_col_names,
        value_column_names=value_col_names,
        value_column_types=value_col_types,
        tile_type="OFFLINE",
        tile_start_date_column=InternalName.TILE_START_DATE,
    )

    tile_generate_ins.execute()

    sql = f"SELECT {value_col_names} FROM {tile_id}"
    result = spark_session.sql(sql).collect()
    assert len(result) == 2

    sql = f"SELECT VALUE_COLUMN_NAMES FROM TILE_REGISTRY WHERE TILE_ID = '{tile_id}'"
    result = spark_session.sql(sql).collect()
    assert len(result) == 1
    assert result[0].VALUE_COLUMN_NAMES == "VALUE"

    value_col_names_2 = "VALUE,VALUE_2"
    value_col_types_2 = "FLOAT,FLOAT"
    tile_sql_2 = (
        f"SELECT {InternalName.TILE_START_DATE},{entity_col_names},{value_col_names_2} FROM {table_name} "
        f"WHERE date_format({InternalName.TILE_START_DATE}, 'yyyy-MM-dd HH:mm:ss') >= '2022-06-05 23:48:00' "
        f"AND date_format({InternalName.TILE_START_DATE}, 'yyyy-MM-dd HH:mm:ss') < '2022-06-05 23:58:00'"
    )

    tile_generate_ins = TileGenerate(
        spark_session=spark_session,
        featurebyte_database="TEST_DB_1",
        tile_id=tile_id,
        tile_modulo_frequency_second=183,
        blind_spot_second=3,
        frequency_minute=5,
        sql=tile_sql_2,
        entity_column_names=entity_col_names,
        value_column_names=value_col_names_2,
        value_column_types=value_col_types_2,
        tile_type="OFFLINE",
        tile_start_date_column=InternalName.TILE_START_DATE,
    )

    tile_generate_ins.execute()

    sql = f"SELECT {value_col_names_2} FROM {tile_id}"
    result = spark_session.sql(sql).collect()
    assert len(result) == 2

    sql = f"SELECT VALUE_COLUMN_NAMES FROM TILE_REGISTRY WHERE TILE_ID = '{tile_id}'"
    result = spark_session.sql(sql).collect()
    assert len(result) == 1
    assert result[0].VALUE_COLUMN_NAMES == "VALUE,VALUE_2"
