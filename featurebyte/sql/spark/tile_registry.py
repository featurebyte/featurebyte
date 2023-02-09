"""
Tile Registry Job Script for SP_TILE_REGISTRY
"""


from .tile_common import TileCommon


class TileRegistry(TileCommon):
    table_name: str
    table_exist: str

    def execute(self) -> None:

        df = self._spark.sql(
            f"select VALUE_COLUMN_NAMES as names, VALUE_COLUMN_TYPES as types from tile_registry where tile_id = '{self.tile_id}'"
        )

        res = df.select("names", "types").collect()
        print("res: ", res)

        input_value_columns = [
            value for value in self.value_column_names.split(",") if value.strip()
        ]
        print("input_value_columns: ", input_value_columns)

        input_value_columns_types = [
            value for value in self.value_column_types.split(",") if value.strip()
        ]
        print("input_value_columns_types: ", input_value_columns_types)

        if res:
            value_cols = res[0].names
            value_cols_types = res[0].types
            print("value_cols: ", value_cols)
            print("value_cols_types: ", value_cols_types)

            exist_columns = [value for value in value_cols.split(",") if value.strip()]
            exist_columns_types = [value for value in value_cols_types.split(",") if value.strip()]

            for i, input_column in enumerate(input_value_columns):
                if input_column not in exist_columns:
                    exist_columns.append(input_column)

                input_column_type = input_value_columns_types[i]
                if input_column_type not in exist_columns_types:
                    exist_columns_types.append(input_column_type)

            new_value_columns_str = ",".join(exist_columns)
            print("new_value_columns_str: ", new_value_columns_str)
            new_value_columns_types_str = ",".join(exist_columns_types)
            print("new_value_columns_types_str: ", new_value_columns_types_str)

            update_sql = f"""
                            UPDATE TILE_REGISTRY SET
                                VALUE_COLUMN_NAMES = '{new_value_columns_str}',
                                VALUE_COLUMN_TYPES = '{new_value_columns_types_str}'
                            WHERE TILE_ID = '{self.tile_id}'
                         """
            self._spark.sql(update_sql)
        else:
            print("No value columns")
            escape_sql = self.sql.replace("'", "''")
            insert_sql = f"""
                insert into tile_registry(
                    TILE_ID,
                    TILE_SQL,
                    ENTITY_COLUMN_NAMES,
                    VALUE_COLUMN_NAMES,
                    VALUE_COLUMN_TYPES,
                    FREQUENCY_MINUTE,
                    TIME_MODULO_FREQUENCY_SECOND,
                    BLIND_SPOT_SECOND,
                    IS_ENABLED,
                    CREATED_AT,
                    LAST_TILE_START_DATE_ONLINE,
                    LAST_TILE_INDEX_ONLINE,
                    LAST_TILE_START_DATE_OFFLINE,
                    LAST_TILE_INDEX_OFFLINE
                )
                VALUES (
                    '{self.tile_id}',
                    '{escape_sql}',
                    '{self.entity_column_names}',
                    '{self.value_column_names}',
                    '{self.value_column_types}',
                    {self.frequency_minute},
                    {self.tile_modulo_frequency_second},
                    {self.blind_spot_second},
                    TRUE,
                    current_timestamp(),
                    null,
                    null,
                    null,
                    null
                )
            """
            print("insert_sql: ", insert_sql)
            self._spark.sql(insert_sql)

        if self.table_exist == "Y":
            df = self._spark.sql(f"SHOW COLUMNS IN {self.table_name}")
            cols = []
            for col in df.collect():
                cols.append(col.col_name)

            print("cols: ", cols)

            tile_add_sql = f"ALTER TABLE {self.table_name} ADD COLUMN\n"
            add_statements = []
            for i, input_column in enumerate(input_value_columns):
                if input_column not in cols:
                    element_type = input_value_columns_types[i]
                    add_statements.append(f"{input_column} {element_type}")
                    if "_MONITOR" in self.table_name:
                        add_statements.append(f"OLD_{input_column} {element_type}")

            if add_statements:
                tile_add_sql += ",\n".join(add_statements)
                print("tile_add_sql: ", tile_add_sql)
                self._spark.sql(tile_add_sql)
