import psycopg2
from profilehooks import timecall
from storage.AbstractStorage import AbstractStorage


class PostgreSQLStorage(AbstractStorage):
    table_name = "taxi_drivers"
    column_id = "driver_id"
    column_position = "driver_position"

    def __init__(self):
        self.db_connect = psycopg2.connect(database="testdb", user="postgres", password="qwerty")

    @timecall(immediate=True)
    def experiment_search(self, test_data):
        cursor = self.db_connect.cursor()

        for item in test_data:
            request = "SELECT * FROM {table_name} " \
                      "WHERE ST_DWithin({column_geo},ST_MakePoint({lat},{lng}), 10000)".format(
                table_name=PostgreSQLStorage.table_name,
                column_geo=PostgreSQLStorage.column_position,
                lat=item["lat"],
                lng=item["lng"])
            cursor.execute(request)
            search_result = cursor.fetchall()
            # print(len(search_result))

    def clear_storage(self):
        cursor = self.db_connect.cursor()
        cursor.execute("DROP TABLE IF EXISTS {table_name}".format(
            table_name=PostgreSQLStorage.table_name
        ))
        self.db_connect.commit()

    @timecall(immediate=True)
    def experiment_update(self, test_data):
        cursor = self.db_connect.cursor()

        for item in test_data:
            request = "UPDATE {table_name} set {update_column_name}=ST_MakePoint({lat},{lng}) " \
                      "where {id_column_name}={id}".format(
                table_name=PostgreSQLStorage.table_name,
                update_column_name=PostgreSQLStorage.column_position,
                id_column_name=PostgreSQLStorage.column_id,
                lat=item["position"]["lat"],
                lng=item["position"]["lng"],
                id=item["id"]
            )
            cursor.execute(request)
            self.db_connect.commit()

    def prepare_storage_for_experiment(self, test_data):
        self.clear_storage()
        cursor = self.db_connect.cursor()
        cursor.execute("CREATE TABLE {table_name} ({column_id} int, {column_geo} GEOGRAPHY(Point))".format(
            table_name=PostgreSQLStorage.table_name,
            column_id=PostgreSQLStorage.column_id,
            column_geo=PostgreSQLStorage.column_position
        ))
        self.db_connect.commit()
        for item in test_data:
            cursor.execute("INSERT INTO {table_name} "
                           "VALUES ({taxi_driver_id}, ST_MakePoint({taxi_driver_lat}, {taxi_driver_lng}))".format(
                table_name=PostgreSQLStorage.table_name,
                taxi_driver_id=item["id"],
                taxi_driver_lat=item["position"]["lat"],
                taxi_driver_lng=item["position"]["lng"],
            ))
        cursor.execute("CREATE INDEX geo_index ON {table_name} USING GIST ({column_geo});".format(
            table_name=PostgreSQLStorage.table_name,
            column_geo=PostgreSQLStorage.column_position
        ))
        cursor.execute("CREATE INDEX id_index ON {table_name}({column_id}); ".format(
            table_name=PostgreSQLStorage.table_name,
            column_id=PostgreSQLStorage.column_id
        ))
        self.db_connect.commit()
