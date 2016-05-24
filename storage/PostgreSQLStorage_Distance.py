from profilehooks import timecall
from storage.PosgreSQLStorage import PostgreSQLStorage


class PostgreSQLStorageUsingDistance(PostgreSQLStorage):

    @timecall(immediate=True)
    def experiment_search(self, test_data):
        cursor = self.db_connect.cursor()

        for item in test_data:
            request = "SELECT * FROM {table_name} " \
                      "WHERE ST_Distance({column_geo},ST_MakePoint({lat},{lng})) < 10000".format(
                table_name=PostgreSQLStorage.table_name,
                column_geo=PostgreSQLStorage.column_position,
                lat=item["lat"],
                lng=item["lng"])
            cursor.execute(request)
            search_result = cursor.fetchall()
