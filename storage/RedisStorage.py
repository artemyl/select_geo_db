import redis
from profilehooks import timecall
from storage.AbstractStorage import AbstractStorage


class RedisStorage(AbstractStorage):
    KEY_DRIVERS = "Drivers"

    def __init__(self):
        self._redis = redis.Redis()

    def clear_storage(self):
        self._redis.delete("RUSSIA")
        super(RedisStorage, self).clear_storage()

    @timecall(immediate=True)
    def experiment_search(self, test_data):
        i = 0
        for item in test_data:
            command = "GEORADIUS {key} {lng} {lat} {dist_km} km WITHCOORD WITHDIST".format(
                key=RedisStorage.KEY_DRIVERS,
                lat=item["lat"],
                lng=item["lng"],
                dist_km=10
            )
            result = self._redis.execute_command(command)
            i += 1
            if i % 1000 == 0:
                print i

    def prepare_storage_for_experiment(self, test_data):
        for item in test_data:
            command = "GEOADD {key} {lng} {lat} \"{id}\"".format(
                key=RedisStorage.KEY_DRIVERS,
                lat=item["position"]["lat"],
                lng=item["position"]["lng"],
                id=item["id"]
            )
            self._redis.execute_command(command)

    @timecall(immediate=True)
    def experiment_update(self, test_data):
        for item in test_data:
            command_rm = "ZREM {key} \"{id}\"".format(
                key=RedisStorage.KEY_DRIVERS,
                id=item["id"]
            )
            self._redis.execute_command(command_rm)
            command_add = "GEOADD {key} {lng} {lat} \"{id}\"".format(
                key=RedisStorage.KEY_DRIVERS,
                lat=item["position"]["lat"],
                lng=item["position"]["lng"],
                id=item["id"]
            )
            self._redis.execute_command(command_add)
