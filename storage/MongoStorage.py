from AbstractStorage import AbstractStorage
from profilehooks import timecall
from pymongo import MongoClient, GEOSPHERE


class MongoStorage(AbstractStorage):
    db_name = "taxi_geo_experiment_test_db"
    collection_name = "taxi_driver_collection"
    key_id = "key_id"
    key_position = "key_position"

    def __init__(self):
        self.client = MongoClient()

    @timecall(immediate=True)
    def experiment_update(self, test_data):

        for t in test_data:
            self.collection.update_one(
                {
                    MongoStorage.key_id: t["id"]
                },
                {
                    '$set': {
                        MongoStorage.key_position: {
                            'type': "Point",
                            'coordinates': [t['position']['lng'], t['position']['lat']]
                        }
                    }
                }
            )

    @timecall(immediate=True)
    def experiment_search(self, test_data):
        def find_point(point):
            cursor = self.collection.find(
                {
                    MongoStorage.key_position:
                        {
                            '$near':
                                {
                                    '$geometry':
                                        {
                                            'type': "Point",
                                            'coordinates': [point['lng'], point['lat']]
                                        },
                                    '$maxDistance': 10000
                                }
                        }
                }
            )
            return cursor

        for point in test_data:
            cursor = find_point(point)
            # print "Next point result"
            # for result in cursor:
            #    print result

    def prepare_storage_for_experiment(self, test_data):
        self.db = self.client[MongoStorage.db_name]
        self.collection = self.db.create_collection(MongoStorage.collection_name)
        self.collection.create_index([(MongoStorage.key_id, 1)])
        self.collection.create_index([(MongoStorage.key_position, GEOSPHERE)])
        for t in test_data:
            self.collection.insert({
                MongoStorage.key_id: t['id'],
                MongoStorage.key_position: {
                    'type': "Point",
                    'coordinates': [t['position']['lng'], t['position']['lat']]
                }
            })

    def clear_storage(self):
        self.client.drop_database(MongoStorage.db_name)
