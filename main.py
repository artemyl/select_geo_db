import random

from storage.MongoStorage import MongoStorage
from storage.PosgreSQLStorage import PostgreSQLStorage
from storage.RedisStorage import RedisStorage

TEST_DATA_SIZE = 3000
STORAGE_LIST = RedisStorage(), MongoStorage(), PostgreSQLStorage()
# STORAGE_LIST = MongoStorage(),


def generate_test_data(count):
    result = []
    for i in range(count):
        result.append({
            'id': i,
            'position': {
                'lat': random.uniform(55.0, 56.0),
                'lng': random.uniform(37.0, 38.0)
            }
        })
    return result


def generate_test_search_data(count):
    result = []
    for i in range(count):
        result.append({
                'lat': random.uniform(55.0, 56.0),
                'lng': random.uniform(37.0, 38.0)
            })
    return result


def generate_test_update_data(count):
    result = generate_test_data(count)
    random.shuffle(result)
    return result

test_data = generate_test_data(TEST_DATA_SIZE)
search_test_data = generate_test_search_data(TEST_DATA_SIZE)
update_test_data = generate_test_update_data(TEST_DATA_SIZE)


for storage in STORAGE_LIST:
    try:
        storage.prepare_storage_for_experiment(test_data)
        storage.experiment_search(search_test_data)
        storage.experiment_update(update_test_data)
    except Exception as e:
        print(e)
    finally:
        storage.clear_storage()
