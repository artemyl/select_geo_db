from abc import ABCMeta, abstractmethod


class AbstractStorage(object):
    __metaclass__ = ABCMeta

    @abstractmethod
    def prepare_storage_for_experiment(self, test_data):
        pass

    @abstractmethod
    def experiment_search(self, test_data):
        pass

    @abstractmethod
    def experiment_update(self, test_data):
        pass

    @abstractmethod
    def clear_storage(self):
        pass
