from abc import ABC, abstractmethod


class DataRepository(ABC):
    '''Repository interface for persisting and loading data products.'''

    @property
    @abstractmethod
    def name(self):
        '''Human-readable repository name.'''

    @abstractmethod
    def save_data_products(self, data_products):
        '''Persist data products.'''

    @abstractmethod
    def load_data_products(self):
        '''Load data products.'''
