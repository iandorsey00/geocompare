from pathlib import Path
import pickle

from repository.base import DataRepository


class PickleRepository(DataRepository):
    '''Data repository backed by a pickle file.'''

    def __init__(self, path):
        self.path = Path(path)

    @property
    def name(self):
        return f'pickle:{self.path}'

    def save_data_products(self, data_products):
        self.path.parent.mkdir(parents=True, exist_ok=True)

        with self.path.open('wb') as f:
            pickle.dump(data_products, f, protocol=pickle.HIGHEST_PROTOCOL)

    def load_data_products(self):
        try:
            with self.path.open('rb') as f:
                return pickle.load(f)
        except FileNotFoundError:
            raise RuntimeError(f'data product file not found: {self.path}')
        except EOFError:
            raise RuntimeError(f'data product file is empty: {self.path}')
        except pickle.UnpicklingError:
            raise RuntimeError(
                f'data product file is corrupted or incompatible: {self.path}'
            )
        except Exception as e:
            raise RuntimeError(
                f'unexpected error loading pickle data products: {e!r}'
            )
