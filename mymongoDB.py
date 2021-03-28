from typing import List

from sortedcontainers import SortedDict

from aux import MongoException
from mymongocoll import MyMongoCollection
import dill as pickle


class MyMongoDB:
    def __init__(self, name: str):
        self.name: str = name
        self.__collections: SortedDict[str, MyMongoCollection] = SortedDict()

    def __len__(self) -> int:
        return len(self.__collections)

    def __getattr__(self, item) -> MyMongoCollection:
        import re
        if not re.fullmatch(r"__.*", item):
            return self.__getitem__(item)
        else:
            raise MongoException("collections must NOT start with double underscore!")

    def __getitem__(self, item):
        if item in self.__collections.keys():
            return self.__collections[item]
        else:
            new = MyMongoCollection(self, item)
            self.__collections[item] = new
            return new

    def __getstate__(self):
        return self.__dict__.copy()

    def __setstate__(self, state):
        self.name = state['name']
        self.__collections = state['_MyMongoDB__collections']

    def list_collections(self) -> List[str]:
        return [key for key in self.__collections.keys()]

    def save(self, name=None, overwrite=False) -> None:
        import re
        mode = 'wb' if overwrite else 'xb'
        if name is None:
            name = re.sub(r"\.mongodb$", "", self.name)
            with open(f"{name}.mongodb", mode) as f:
                pickle.dump(self.__dict__, f)
        else:
            name = re.sub(r"\.mongodb$", "", name)
            with open(f"{name}.mongodb", mode) as f:
                pickle.dump(self.__dict__, f)

    @staticmethod
    def load(pickled_file):
        import _io
        new_db = MyMongoDB('new')
        if isinstance(pickled_file, str):
            with open(pickled_file, 'rb') as f:
                db = pickle.load(f)
        elif isinstance(pickled_file, _io.TextIOWrapper):
            db = pickle.load(pickled_file)
        else:
            raise FileNotFoundError(f"Cannot find such file: {pickled_file}")
        new_db.__dict__.update(db)
        return new_db
