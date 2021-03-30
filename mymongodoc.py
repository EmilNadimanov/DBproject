from dataclasses import dataclass
from typing import MutableMapping, Dict, Optional, Union
from copy import deepcopy
from typing import Final, Any, Iterator

import json


@dataclass(frozen=True, repr=False)
class MongoId:
    objectId: str

    def __repr__(self):
        return f"MongoId('{self.objectId.__repr__()}')"

    def __str__(self):
        return f"MongoId('{self.objectId.__str__()}')"

    def __eq__(self, other: Union[str, 'MongoId']):
        if isinstance(other, str):
            return self.objectId == other
        elif isinstance(other, MongoId):
            return self.objectId == other.objectId
        else:
            f"Cannot compare objects of classes 'Mongoid' and {other.__class__}"

    def __lt__(self, other):
        if isinstance(other, MongoId):
            return self.objectId < other.objectId
        else:
            f"Cannot compare objects of classes 'Mongoid' and {other.__class__}"

    def __le__(self, other):
        if isinstance(other, MongoId):
            return self.objectId <= other.objectId
        else:
            f"Cannot compare objects of classes 'Mongoid' and {other.__class__}"

    def __gt__(self, other):
        if isinstance(other, MongoId):
            return self.objectId > other.objectId
        else:
            f"Cannot compare objects of classes 'Mongoid' and {other.__class__}"

    def __ge__(self, other):
        if isinstance(other, MongoId):
            return self.objectId >= other.objectId
        else:
            f"Cannot compare objects of classes 'Mongoid' and {other.__class__}"


class MongoIdEncoder(json.JSONEncoder):

    def default(self, obj) -> dict:
        return obj.__dict__


""" Документ - самостоятельный объект """


class MyMongoDoc(MutableMapping):
    def __init__(self, mongo_id: 'MongoId',
                 body: Optional[MutableMapping] = None):
        self.objectId: Final[MongoId] = mongo_id

        if body is None:
            self.__body: MutableMapping[str, Any] = {"objectId": self.objectId}
        else:
            self.__body: MutableMapping[str, Any] = body | {
                "objectId": self.objectId}

    def __repr__(self):
        repr_body: str = f"{{'objectId': {self.objectId}"
        for key in self.__body:
            value: Any = self.__body[key]
            value_repr: str = repr(value) if hasattr(
                value, '__repr__') else str(value)
            repr_body += f",\n'{key}': {value_repr}"
        return repr_body + '}\n'

    def __hash__(self):
        return self.objectId.__hash__()

    def __getitem__(self, key: str) -> Any:
        return self.__body[key]

    def __setitem__(self, key, value) -> None:
        if not isinstance(key, str):
            raise Exception("Only str objects can be keys!")
        self.__body[key] = value

    def __delitem__(self, k: str) -> None:
        del self.__body[k]

    def __len__(self) -> int:
        return len(self.__body)

    def __iter__(self) -> Iterator[Any]:
        copy = deepcopy(self.__body)
        for it in copy.keys():
            yield it


class MyMongoDocEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, MyMongoDoc):
            return obj.__dict__
        json.JSONEncoder.default(self, obj)  # raise error


class MyMongoDocFactory:

    # Нуждается в id_factory извне - это объект, который можно было бы создать и контролировать снаружи,
    # но в то же время хранить как защищённое поле этого Factory-объекта
    @staticmethod
    def get_doc(
            data: Optional[MutableMapping] = None) -> MyMongoDoc:
        data_id: str = hex(id(data))[2:]
        mongo_id: MongoId = MongoId(data_id)
        if data is None:
            return MyMongoDoc(mongo_id)
        else:
            d: MutableMapping = deepcopy(data)
            return MyMongoDoc(mongo_id, d)
