from copy import deepcopy
from typing import Dict, Union, MutableMapping, Iterable, Any, MutableSet, List, \
    Tuple

from aux import MongoLastInserted, MongoException
from mymongodoc import\
    MyMongoDoc,\
    MyMongoDocFactory as _MyMongoDocFactory,\
    MongoId
from sortedcontainers import SortedDict, SortedKeyList, SortedSet

# alias
FieldName = str


class MyMongoCollection:

    def __init__(self, database, name: str):
        from mymongoDB import MyMongoDB

        self.name: str = name
        if not isinstance(database, MyMongoDB):
            raise MongoException(
                "You fool! What is this disgusting thing that you pass to constructor method?")
        self.parent_database: MyMongoDB = database
        # сортирован по ключам словарей
        self.__docs: SortedKeyList[MongoId, MyMongoDoc] = SortedKeyList(key=lambda doc: doc['objectId'])
        self.__indices: Dict[FieldName, SortedKeyList] = SortedDict()
        self.__reserved_ids: MutableSet = SortedSet(set())

    def __repr__(self) -> str:
        meta = f"MyMongoCollection({repr(self.parent_database)}, {self.name})"
        return meta

    def __len__(self) -> int:
        return len(self.__docs)

    def __getstate__(self):
        return self.__dict__

    def __setstate__(self, state):
        self.__dict__ = state

    def sort(self, key):
        self.__docs = SortedKeyList(self.__docs, key=key)
        for field in self.__indices.keys():
            self.create_index(field)

    def create_index(self, field: str) -> None:
        if not isinstance(field, str):
            raise MongoException(
                "'Field' argument must be a string")
        # только документы с данным полем
        relevant_docs: List[MyMongoDoc] = [doc for doc in self.__docs if field in doc.keys()]
        self.__indices[field] = SortedKeyList(relevant_docs, key=lambda doc: doc[field])

    def insert_one(self, doc: Union[Dict,
                   MutableMapping]) -> MongoLastInserted:
        if not (isinstance(doc, dict) or issubclass(
                doc.__class__, MutableMapping)):
            raise MongoException(f"Document \n{doc}\n must be an instance of dict"
                                 "or a type that inherits from collections.MutableMapping")
        new_doc = _MyMongoDocFactory.get_doc(data=doc)
        if new_doc.objectId in self.__reserved_ids:
            raise MongoException(
                f"Duplicate key error: document already in collection {new_doc}")
        else:
            self.__reserved_ids.add(new_doc.objectId)
        self.__docs.add(new_doc)

        return MongoLastInserted(new_doc)

    def insert_many(self, *docs) -> List[MyMongoDoc]:
        last: List[MyMongoDoc] = list()
        if isinstance(docs[0], dict) or issubclass(docs[0].__class__, MutableMapping):
            pass
        # любой другой iterable на свой страх и риск
        elif hasattr(docs[0], "__iter__"):
            docs = docs[0]
        else:
            raise MongoException(f"Function accepts iterables of dicts or a type that inherits from "
                                 "collections.MutableMapping, or simply non-keyword arguments.")
        for doc in docs:
            last.append(self.insert_one(doc).document)
        return last

    def delete_one(self, object_id: MongoId) -> None:
        try:
            if isinstance(object_id, MongoId):
                self.__reserved_ids.remove(object_id)
                dummy = {"objectId": object_id}
                # очистим индексы от удаляемого объекта
                for field in self.__indices.keys():
                    obj_idx = self.__indices[field].bisect_left(dummy)
                    self.__indices['objectId'].remove(obj_idx)
                obj_idx = self.__docs.bisect_left(dummy)
                self.__docs.pop(obj_idx)
            else:
                raise TypeError(
                    "Only instances of MongoId can serve as document identifiers.")
        except KeyError as e:
            raise KeyError(
                f"Collection {self.name} has no object with id {object_id}.")

    def delete_many(self, object_ids: Iterable[MongoId]):
        for object_id in object_ids:
            self.delete_one(object_id)

    def clear(self):
        self.__docs.clear()

    def find_one(self, query: Dict[str, Any]):
        result = None
        relevant_docs, _query = self.__get_relevant_info(query)
        if len(_query) == 0:
            return relevant_docs
        else:
            relevant_docs = iter(relevant_docs)
        while result is None:
            candidate = next(relevant_docs)
            try:
                boolean = [candidate[key] ==
                           value for key, value in _query.items()]
                if all(boolean):
                    result = candidate
            except KeyError:
                pass
        return result

    def find(self, query: Dict[str, Any]) -> Iterable[MyMongoDoc]:
        result = list()
        relevant_docs, _query = self.__get_relevant_info(query)
        if len(_query) == 0:
            return list(relevant_docs)
        else:
            relevant_docs = iter(relevant_docs)
        for candidate in relevant_docs:
            try:
                boolean = [candidate[key] ==
                           value for key, value in _query.items()]
                if all(boolean):
                    result.append(candidate)
            except KeyError:
                pass
        return list(result)

    def __get_relevant_info(self, query) -> Tuple[Iterable, MutableMapping]:
        relevant_docs = set()
        query_ = deepcopy(query)
        indexed_query_fields = self.__indices.keys() & query_.keys()
        # Авось придёт запрос по индексирвованным полям
        if len(indexed_query_fields) != 0:
            # если по полям составлен индекс, то учитывая, что все элементы query логически
            # связаны оператором AND, то для начала можно просто вынуть пересечение документов, удовлетворяющих
            # требованиям к индексированным полям.
            for indexed_query_field in indexed_query_fields:
                # marker - dummy объект, словарик с искомым полем и значением. Мы ищем с логарифмической сложностью,
                # куда его можно приткнуть в наш индекс (находим точку до документов с идентичным значением в
                # искомом поле), а затем извлекаем 0+ равнозначных (в рамках искомого поля) объектов
                marker = {indexed_query_field: query[indexed_query_field]}
                index: SortedKeyList = self.__indices[indexed_query_field]

                # Return an index to insert value in the sorted list. If the value is already present,
                # the insertion point will be before (to the left of) any
                # existing values.
                idx = index.bisect_left(marker)
                while idx < len(index) and query[indexed_query_field] == index[idx][indexed_query_field]:
                    relevant_docs.add(index[idx])
                    idx += 1
            # мы можем больше не смотреть на поля, по которым имеется индекс
            query_ = {k: v for k, v in query.items(
            ) if k not in indexed_query_fields}

        else:
            # Что поделать, раз уж запрос такой?
            relevant_docs = self.__docs
        return relevant_docs, query_
