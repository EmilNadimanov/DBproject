from mymongodoc import MyMongoDoc, MongoId


class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(
                Singleton, cls).__call__(*args)
        else:
            cls._instances[cls].__call__(*args)
        return cls._instances[cls]


class MongoLastInserted(metaclass=Singleton):
    def __init__(self, document):
        self.document: MyMongoDoc = document

    def __call__(self, document):
        self.document = document

    def inserted_id(self) -> MongoId:
        return self.document.objectId


class MongoException(Exception):

    def __init__(self, msg):
        super().__init__(msg)
