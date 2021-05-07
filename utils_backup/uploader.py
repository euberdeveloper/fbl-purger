from pymongo import MongoClient, ASCENDING, UpdateOne
from pymongo.errors import BulkWriteError

class Uploader:
    def __init__(self, language: str, threshold: int, dbname: str):
        self.client = MongoClient()
        self.language = language
        self.database = self.client.get_database(dbname)
        self.collection = self.database.get_collection(language)

        self.buffer = []
        self.threshold = threshold

    def upload(self) -> None:
        if self.buffer:
            self.collection.insert_many(self.buffer)
            self.buffer = []
        
    def append(self, person: dict[str, str]) -> None:
        self.buffer.append(person)

        if len(self.buffer) == self.threshold:
            self.upload()

    def destroy(self) -> None:
        self.buffer = {}
        self.client.close()
