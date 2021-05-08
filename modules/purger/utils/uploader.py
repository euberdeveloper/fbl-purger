from pymongo import MongoClient
from ...utils.logger import log

class Uploader:
    def __init__(self, language: str, threshold: int, dbname: str, force: bool):
        self.client = MongoClient()
        self.language = language
        self.database = self.client.get_database(dbname)
        self.collection = self.database.get_collection(language)

        self.buffer = []
        self.threshold = threshold

        if self.collection.count != 0:
            if force:
                log(f'{self.language} already exists: dropping')
                self.collection.drop()
                self.client.close()
            else:
                raise Exception(f'Collection {self.language} already exists')            

    def upload(self) -> None:
        if self.buffer:
            log(f'Uploading to {self.language} {len(self.buffer)} elements')
            self.collection.insert_many(self.buffer)
            self.buffer = []
        
    def append(self, person: dict[str, str]) -> None:
        self.buffer.append(person)

        if len(self.buffer) == self.threshold:
            self.upload()

    def destroy(self) -> None:
        self.buffer = []
        self.client.close()
