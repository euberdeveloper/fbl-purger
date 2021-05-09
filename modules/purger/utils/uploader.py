from pymongo import MongoClient, ASCENDING
from ...utils.logger import log

class Uploader:
    def __check_collection_already_exists(self, force: bool) -> None:
        if self.collection.count != 0:
            if force:
                log(f'{self.language} already exists: dropping')
                self.collection.drop()    

    def __add_unique_line_index(self) -> None:
        self.collection.create_index([('line', ASCENDING)], name='lineIndex', unique=True)

    def __init__(self, language: str, threshold: int, dbname: str, force: bool):
        # Open connection and get collection
        self.client = MongoClient()
        self.language = language
        self.database = self.client.get_database(dbname)
        self.collection = self.database.get_collection(language)

        # Initialize buffer and other fields
        self.buffer = []
        self.threshold = threshold

        # Check if collection already exists
        self.__check_collection_already_exists(force)

        # Add unique index to collection for field "line"
        self.__add_unique_line_index() 

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
