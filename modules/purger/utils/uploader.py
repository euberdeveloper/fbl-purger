from pymongo import MongoClient, ASCENDING
from ...utils import logger as log

class Uploader:
    def __check_collection_already_exists(self, force: bool) -> None:
        db_collections = self.database.list_collection_names()
        if self.collection.name in db_collections:
            if force:
                log.warn(f'Language already exists: dropping', lang=self.language, asset=self.asset)
                self.collection.drop()
            else:
                raise Exception(f'{self.language} already exists')

    def __add_unique_line_index(self) -> None:
        self.collection.create_index([('line', ASCENDING)], name='lineIndex', unique=True)

    def __init__(self, language: str, asset: str, threshold: int, dbname: str, force: bool):
        # Open connection and get collection
        self.client = MongoClient()
        self.language = language
        self.database = self.client.get_database(dbname)
        self.collection = self.database.get_collection(language)

        # Initialize buffer and other fields
        self.buffer = []
        self.threshold = threshold
        self.asset = asset

        # Check if collection already exists
        self.__check_collection_already_exists(force)

        # Add unique index to collection for field "line"
        self.__add_unique_line_index() 

    def upload(self) -> None:
        if self.buffer:
            n_elements = len(self.buffer)
            log.debug(f'Uploading {n_elements} elements', lang=self.language, asset=self.asset)
            self.collection.insert_many(self.buffer)
            self.buffer = []
            log.debug(f'Uploaded {n_elements} elements', lang=self.language, asset=self.asset)
        
    def append(self, person: dict[str, str]) -> None:
        self.buffer.append(person)

        if len(self.buffer) == self.threshold:
            self.upload()

    def destroy(self) -> None:
        self.buffer = []
        self.client.close()
