from pymongo import MongoClient, ASCENDING, UpdateOne
from pymongo.errors import BulkWriteError

class Uploader:
    def __init__(self, language: str, threshold: int, dbname: str):
        self.client = MongoClient()
        self.language = language
        self.database = self.client.get_database(dbname)
        self.collection = self.database.get_collection(language)
        self.collection.create_index([('facebookId', ASCENDING)], name='fidIndex', unique=True)

        self.buffer = {}
        self.threshold = threshold

    def handle_inserts_errors(self, error: BulkWriteError) -> None:
        bulk_updates = [
            UpdateOne(
                err['keyValue'], 
                {'$push': {'telephone': {'$each': err['op']['telephone']}}}
            ) 
            for err in error.details['writeErrors'] 
            if err['code'] == 11000
        ]
        
        self.collection.bulk_write(bulk_updates)


    def upload(self) -> None:
        if self.buffer:
            persons = self.buffer.values()
            try:
                self.collection.insert_many(persons, ordered=False)
            except BulkWriteError as err:
                self.handle_inserts_errors(err)
            finally:
                self.buffer = {}
        
    def append(self, person: dict[str, str]) -> None:
        fid = person['facebookId']
        if fid in self.buffer:
            self.buffer[fid]['telephone'] += person['telephone']
        else:
            self.buffer[fid] = person

        if len(self.buffer) == self.threshold:
            self.upload()

    def destroy(self) -> None:
        self.buffer = {}
        self.client.close()
