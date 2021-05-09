from pymongo import MongoClient
from datetime import datetime

client = MongoClient()
database = client.get_database('fbl')
collection_in = database.get_collection('BRA_Brasile')
collection_out = database.get_collection('BRA_Brasile_parsed')

print(f'Init aggregation', datetime.now().isoformat())
profiles = collection_in.aggregate([
    {
        '$sort': {
            'line': 1
        }
    },
    {
        '$group': {
            '_id': '$fid', 
            'current': {
                '$last': '$$ROOT'
            }, 
            'all': {
                '$push': '$$ROOT'
            }
        }
    }, {
        '$project': {
            'current': '$current', 
            'history': {
                '$cond': [
                    {
                        '$gt': [
                            {
                                '$size': '$all'
                            }, 1
                        ]
                    }, {
                        '$slice': [
                            '$all', 0, {
                                '$subtract': [
                                    {
                                        '$size': '$all'
                                    }, 1
                                ]
                            }
                        ]
                    }, []
                ]
            }
        }
    }, {
        '$replaceRoot': {
            'newRoot': {
                '$mergeObjects': [
                    '$current', {
                        'history': '$history'
                    }
                ]
            }
        }
    }, {
        '$project': {
            '_id': False, 
            'history._id': False
        }
    }
], allowDiskUse=True)
print(f'Aggregation finished', datetime.now().isoformat())

print(f'Init uploading', datetime.now().isoformat())

class BatchUploader:
    def __init__(self, threshold: int):
        self.threshold = threshold
        self.buffer = []

    def flush(self) -> None:
        print(f'Flushing', datetime.now().isoformat())
        if self.buffer:
            collection_out.insert_many(self.buffer)
            self.buffer = []

    def add(self, profile: dict) -> None:
        self.buffer.append(profile)
        if len(self.buffer) == self.threshold:
            self.flush()
        
uploader = BatchUploader(int(1e6))
for profile in profiles:
    uploader.add(profile)
uploader.flush()

print(f'Finish uploading', datetime.now().isoformat())
