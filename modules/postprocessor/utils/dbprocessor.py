from pymongo.collection import Collection, CommandCursor
from .batchuploader import BatchUploader

def fetch_processed_data(collection: Collection) -> CommandCursor:
    sort_by_line = {
        '$sort': {
            'line': 1
        }
    }
    group_by_fid = {
        '$group': {
            '_id': '$fid', 
            'current': {
                '$last': '$$ROOT'
            }, 
            'all': {
                '$push': '$$ROOT'
            }
        }
    }
    handle_history = {
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
    }
    flat_current = {
        '$replaceRoot': {
            'newRoot': {
                '$mergeObjects': [
                    '$current', {
                        'history': '$history'
                    }
                ]
            }
        }
    }
    remove_id_and_line = {
        '$project': {
            '_id': False, 
            'line': False,
            'history._id': False,
            'history.line': False
        }
    }
    return collection.aggregate([sort_by_line, group_by_fid, handle_history, flat_current, remove_id_and_line], allowDiskUse=True)

def upload_processed_data(collection: Collection, data: CommandCursor, threshold: int):
    uploader = BatchUploader(threshold, collection)
    for profile in data:
        uploader.add(profile)
    uploader.flush()