from pymongo.collection import Collection, CommandCursor

from ...utils import logger as log
from .batchuploader import BatchUploader


class DbProcessor:

    def __fetch_processed_data(self) -> CommandCursor:
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
        return self.raw_coll.aggregate([sort_by_line, group_by_fid, handle_history, flat_current, remove_id_and_line], allowDiskUse=True)

    def __upload_processed_data(self, data: CommandCursor):
        uploader = BatchUploader(self.lang, self.threshold, self.parsed_coll)
        for profile in data:
            uploader.add(profile)
        uploader.flush()

    def __init__(self, lang: str, threshold: int, raw_coll: Collection, parsed_coll: Collection):
        self.lang = lang
        self.threshold = threshold
        self.raw_coll = raw_coll
        self.parsed_coll = parsed_coll

    def lavora(self) -> None:
        log.info('Start fetching data', lang=self.lang)
        profiles = self.__fetch_processed_data()
        log.succ('End fetching data', lang=self.lang)
        log.info('Start uploading data', lang=self.lang)
        self.__upload_processed_data(profiles)
        log.succ('End uploading data', lang=self.lang)
