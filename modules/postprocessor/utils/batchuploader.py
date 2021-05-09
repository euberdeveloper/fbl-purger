from pymongo.collection import Collection
from ...utils.logger import log

class BatchUploader:
    def __init__(self, threshold: int, collection: Collection):
        self.threshold = threshold
        self.collection = collection
        self.buffer = []

    def flush(self) -> None:
        log(f'Flushing')
        if self.buffer:
            self.collection.insert_many(self.buffer)
            self.buffer = []

    def add(self, profile: dict) -> None:
        self.buffer.append(profile)
        if len(self.buffer) == self.threshold:
            self.flush()