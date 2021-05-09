from pymongo.collection import Collection
from ...utils import logger as log

class BatchUploader:
    def __init__(self, lang: str, threshold: int, collection: Collection):
        self.lang = lang
        self.threshold = threshold
        self.collection = collection
        self.buffer = []

    def flush(self) -> None:
        if self.buffer:
            n_elements = len(self.buffer)
            log.debug(f'Uploading {n_elements} elements', lang=self.lang)
            self.collection.insert_many(self.buffer)
            self.buffer = []
            log.debug(f'Uploaded {n_elements} elements', lang=self.lang)

    def add(self, profile: dict) -> None:
        self.buffer.append(profile)
        if len(self.buffer) == self.threshold:
            self.flush()