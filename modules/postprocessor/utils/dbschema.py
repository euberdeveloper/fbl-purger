import re
from pymongo import MongoClient
from pymongo.collection import Collection
from typing import Optional


class DbSchema:

    def __retrieve_collections(self) -> list[dict]:
        coll_names = sorted(self.database.list_collection_names())
        coll_name_regex = r'^[A-Z]{3}(?:_[A-Za-z]+)+$'
        coll_name_parsed_regex = r'_parsed$'
        return [
            {
                'lang': coll_name.split('_')[0],
                'fullname': coll_name,
                'collection': self.database.get_collection(coll_name),
                'is_parsed': True if re.search(coll_name_parsed_regex, coll_name) else False
            }
            for coll_name in coll_names
            if re.search(coll_name_regex, coll_name)
        ]

    def __get_coll_from_lang(self, lang: str, parsed: bool) -> Optional[dict]:
        try:
            return next(filter(lambda el: el['is_parsed'] == parsed and el['lang'].lower() == lang.lower(), self.collections))
        except Exception:
            return None

    def __init__(self, dbname: str):
        self.client = MongoClient()
        self.database = self.client.get_database(dbname)
        self.collections = self.__retrieve_collections()

    def retrieve_langs(self) -> list[str]:
        return [
            coll['lang']
            for coll in self.collections
            if not coll['is_parsed']
        ]

    def retrieve_lang_full_name(self, lang: str) -> Optional[str]:
        lang_obj = self.__get_coll_from_lang(lang, False)
        return lang_obj['fullname'] if lang_obj else None

    def retrieve_lang_raw_coll(self, lang: str) -> Optional[Collection]:
        lang_obj = self.__get_coll_from_lang(lang, False)
        return lang_obj['collection'] if lang_obj else None

    def retrieve_lang_parsed_coll(self, lang: str) -> Optional[Collection]:
        lang_obj = self.__get_coll_from_lang(lang, True)
        return lang_obj['collection'] if lang_obj else None

    def create_lang_parsed_coll(self, lang: str) -> Optional[Collection]:
        full_name = self.retrieve_lang_full_name(lang)
        parsed_coll = self.database.get_collection(f'{full_name}_parsed')
        self.collections.append(parsed_coll)
        return parsed_coll

    def destroy(self) -> None:
        self.client.close()
