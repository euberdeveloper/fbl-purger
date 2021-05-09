import re
from pathlib import Path
from pymongo import MongoClient
from typing import Optional, Union

class DbHandler:

    def __retrieve_collections(self) -> list[dict]:
        coll_names = sorted(self.database.list_collection_names())
        coll_name_regex = r'^[A-Z]{3}(?:_[A-Za-z]+)+$'
        coll_name_parsed_regex = r'_parsed$'
        return [
            { 
                'lang': coll_name.split('_')[0],
                'fullname': coll_name,
                'collection': self.database.get_collection(coll_name),
                'is_parsed': re.search(coll_name_parsed_regex, coll_name)
            }
            for coll_name in coll_names
            if re.search(coll_name_regex, coll_name)
        ]

    def __get_coll_from_lang(self, lang: str, parsed: bool):
        try:
            return next(filter(lambda el: el['parsed'] == parsed and el['lang'].lower() == lang.lower(), self.collections))
        except Exception as err:
             raise Exception(f'Language {lang} not found') from err
   
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

    def retrieve_lang_raw_coll(self, lang: str) -> list[Path]:
        lang_obj = self.__get_coll_from_lang(lang, False)
        return lang_obj['fullname']

    def retrieve_lang_parsed_coll(self, lang: str) -> list[Path]:
        lang_obj = self.__get_coll_from_lang(lang, True)
        return lang_obj['fullname']

    def destroy(self) -> None:
        self.client.close()
        