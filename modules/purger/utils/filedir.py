from pathlib import Path

from ...utils import logger as log


class FileDir:
    scope = 'FileDir'

    def __check_path_is_dir(self, path: Path) -> None:
        if not path.is_dir():
            txt = f'Datasets path is not a directory {path}'
            log.err(txt, scope=self.scope)
            raise Exception('Datasets path is not a directory', path)

    def __get_lang_from_dirname(self, dirname: str) -> str:
        try:
            return dirname.split('_')[0]
        except Exception as err:
            raise Exception(f'Dirname {dirname} is not properly formatted (es. ITA_Italia)') from err

    def __fetch_langs_from_datasets(self) -> list[dict]:
        lang_paths: list[Path] = sorted([dir for dir in self.root.iterdir() if dir.is_dir()])
        return [
            {'path': lang_path, 'lang': self.__get_lang_from_dirname(lang_path.stem), 'fullname': lang_path.stem}
            for lang_path in lang_paths
        ]

    def __get_lang_obj_from_lang(self, lang: str) -> dict:
        try:
            return next(filter(lambda el: el['lang'].lower() == lang.lower(), self.langs))
        except Exception as err:
            txt = f'Dataset lang {lang} not found'
            log.err(txt, scope=self.scope)
            raise Exception(txt) from err

    def __init__(self, datasets_path: Path):
        # check that the given path is a dir
        self.__check_path_is_dir(datasets_path)
        # assign it to the root
        self.root = datasets_path
        # assigns the langs inspecting the datasets dir
        self.langs = self.__fetch_langs_from_datasets()

    def retrieve_langs(self) -> list[Path]:
        return [lang_path['lang'] for lang_path in self.langs]

    def retrieve_lang_assets(self, lang: str) -> list[Path]:
        lang_obj = self.__get_lang_obj_from_lang(lang)
        lang_path = lang_obj['path']
        return sorted([file for file in lang_path.iterdir() if file.is_file() and file.suffix == '.bz2'], key=lambda f: f.name)

    def retrieve_lang_fullname(self, lang: str) -> list[Path]:
        lang_obj = self.__get_lang_obj_from_lang(lang)
        return lang_obj['fullname']
