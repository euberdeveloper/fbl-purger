from pathlib import Path
from typing import Optional, Union

class FileDir:
    def __get_lang_from_dirname(self, dirname: str) -> str:
        try:
            return dirname.split('_')[0]
        except err:
            raise Exception(f'Dirname {dirname} is not properly formatted (es. ITA_Italia)') from err

    def __retrieve_langs_paths(self) -> list[dict]:
        lang_paths: list[Path] = sorted([dir for dir in self.root.iterdir() if dir.is_dir()])
        return [
            { 'path': lang_path, 'lang': self.__get_lang_from_dirname(lang_path.stem)} 
            for lang_path in lang_paths
        ]

    def __init__(self, datasets_path: Path):
        if not datasets_path.is_dir():
            raise Exception('Datasets path is not a directory', datasets_path)

        self.root = datasets_path
        self.langs_paths = self.__retrieve_langs_paths()

    def retrieve_langs(self) -> list[Path]:
        return [lang_path['lang'] for lang_path in self.langs_paths]

    def retrieve_lang_assets(self, lang: str) -> list[Path]:
        try:
            lang_path: Path = next(filter(lambda el: el['lang'].lower() == lang.lower(), self.langs_paths))['path']
        except err:
             raise Exception(f'Language {lang} not found') from err

        return sorted([file for file in lang_path.iterdir() if file.is_file() and file.suffix == '.bz2'])
