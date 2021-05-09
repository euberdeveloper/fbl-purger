import bz2
from pathlib import Path
from joblib import Parallel, delayed
from typing import Optional

from ..utils import logger as log
from .utils.defaults import DEFAULT_SRC, DEFAULT_LANGS, DEFAULT_DBNAME, DEFAULT_THRESHOLD, DEFAULT_BIAS, DEFAULT_PARALLEL, DEFAULT_PROCESSES, DEFAULT_FORCE, DEFAULT_SKIP, DEFAULT_OCTOPUS, DEFAULT_NAZI
from .utils.filedir import FileDir
from .utils.uploader import Uploader
from .utils.parser import Parser

class Purger:

    def __set_fields(self, langs: list[str], dbname: str, threshold: int, bias: int, parallel: bool, processes: int, force: bool, skip: bool, octopus: bool, nazi: bool):
        self.langs = self.available_langs if 'all' in langs else langs
        self.dbname = dbname
        self.threshold = threshold
        self.bias = bias
        self.parallel = parallel
        self.processes = processes
        self.force = force
        self.skip = skip
        self.octopus = octopus
        self.nazi = nazi

    def __print_settings(self) -> None:
        print('---------------')
        print(f'Datasets dir is {self.src}')
        print(f'Languages to parse are {" ".join(self.langs)}')
        print(f'Name of db is {self.dbname}')
        print(f'Threshold of bufferized profiles before updating is {self.threshold}')
        print(f'Bias added to assets of same lang to avoid conflicts for the field line is {self.bias}')
        print(f'Will I try to parallelize? {self.parallel}')
        print(f'If I parallelize, I will use {self.processes} processes')
        print(f'If I parallelize, I will parallelize even the assets? {self.octopus}')
        print(f'If a collection already exists, will I override it? {self.force}')
        print(f'If a collection already exists, will I skip it? {self.skip}')
        print(f'If a line fails, will I terminate the program? {self.nazi}')
        print('---------------')

    def __purge(self) -> None:
        if self.parallel:
            if self.octopus:
                Parallel(n_jobs=self.processes)(
                    delayed(self._purge_asset)(lang, asset, index)
                    for lang in self.langs
                    for index, asset in enumerate(self.filedir.retrieve_lang_assets(lang))
                )
            else:
                Parallel(n_jobs=self.processes)(
                    delayed(self._purge_lang)(lang)
                    for lang in self.langs
                )
        else:
            for lang in self.langs:
                self._purge_lang(lang)

    def _purge_asset(self, lang: str, asset: Path, index: int) -> None:
        log.info('Start purging asset', lang=lang, asset=asset.name)

        bias = self.bias * index
        parser = Parser(lang, asset.name, self.nazi)

        try:
            lang_full_name = self.filedir.retrieve_lang_fullname(lang)
            uploader = Uploader(lang_full_name, asset.name, self.threshold, self.dbname, self.force)
        except Exception as err:
            if self.skip and index == 0:
                log.warn('Skipping purging, collection already exists', lang=lang, asset=asset.name)
                return
            else:
                log.err('Collection already exists', lang=lang, asset=asset.name)
                raise err

        with bz2.open(asset, 'rt') as input_file:
            # in some langs such as BRA two files split a line
            if bias > 0:
                next(input_file)
            for index, line in enumerate(input_file):
                line = line.rstrip('\n')
                profile = parser.parse_line(bias + index, line)
                if profile:
                    uploader.append(profile)
            uploader.upload()

        uploader.destroy()

        log.succ('Finish purging asset', lang=lang, asset=asset.name)

    def _purge_lang(self, lang: str) -> None:
        log.info('Start purging lang', lang=lang)
        assets_paths = self.filedir.retrieve_lang_assets(lang)
        for index, asset_path in enumerate(assets_paths):
            self._purge_asset(lang, asset_path, index)
        log.succ('Finish purging lang', lang=lang)

    def __init__(self, src = DEFAULT_SRC):
        src_path = Path(src)
        self.src = src
        self.filedir = FileDir(src_path)
        self.available_langs = self.filedir.retrieve_langs()

    def purge(self, langs: list[str] = DEFAULT_LANGS, dbname = DEFAULT_DBNAME, threshold = DEFAULT_THRESHOLD, bias = DEFAULT_BIAS, parallel = DEFAULT_PARALLEL, processes = DEFAULT_PROCESSES, force = DEFAULT_FORCE, skip = DEFAULT_SKIP, octopus = DEFAULT_OCTOPUS, nazi = DEFAULT_NAZI) -> None:
        self.__set_fields(langs, dbname, threshold, bias, parallel, processes, force, skip, octopus, nazi)
        self.__print_settings()
        self.__purge()

