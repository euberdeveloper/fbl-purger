import bz2
from pathlib import Path
from joblib import Parallel, delayed

from ..utils import logger as log
from .utils.defaults import DEFAULT_JUMP_LINES, DEFAULT_SRC, DEFAULT_LANGS, DEFAULT_DBNAME, DEFAULT_THRESHOLD, DEFAULT_BIAS, DEFAULT_PARALLEL, DEFAULT_PROCESSES, DEFAULT_FORCE, DEFAULT_SKIP, DEFAULT_OCTOPUS, DEFAULT_NAZI, DEFAULT_SKIP_FIRST_LINE, DEFAULT_WIDE
from .utils.filedir import FileDir
from .utils.uploader import Uploader
from .utils.parser import Parser


class Purger:

    def __set_fields(self, langs: list[str], dbname: str, threshold: int, bias: int, parallel: bool, processes: int, force: bool, skip: bool, octopus: bool, nazi: bool, skip_first_line: bool, wide: bool, jump_lines: int):
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
        self.skip_first_line = skip_first_line
        self.wide = wide
        self.jump_lines = jump_lines

    def __print_settings(self) -> None:
        print('---------------')
        print(f'Datasets dir is {self.src}')
        print(f'Languages to parse are {" ".join(self.langs)}')
        print(f'Name of db is {self.dbname}')
        print(
            f'Threshold of bufferized profiles before updating is {self.threshold}')
        print(
            f'Bias added to assets of same lang to avoid conflicts for the field line is {self.bias}')
        print(f'Will I try to parallelize? {self.parallel}')
        print(f'If I parallelize, I will use {self.processes} processes')
        print(
            f'If I parallelize, I will parallelize even the assets? {self.octopus}')
        print(
            f'If a collection already exists, will I override it? {self.force}')
        print(f'If a collection already exists, will I skip it? {self.skip}')
        print(f'If a line fails, will I terminate the program? {self.nazi}')
        print(f'Skip first line: {self.skip_first_line}')
        print(f'Will consider also txt files: {self.wide}')
        print('---------------')

    def __purge(self) -> None:
        if self.parallel:
            if self.octopus:
                Parallel(n_jobs=self.processes)(
                    delayed(self._purge_asset)(lang, asset, index)
                    for lang in self.langs
                    for index, asset in enumerate(self.filedir.retrieve_lang_assets(lang, self.wide))
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

        lang_full_name = self.filedir.retrieve_lang_fullname(lang)
        uploader = Uploader(lang_full_name, asset.name,
                            self.threshold, self.dbname, self.force and index == 0)

        try:
            uploader.check_and_add_index()
        except Exception as err:
            if index == 0:
                if self.skip:
                    log.warn('Skipping purging, collection already exists',
                             lang=lang, asset=asset.name)
                    return
                else:
                    log.err('Collection already exists',
                            lang=lang, asset=asset.name)
                    raise err

        with (bz2.open if asset.suffix == '.bz2' else open)(asset, 'rt', encoding='ISO-8859-1') as input_file:
            lines_to_skip = self.jump_lines

            def skip_line():
                nonlocal lines_to_skip
                try:
                    next(input_file)
                except StopIteration:
                    pass
                lines_to_skip -= 1

            # in some langs such as BRA two files split a line
            if index > 0 and self.skip_first_line:
                skip_line()
            # skip lines if needed
            while lines_to_skip > 0:
                skip_line()
            # do the real job
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
        assets_paths = self.filedir.retrieve_lang_assets(lang, self.wide)
        for index, asset_path in enumerate(assets_paths):
            self._purge_asset(lang, asset_path, index)
        log.succ('Finish purging lang', lang=lang)

    def __init__(self, src=DEFAULT_SRC):
        src_path = Path(src)
        self.src = src
        self.filedir = FileDir(src_path)
        self.available_langs = self.filedir.retrieve_langs()

    def purge(self, langs: list[str] = DEFAULT_LANGS, dbname=DEFAULT_DBNAME, threshold=DEFAULT_THRESHOLD, bias=DEFAULT_BIAS, parallel=DEFAULT_PARALLEL, processes=DEFAULT_PROCESSES, force=DEFAULT_FORCE, skip=DEFAULT_SKIP, octopus=DEFAULT_OCTOPUS, nazi=DEFAULT_NAZI, skip_first_line=DEFAULT_SKIP_FIRST_LINE, wide=DEFAULT_WIDE, jump_lines=DEFAULT_JUMP_LINES) -> None:
        self.__set_fields(langs, dbname, threshold, bias, parallel,
                          processes, force, skip, octopus, nazi, skip_first_line, wide, jump_lines)
        self.__print_settings()
        self.__purge()
