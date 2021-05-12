from joblib import Parallel, delayed

from ..utils import logger as log
from .utils.dbschema import DbSchema
from .utils.dbprocessor import DbProcessor
from .utils.defaults import DEFAULT_LANGS, DEFAULT_DBNAME, DEFAULT_THRESHOLD, DEFAULT_PARALLEL, DEFAULT_PROCESSES, DEFAULT_FORCE, DEFAULT_SKIP, DEFAULT_NAZI


class Postprocessor:

    def __set_fields(self, langs: list[str], threshold: int, parallel: bool, processes: int, force: bool, skip: bool, nazi: bool):
        self.langs = self.available_langs if 'all' in langs else langs
        self.threshold = threshold
        self.parallel = parallel
        self.processes = processes
        self.force = force
        self.skip = skip
        self.nazi = nazi

    def __print_settings(self) -> None:
        print('---------------')
        print(f'Languages to parse are {" ".join(self.langs)}')
        print(f'Name of db is {self.dbname}')
        print(
            f'Threshold of bufferized profiles before updating is {self.threshold}')
        print(f'Will I try to parallelize? {self.parallel}')
        print(f'If I parallelize, I will use {self.processes} processes')
        print(
            f'If a parsed collection already exists, will I override it? {self.force}')
        print(
            f'If a parsed collection already exists, will I skip it? {self.skip}')
        print(f'If a line fails, will I terminate the program? {self.nazi}')
        print('---------------')

    def __process(self) -> None:
        if self.parallel:
            Parallel(n_jobs=self.processes)(
                delayed(self._process_lang)(lang)
                for lang in self.langs
            )
        else:
            for lang in self.langs:
                self._process_lang(lang)

    def _process_lang(self, lang: str) -> None:
        log.info('Start processing lang', lang=lang)
        dbschema = DbSchema(self.dbname)

        raw_coll = dbschema.retrieve_lang_raw_coll(lang)
        if raw_coll is None:
            txt = 'Raw collection not found'
            log.err(txt)
            raise Exception(txt)

        parsed_coll = dbschema.retrieve_lang_parsed_coll(lang)
        if parsed_coll is not None:
            if self.force:
                log.warn('Parsed collection already exists, dropping', lang=lang)
                parsed_coll.drop()
            elif self.skip:
                log.warn('Parsed collection already exists, skipping', lang=lang)
                return
            elif self.nazi:
                txt = 'Parsed collection already exists'
                log.err(txt, lang=lang)
                raise Exception(txt)
        else:
            parsed_coll = dbschema.create_lang_parsed_coll(lang)

        dbprocessor = DbProcessor(lang, self.threshold, raw_coll, parsed_coll)
        dbprocessor.lavora()

        dbschema.destroy()
        log.succ('Finish processing lang', lang=lang)

    def __init__(self, dbname=DEFAULT_DBNAME):
        self.dbname = dbname

        dbschema = DbSchema(dbname)
        self.available_langs = dbschema.retrieve_langs()

    def process(self, langs: list[str] = DEFAULT_LANGS, threshold=DEFAULT_THRESHOLD, parallel=DEFAULT_PARALLEL, processes=DEFAULT_PROCESSES, force=DEFAULT_FORCE, skip=DEFAULT_SKIP, nazi=DEFAULT_NAZI) -> None:
        self.__set_fields(langs, threshold, parallel, processes, force, skip, nazi)
        self.__print_settings()
        self.__process()
