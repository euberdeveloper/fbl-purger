from joblib import Parallel, delayed
from typing import Optional

from ..utils import logger as log
from .utils.dbschema import DbSchema
from .utils.dbprocessor import fetch_processed_data, upload_processed_data
from .utils.defaults import DEFAULT_LANGS, DEFAULT_DBNAME, DEFAULT_THRESHOLD, DEFAULT_PARALLEL, DEFAULT_THREADS, DEFAULT_FORCE, DEFAULT_SKIP, DEFAULT_OCTOPUS, DEFAULT_NAZI

def _print_settings(langs: list[str], dbname: str, threshold: int, parallel: bool, threads: int) -> None:
    print('---------------')
    print(f'Languages to parse are {" ".join(langs)}')
    print(f'Name of db is {dbname}')
    print(f'Threshold before updating is {threshold}')
    print(f'Will I try to parallelize? {parallel}')
    print(f'If I parallelize, I will use {threads} processes')
    print('---------------')

def _process_lang(lang: str, threshold: int, force: bool, skip: bool, nazi: bool, dbschema: DbSchema) -> None:
    log(f'Start process lang {lang}')

    raw_coll = dbschema.retrieve_lang_raw_coll(lang)
    if raw_coll is None:
        log(f'{lang} raw collection not found')
        if nazi:
            raise Exception(f'{lang} raw collection not found')

    parsed_coll = dbschema.retrieve_lang_parsed_coll(lang)
    if parsed_coll is not None:
        if force:
            log(f'{lang} parsed collection already exists, dropping')
            parsed_coll.drop()
        elif skip:
            log(f'{lang} parsed collection already exists, skipping')
            return
        elif nazi:
            log(f'{lang} parsed collection already exists')
            raise Exception(f'{lang} parsed collection already exists')
    else:
        parsed_coll = dbschema.create_lang_parsed_coll(lang)

    log(f'Start fetch lang {lang}')
    profiles = fetch_processed_data(raw_coll)
    log(f'Stop fetch lang {lang}')

    log(f'Start upload lang {lang}')
    upload_processed_data(parsed_coll, profiles, threshold)
    log(f'Stop upload lang {lang}')

    log(f'End process lang {lang}')
    
def _process_lang_with_new_dbschema(lang: str, threshold: int, force: bool, skip: bool, nazi: bool, dbname: str):
    dbschema = DbSchema(dbname)
    _process_lang(lang, threshold, force, skip, nazi, dbschema)
    dbschema.destroy()

def process(langs: list[str] = DEFAULT_LANGS, dbname = DEFAULT_DBNAME, threshold = DEFAULT_THRESHOLD, parallel = DEFAULT_PARALLEL, threads = DEFAULT_THREADS, force = DEFAULT_FORCE, skip = DEFAULT_SKIP, nazi = DEFAULT_NAZI) -> None:
    _print_settings(langs, dbname, threshold, parallel, threads)

    dbschema = DbSchema(dbname)
    langs = dbschema.retrieve_langs() if 'all' in langs else langs

    if parallel:
        Parallel(n_jobs=threads)(delayed(_process_lang_with_new_dbschema)(lang, threshold, force, skip, nazi, dbname) for lang in langs)
    else:
        for lang in langs:
            _process_lang(lang, threshold, force, skip, nazi, dbschema)

    dbschema.destroy()

def langs(dbname = DEFAULT_DBNAME) -> list[str]:
    dbschema = DbSchema(dbname)
    langs = dbschema.retrieve_langs()
    dbschema.destroy()
    return langs