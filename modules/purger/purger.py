import bz2
from pathlib import Path
from joblib import Parallel, delayed
from typing import Optional

from ..utils.logger import log
from .utils.defaults import DEFAULT_SRC, DEFAULT_LANGS, DEFAULT_DBNAME, DEFAULT_THRESHOLD, DEFAULT_PARALLEL, DEFAULT_THREADS, DEFAULT_FORCE, DEFAULT_SKIP
from .utils.filedir import FileDir
from .utils.uploader import Uploader
from .utils.parser import parse_line

def _print_settings(src: Path, langs: list[str], dbname: str, threshold: int, parallel: bool, threads: int) -> None:
    print('---------------')
    print(f'Datasets dir is {src}')
    print(f'Languages to parse are {" ".join(langs)}')
    print(f'Name of db is {dbname}')
    print(f'Threshold before updating is {threshold}')
    print(f'Will I try to parallelize? {parallel}')
    print(f'If I parallelize, I will use {threads} processes')
    print('---------------')

def _purge_asset(path: Path, uploader: Uploader) -> None:
    with bz2.open(path, 'rt') as input_file:
        for line in input_file:
            line = line.rstrip('\n')
            person = parse_line(line)
            uploader.append(person)
        uploader.upload()

def _purge_lang(lang: str, threshold: int, dbname: str, force: bool, skip: bool, filedir: FileDir) -> None:
    log(f'Start purge lang {lang}')
    assets = filedir.retrieve_lang_assets(lang)
    lang_full_name = filedir.retrieve_lang_fullname(lang)

    try:
        uploader = Uploader(lang_full_name, threshold, dbname, force)
    except Exception as err:
        if skip:
            log(str(err))
            return
        else:
            raise err

    for asset in assets:
        log(f'Start purge lang {lang} asset {asset.name}')
        _purge_asset(asset, uploader)
        log(f'Finish purge lang {lang} asset {asset.name}')

    uploader.destroy()
    log(f'Finish purge lang {lang}')


def purge(src = DEFAULT_SRC, langs: list[str] = DEFAULT_LANGS, dbname = DEFAULT_DBNAME, threshold = DEFAULT_THRESHOLD, parallel = DEFAULT_PARALLEL, threads = DEFAULT_THREADS, force = DEFAULT_FORCE, skip = DEFAULT_SKIP) -> None:
    src_path = Path(src)
    _print_settings(src_path, langs, dbname, threshold, parallel, threads)

    filedir = FileDir(src_path)
    langs = filedir.retrieve_langs() if 'all' in langs else langs

    if parallel:
        Parallel(n_jobs=threads)(delayed(_purge_lang)(lang, threshold, dbname, force, skip, filedir) for lang in langs)
    else:
        for lang in langs:
            _purge_lang(lang, threshold, dbname, force, skip, filedir)


def langs(src = DEFAULT_SRC) -> list[str]:
    src_path = Path(src)
    filedir = FileDir(src_path)
    return filedir.retrieve_langs()
    
