from pathlib import Path
from typing import Optional

from .utils.defaults import DEFAULT_SRC, DEFAULT_LANGS, DEFAULT_DBNAME, DEFAULT_THRESHOLD, DEFAULT_PARALLEL, DEFAULT_THREADS
from .utils.filedir import FileDir

def _print_settings(src: Path, langs: list[str], dbname: str, threshold: int, parallel: bool, threads: int) -> None:
    print('---------------')
    print(f'Datasets dir is {src}')
    print(f'Languages to parse are {" ".join(langs)}')
    print(f'Name of db is {dbname}')
    print(f'Threshold before updating is {threshold}')
    print(f'Will I try to parallelize? {parallel}')
    print(f'If I parallelize, I will use {threads} processes')
    print('---------------')


def purge(src = DEFAULT_SRC, langs: list[str] = DEFAULT_LANGS, dbname = DEFAULT_DBNAME, threshold = DEFAULT_THRESHOLD, parallel = DEFAULT_PARALLEL, threads = DEFAULT_THREADS) -> None:
    src_path = Path(src)
    _print_settings(src_path, langs, dbname, threshold, parallel, threads)

def langs(src = DEFAULT_SRC) -> None:
    src_path = Path(src)
    filedir = FileDir(src_path)
    langs = filedir.retrieve_langs()
    langs_list = "\n".join(langs)
    print(f'Available langs are: \n{langs_list}')
