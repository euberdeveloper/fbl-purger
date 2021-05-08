import bz2
from pathlib import Path
from joblib import Parallel, delayed
from typing import Optional

from ..utils.logger import log
from .utils.defaults import DEFAULT_SRC, DEFAULT_LANGS, DEFAULT_DBNAME, DEFAULT_THRESHOLD, DEFAULT_PARALLEL, DEFAULT_THREADS, DEFAULT_FORCE, DEFAULT_SKIP, DEFAULT_OCTOPUS, DEFAULT_NAZI
from .utils.filedir import FileDir
from .utils.uploader import Uploader
from .utils.parser import Parser

ASSET_BIAS = int(100e6)

def _print_settings(src: Path, langs: list[str], dbname: str, threshold: int, parallel: bool, threads: int) -> None:
    print('---------------')
    print(f'Datasets dir is {src}')
    print(f'Languages to parse are {" ".join(langs)}')
    print(f'Name of db is {dbname}')
    print(f'Threshold before updating is {threshold}')
    print(f'Will I try to parallelize? {parallel}')
    print(f'If I parallelize, I will use {threads} processes')
    print('---------------')

def _purge_asset(path: Path, uploader: Uploader, parser: Parser, bias: int) -> None:
    with bz2.open(path, 'rt') as input_file:
        # in some langs such as bra two files split a line
        if bias != 0:
            next(input_file)
        for index, line in enumerate(input_file):
            line = line.rstrip('\n')
            profile = parser.parse_line(bias + index, line)
            if profile:
                uploader.append(profile)
        uploader.upload()

def _purge_lang(lang: str, threshold: int, dbname: str, force: bool, skip: bool, nazi: bool, filedir: FileDir) -> None:
    log(f'Start purge lang {lang}')
    assets = filedir.retrieve_lang_assets(lang)
    lang_full_name = filedir.retrieve_lang_fullname(lang)
    parser = Parser(lang, nazi)

    try:
        uploader = Uploader(lang_full_name, threshold, dbname, force)
    except Exception as err:
        if skip:
            log(str(err))
            return
        else:
            raise err

    for index, asset in enumerate(assets):
        log(f'Start purge lang {lang} asset {asset.name}')
        _purge_asset(asset, uploader, parser, ASSET_BIAS * index)
        log(f'Finish purge lang {lang} asset {asset.name}')

    uploader.destroy()
    log(f'Finish purge lang {lang}')

def _purge_asset_of_lang(lang: str, lang_full_name: str, asset: Path, threshold: int, dbname: str, force: bool, skip: bool, nazi: bool, bias: int) -> None:
    log(f'Start purge lang {lang} asset {asset.name}')
    parser = Parser(lang, nazi)

    try:
        uploader = Uploader(lang_full_name, threshold, dbname, force and bias == 0)
    except Exception as err:
        if skip and bias != 0:
            log(str(err))
            return
        else:
            raise err

    _purge_asset(asset, uploader, parser, bias)

    uploader.destroy()
    log(f'Finish purge lang {lang} asset {asset.name}')


def purge(src = DEFAULT_SRC, langs: list[str] = DEFAULT_LANGS, dbname = DEFAULT_DBNAME, threshold = DEFAULT_THRESHOLD, parallel = DEFAULT_PARALLEL, threads = DEFAULT_THREADS, force = DEFAULT_FORCE, skip = DEFAULT_SKIP, octopus = DEFAULT_OCTOPUS, nazi = DEFAULT_NAZI) -> None:
    src_path = Path(src)
    _print_settings(src_path, langs, dbname, threshold, parallel, threads)

    filedir = FileDir(src_path)
    langs = filedir.retrieve_langs() if 'all' in langs else langs

    if parallel:
        if octopus:
            assets_gears = [
                (lang, filedir.retrieve_lang_fullname(lang), asset, threshold, dbname, force, skip, nazi, ASSET_BIAS * index)
                for lang in langs
                for index, asset in enumerate(filedir.retrieve_lang_assets(lang))
            ]
            Parallel(n_jobs=threads)(delayed(_purge_asset_of_lang)(*gear) for gear in assets_gears)
        else:
            Parallel(n_jobs=threads)(delayed(_purge_lang)(lang, threshold, dbname, force, skip, nazi, filedir) for lang in langs)
    else:
        for lang in langs:
            _purge_lang(lang, threshold, dbname, force, skip, nazi, filedir)


def langs(src = DEFAULT_SRC) -> list[str]:
    src_path = Path(src)
    filedir = FileDir(src_path)
    return filedir.retrieve_langs()
    
