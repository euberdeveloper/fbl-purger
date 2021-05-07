from sys import argv
import bz2
from datetime import datetime
from joblib import Parallel, delayed
import multiprocessing

from utils.uploader import Uploader
from utils.parser import parse_line
from utils.filedir import retrieve_assets_by_lang, retrieve_langs

THRESHOLD = int(1e6)
LANGUAGES = [argv[1]] if len(argv) > 1 else retrieve_langs()
DB_NAME = 'facebookLeaks'

def print_settings() -> None:
    print('---------------')
    print(f'Threshold is {THRESHOLD}')
    print(f'Languages are {LANGUAGES}')
    print(f'Name of db is {DB_NAME}')
    print(f'Parallelize is {PARALLELIZE}')
    print(f'Number of cores are {NUM_CORES}')
    print('---------------')


def analyze_file(path: str, lang: str) -> None:
    uploader = Uploader(lang, THRESHOLD, DB_NAME)
    bz2.open(path, 'rt') as input_file:
        for line in input_file:
            line = line.rstrip('\n')
            person = parse_line(line)
            uploader.append(person)
        uploader.upload()
        uploader.destroy()

def main() -> None:
    print_settings()

    print(f'Start everything', datetime.now().isoformat())
    
    for lang in LANGUAGES:
        print(f'Start language {lang}', datetime.now().isoformat())
        assets = retrieve_assets_by_lang(lang)

        if PARALLELIZE:
            Parallel(n_jobs=NUM_CORES)(delayed(analyze_file)(path, lang) for path in assets)
        else:
            for path in assets:
                print(f'Doing {path}...')
                analyze_file(path, lang)

    print(f'End everything', datetime.now().isoformat())

main()



