from pathlib import Path
from typing import Optional, Union

DEFAULT_SOURCE = 'datasets'

def retrieve_langs(source: Optional[Union[str, Path]] = None) -> list[str]:
    source = source or DEFAULT_SOURCE
    source_path = Path(source)

    if not source_path.is_dir():
        raise Exception('Source is not a directory', source)

    return sorted([dir.name for dir in source_path.iterdir() if dir.is_dir()])


def retrieve_assets_by_lang(lang: str, source: Optional[Union[str, Path]] = None) -> list[Path]:
    source = source or DEFAULT_SOURCE
    source_path = Path(source)
    lang_path = source_path.joinpath(lang)

    if not lang_path.is_dir():
        raise Exception('Lang path is not a directory', lang_path)
    
    return sorted([str(file) for file in lang_path.iterdir() if file.is_file()])
