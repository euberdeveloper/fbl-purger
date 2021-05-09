import re
from pathlib import Path
from json import loads
from datetime import datetime
from typing import Optional

from ...utils import logger as log

# regexps to parse the lines
REGEXPS = {
    'numeric': r'(?:[\d])',
    'phone': r'(?:[\+\d])',
    'char': r'(?:[A-Za-z])',
    'uchar': r'(?:[\p{L}\p{M}*])',
    'whole': r'(?:[^:])',
    'whole_comma': r'(?:[^,])',
    'datetime': r'(?:\d{1,2}\/\d{1,2}\/\d{1,4} \d{1,2}:\d{1,2}:\d{1,2} (?:AM|PM))',
    'date': r'(?:(?:\d{1,2})(?:\/\d{1,2})?(?:\/\d{1,4})?)'
}
# if more than n subsequent lines fail, something is not working
MAX_FAILURES = 10

class Parser:
    def __fetch_schemas(self, lang: str) -> dict:
        with open(Path(__file__).parent.joinpath('schemas.json').absolute()) as schemas_file:
            text = schemas_file.read()
            schemas = loads(text)
            return schemas[lang] if lang in schemas else schemas['default']

    def __regex_from_details(self, prop: str, details: dict) -> str:
            body = REGEXPS[details['regex']]
            multiplier = '*' if details['optional'] else '+'
            return rf'{body}{multiplier}'

    def __compute_regex(self, schema: dict) -> str:
        props = schema['props']
        separator = schema['separator']
        return '^' + separator.join([
            rf'(?P<{prop}>{self.__regex_from_details(prop, details)})'
            for prop, details in props.items()
        ]) + '$'

    def __parse_value(self, value: str, vtype: str):
        if not value:
            return None
        if vtype == 'string':
            return value
        if vtype == 'number':
            return int(value)
        if vtype == 'datetime':
            return datetime.strptime(value, "%m/%d/%Y %H:%M:%S %p")
        if vtype == 'date':
            parts = value.split('/')
            month = int(parts[0])
            day = int(parts[1])
            try:
                year = int(parts[2])
            except:
                # It has to be bisestile or it raises an error for 29/02 :)
                year = 12
            return datetime(year, month, day)
        log.warn(f'Unrecognized type {vtype}', lang=self.lang, asset=self.asset)
        

    def __parse_matched(self, matched: dict, index: int) -> dict:
        result = {
            prop : self.__parse_value(matched[prop], details['type'])
            for prop, details in self.schema['props'].items()
            if details['keep']
        }
        result['line'] = index
        return result

    def __parse_line(self, line: str) -> Optional[dict]:
        matched = re.match(self.regex, line)
        return None if matched is None else matched.groupdict()

    def __init__(self,  lang: str, asset: str, nazi: bool):
        self.lang = lang
        self.asset = asset
        self.nazi = nazi

        self.schema = self.__fetch_schemas(lang)
        self.regex = self.__compute_regex(self.schema)
        
        self.failed_line = None
        self.subseq_failures = 0

    def parse_line(self, index: int, line: str) -> Optional[dict]:
        extracted = self.__parse_line(line)

        if extracted is None:
            if self.failed_line:
                whole_line = self.failed_line + line
                extracted = self.__parse_line(whole_line)
                self.failed_line = whole_line if extracted is None else None
                self.subseq_failures = self.subseq_failures + 1 if extracted is None else 0
            else:
                self.failed_line = line
                self.subseq_failures += 1
            if self.subseq_failures > MAX_FAILURES:
                if self.nazi:
                    txt = f'Too many lines failed ({self.subseq_failures})'
                    log.err(txt, lang=self.lang, asset=self.asset)
                    raise Exception(txt)
                else:
                    log.warn(f'{self.subseq_failures} subsequent failures, file is probably nonsense', lang=self.lang, asset=self.asset)
                    self.failed_line = None
        elif self.failed_line:
            txt = f'Failed parsing line at (biased) index {index}'
            if self.nazi:
                log.err(txt)
                raise Exception(f'Failed parsing {self.lang} line at index {index}')
            else:
                log.warn(txt)
            self.failed_line = None

        return None if extracted is None else self.__parse_matched(extracted, index)
