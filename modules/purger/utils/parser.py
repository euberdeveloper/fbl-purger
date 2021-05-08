import re
from pathlib import Path
from json import loads
from datetime import datetime, date
from typing import Optional

# from ...utils.logger import log

REGEXPS = {
    'numeric': r'(?:[\d])',
    'char': r'(?:[A-Za-z])',
    'uchar': r'(?:[\p{L}\p{M}*])',
    'whole': r'(?:[^:])',
    'datetime': r'(?:\d{1,2}\/\d{1,2}\/\d{1,4} \d{1,2}:\d{1,2}:\d{1,2} (?:AM|PM))',
    'date': r'(?:(?:\d{1,2})(?:\/\d{1,2})?(?:\/\d{1,4})?)'
}

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
        return '^' + ':'.join([
            rf'(?P<{prop}>{self.__regex_from_details(prop, details)})'
            for prop, details in schema.items()
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
            month = parts[0]
            day = parts[1]
            try:
                year = parts[2]
            except:
                year = 1
            return date(year, month, day)
        # log(f'Unrecognized type {vtype}')
        

    def __parse_matched(self, matched: dict, index: int) -> dict:
        result = {
            prop : self.__parse_value(matched[prop], details['type'])
            for prop, details in self.schema.items()
        }
        result['line'] = index
        return result

    def __parse_line(self, line: str) -> Optional[dict]:
        matched = re.match(self.regex, line)
        return None if matched is None else matched.groupdict()

    def __init__(self,  lang: str):
        self.lang = lang
        self.schema = self.__fetch_schemas(lang)
        self.regex = self.__compute_regex(self.schema)
        self.failed_line = None

    def parse_line(self, index: int, line: str) -> Optional[dict]:
        extracted = self.__parse_line(line)

        if extracted is None:
            if self.failed_line:
                whole_line = self.failed_line + line
                extracted = self.__parse_line(whole_line)
                self.failed_line = line if extracted is None else None
            else:
                self.failed_line = line
        elif self.failed_line:
            # log(f'Failed parsing line at index {index}')
            pass

        return None if extracted is None else self.__parse_matched(extracted, index)

p = Parser('it')
line_1 = '244911787254:100004307984155:Marcelina Elizeth Cavelho:Cavelho:เพศหญิง:ลูอันดา::::1/1/0001 12:00:00 AM::'
line = line_1
res = p.parse_line(1, line)
if res:
    print(res)
else:
    res = p.parse_line(1, line)