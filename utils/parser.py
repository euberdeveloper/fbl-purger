from datetime import datetime
from typing import Optional

def parse_unknown_date(text: Optional[str]) -> Optional[datetime]:
    try: 
        return datetime.strptime(text, "%m/%d/%Y %H:%M:%S %p")
    except:
        return None

def parse_birthday_date(text: Optional[str]) -> Optional[datetime]:
    try:
        return datetime.strptime(text, "%m/%d/%Y")
    except:
        return None

def preparse_line(line: str) -> list[str]:
    parts = line.split(':')
    length = len(parts)
    
    if length < 12:
        parts = parts + ([''] * (12 - length))
    elif length > 12:
        parts = parts[0:9] + [f'{parts[9]}:{parts[10]}:{parts[11]}'] + parts[12:]
        
    return parts

def parse_line(line: str) -> dict[str, str]:
    parts = preparse_line(line)
    [
        telephone,
        facebookId,
        name,
        surname,
        sex,
        birthPlace,
        residence,
        maritalStatus,
        profession,
        unknownDate,
        email,
        birthDate
    ] = parts

    return dict(
        telephone=[telephone],
        facebookId=facebookId,
        name=name,
        surname=surname,
        sex=sex or None,
        birthPlace=birthPlace or None,
        residence=residence or None,
        maritalStatus=maritalStatus or None,
        profession=profession or None,
        unknownDate=parse_unknown_date(unknownDate),
        email=email or None,
        birthDate=parse_birthday_date(birthDate)
    )