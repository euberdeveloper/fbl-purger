from datetime import datetime

def log(*args: list[str]) -> None:
    timestamp = datetime.now().isoformat()
    print(f'[{timestamp}]', *args)