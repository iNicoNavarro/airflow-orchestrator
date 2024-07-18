from datetime import datetime

def convert_date(text: str) -> datetime.date:
    list_format = [
        '%d/%m/%Y',
        '%d/%m/%y',
        '%d-%m-%Y',
        '%d-%m-%y',
        '%m/%d/%Y',
        '%Y-%m-%dT%H:%M:%S.%f',
        '%Y-%m-%d'
    ]

    for form in list_format:
        try:
            return datetime.strptime(str(text), form).date()
        except ValueError:
            pass

    return None