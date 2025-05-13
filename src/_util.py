from datetime import datetime


def transform_datetime(date: str, input_format: str, output_formt: str) -> str:
    return datetime.strptime(date, input_format).strftime(output_formt)
