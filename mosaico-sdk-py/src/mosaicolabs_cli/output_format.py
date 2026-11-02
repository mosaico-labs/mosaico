from enum import Enum


class OutputFormat(str, Enum):
    TABLE = "table"
    CSV = "csv"
    JSON = "json"
    JSONL = "jsonl"
