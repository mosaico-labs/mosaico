from enum import StrEnum


class APIKeyPermissionEnum(StrEnum):
    READ = "read"
    WRITE = "write"
    DELETE = "delete"
    MANAGE = "manage"
