from enum import Enum


class APIKeyPermissionEnum(Enum):
    READ = "read"
    WRITE = "write"
    DELETE = "delete"
    MANAGE = "manage"
