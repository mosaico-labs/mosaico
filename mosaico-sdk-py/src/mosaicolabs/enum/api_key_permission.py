from enum import Enum


class APIKeyPermissionEnum(Enum):
    Read = "read"
    Write = "write"
    Delete = "delete"
    Manage = "manage"
