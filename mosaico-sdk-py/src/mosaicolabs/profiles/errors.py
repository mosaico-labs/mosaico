class ProfileError(Exception):
    """Base class for profile errors."""


class ProfileNotFoundError(ProfileError, KeyError):
    def __str__(self) -> str:
        return str(self.args[0])  # KeyError would quote the message


class ProfileExistsError(ProfileError):
    pass


class ProfileConfigError(ProfileError):
    """The configuration file cannot be read or written."""
