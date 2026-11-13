"""
Run your own code on the events mosaicod sends.

A hook is an HTTP endpoint that mosaicod sends events to. `HookServer` receives them
and passes each one to every function decorated with it.
"""

from .events import (
    Event as Event,
    SessionUploaded as SessionUploaded,
    TopicUploaded as TopicUploaded,
)
from .server import HookServer as HookServer
