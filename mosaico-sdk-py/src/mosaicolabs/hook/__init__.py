"""
Run your own code when data lands in mosaicod.

A hook is an HTTP endpoint that mosaicod sends an event to when a topic finishes
uploading and when a session is finalized. `HookServer` receives those events and
passes each one to every function decorated with it.
"""

from .events import (
    Event as Event,
    SessionUploaded as SessionUploaded,
    TopicUploaded as TopicUploaded,
)
from .server import HookServer as HookServer
