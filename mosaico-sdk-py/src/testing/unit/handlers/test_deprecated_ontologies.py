import logging

import pytest

from mosaicolabs import Image, ImageSequence, ImageSequenceRaw
from mosaicolabs.handlers.base_session_writer import _BaseSessionWriter


class _FakeWriter:
    """A session writer whose topic `camera` exists already: `topic_create`
    returns before contacting the server, after the checks on its ontology."""

    _topic_writers = {"camera": None}
    _logger = logging.getLogger("test")
    _name = "sequence"

    def _check_entered(self):
        pass


def test_writing_an_image_topic_is_deprecated():
    with pytest.warns(
        DeprecationWarning,
        match="use `ImageSequenceRaw` for raw images, or `ImageSequence` for PNGs",
    ):
        _BaseSessionWriter.topic_create(_FakeWriter(), "camera", {}, Image)


@pytest.mark.parametrize("ontology", [ImageSequence, ImageSequenceRaw])
def test_writing_an_image_sequence_topic_is_not(ontology, recwarn):
    _BaseSessionWriter.topic_create(_FakeWriter(), "camera", {}, ontology)
    assert not [w for w in recwarn if issubclass(w.category, DeprecationWarning)]
