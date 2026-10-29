"""
Image Sequence Ontology Module.

Defines the data structure for an image of a sequence, with the time it was
captured, stored with the `ImageSequence` serialization format. The images of
a sequence are all encoded in the same format, either PNG or TIFF.
"""

from enum import Enum

from mosaicolabs.enum import SerializationFormat

from ..core import MosaicoField, MosaicoType, Serializable


class ImageSequenceFormat(str, Enum):
    """
    Defines the encodings an [`ImageSequence`][mosaicolabs.models.sensors.ImageSequence] can hold.
    """

    PNG = "png"
    """Portable Network Graphics, a lossless format."""

    TIFF = "tiff"
    """Tagged Image File Format, suited to high bit depths (e.g. 16 bits)."""


class ImageSequence(Serializable):
    """
    Represents an image of a sequence, encoded as PNG or TIFF, with the time
    it was captured.

    All the images of a topic must have the same format (e.g. all PNGs or
    all TIFFs): the server rejects an upload mixing formats.

    The capture time is the time the camera took the image, while the
    `timestamp_ns` of the [`Message`][mosaicolabs.models.core.Message]
    wrapping it is the time the image was recorded.

    The class uses the `ImageSequence` serialization format: the server
    compresses `capture_timestamp_ns` and `format`, and keeps statistics on
    them for filtering, and compresses `data`, without statistics: TIFFs get
    smaller, while PNGs, already compressed, stay nearly unchanged.
    The format relies on these field names, so they must not be renamed.

    Attributes:
        format (ImageSequenceFormat): The image encoding ('png' or 'tiff').
        capture_timestamp_ns (int): The capture time, in nanoseconds since the Unix epoch.
        data (bytes): The encoded image.

    ### Querying with the **`.Q` Proxy**
    This class is fully queryable via the **`.Q` proxy**, except for `data`
    (bytes are not comparable).

    Example:
        ```python
        from mosaicolabs import MosaicoClient, ImageSequence, QueryOntologyCatalog

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter for PNG images captured within a time range
            qresponse = client.query(
                QueryOntologyCatalog(ImageSequence.Q.format.eq("png"))
                .with_expression(
                    ImageSequence.Q.capture_timestamp_ns.between(1700000000, 1800000000)
                ),
            )

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {[topic.name for topic in item.topics]}")
        ```
    """

    __serialization_format__ = SerializationFormat.ImageSequence

    format: ImageSequenceFormat = MosaicoField(
        description="The image encoding ('png' or 'tiff')."
    )
    """
    The image encoding ('png' or 'tiff').

    ### Querying with the **`.Q` Proxy**
    | Field Access Path | Queryable Type | Supported Operators |
    | :--- | :--- | :--- |
    | `ImageSequence.Q.format` | `String` | `.eq()`, `.match()`, `.in_()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.between()`, `.outside()` |
    """

    capture_timestamp_ns: MosaicoType.int64 = MosaicoField(
        description="Capture time, in nanoseconds since the Unix epoch."
    )
    """
    The capture time, in nanoseconds since the Unix epoch.

    ### Querying with the **`.Q` Proxy**
    | Field Access Path | Queryable Type | Supported Operators |
    | :--- | :--- | :--- |
    | `ImageSequence.Q.capture_timestamp_ns` | `Numeric` | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()`, `.outside()` |
    """

    data: MosaicoType.binary = MosaicoField(description="The encoded image.")
    """
    The encoded image.

    ### Querying with the **`.Q` Proxy**
    The data is not queryable via the `data` field (bytes are not comparable).
    """
