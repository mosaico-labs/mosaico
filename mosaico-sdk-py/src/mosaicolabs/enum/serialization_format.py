from enum import Enum


class SerializationFormat(Enum):
    """
    Defines the structural format used when serializing ontology data
    for storage or transmission.

    The format dictates how the data is organized (e.g., fixed-schema tables
    vs. variable-length structures) and may imply specific handling during
    the serialization and deserialization process.
    """

    Default = "default"
    """
    Represents data that conforms to a strict, fixed-width tabular format 
    (like a standard DataFrame or a PyArrow Table of records). 
    Suitable for simple sensors with a constant number of fields and fixed-size data.
    """

    Ragged = "ragged"
    """
    Represents data containing variable-length lists or sequences (e.g., point clouds, 
    lists of detections, or non-uniform arrays). 
    This format is typically serialized using specialized PyArrow features 
    to handle the non-uniform structure efficiently.
    """

    Image = "image"
    """
    Represents raw or compressed image data. 
    This format signals that the data consists primarily of a binary blob 
    (the image content) along with associated metadata (width, height, format), 
    often requiring specialized compression/decompression handling.
    """

    ImageSequence = "image_sequence"
    """
    Represents a sequence of losslessly compressed images (PNG), each with
    its capture time, as modeled by
    [`ImageSequence`][mosaicolabs.models.sensors.ImageSequence]. Lossy
    images, such as JPEG, are not supported.

    The server compresses `capture_timestamp_ns` and `format`, and stores
    `data`, already compressed, as it is. It also keeps statistics on
    `capture_timestamp_ns` and `format`, which makes queries filtering on
    them faster.
    """

    ImageSequenceRaw = "image_sequence_raw"
    """
    Represents a sequence of raw images, as a camera driver writes them
    (e.g. a ROS `sensor_msgs/Image`), each with its capture time, as modeled
    by [`ImageSequenceRaw`][mosaicolabs.models.sensors.ImageSequenceRaw].

    The server compresses the columns describing the images and their
    `data`, and keeps statistics on the former.
    """
