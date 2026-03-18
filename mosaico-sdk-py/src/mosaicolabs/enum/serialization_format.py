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
