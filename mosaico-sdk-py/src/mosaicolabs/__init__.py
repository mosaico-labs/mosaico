"""
Mosaico SDK - Python client for the Mosaico Data Platform.

This module provides the main entry points for interacting with Mosaico:

- **MosaicoClient**: The primary client for connecting to the platform.
- **Handlers**: Classes for reading/writing sequences and topics.
- **Models**: Data structures for the Mosaico ontology (sensors, geometry, etc.).

Example:
    >>> from mosaicolabs import MosaicoClient
    >>> with MosaicoClient.connect("localhost", 6726) as client:
    ...     handler = client.sequence_handler("my_sequence")
"""

# --- Client ---
from .comm import MosaicoClient as MosaicoClient

# --- Handlers ---
from .handlers import (
    SequenceHandler as SequenceHandler,
    SequenceWriter as SequenceWriter,
    SequenceDataStreamer as SequenceDataStreamer,
    TopicHandler as TopicHandler,
    TopicWriter as TopicWriter,
    TopicDataStreamer as TopicDataStreamer,
    SystemInfo as SystemInfo,
)

# --- Core Models ---
from .models import (
    BaseModel as BaseModel,
    Serializable as Serializable,
    Header as Header,
    Time as Time,
    Message as Message,
    HeaderMixin as HeaderMixin,
    CovarianceMixin as CovarianceMixin,
    VarianceMixin as VarianceMixin,
)

# --- Sensors ---
from .models.sensors import (
    CameraInfo as CameraInfo,
    GPS as GPS,
    GPSStatus as GPSStatus,
    NMEASentence as NMEASentence,
    Image as Image,
    ImageFormat as ImageFormat,
    CompressedImage as CompressedImage,
    IMU as IMU,
    Magnetometer as Magnetometer,
    RobotJoint as RobotJoint,
)

# --- Base Types ---
from .models.data import (
    Boolean as Boolean,
    Integer8 as Integer8,
    Integer16 as Integer16,
    Integer32 as Integer32,
    Integer64 as Integer64,
    Unsigned8 as Unsigned8,
    Unsigned16 as Unsigned16,
    Unsigned32 as Unsigned32,
    Unsigned64 as Unsigned64,
    Floating16 as Floating16,
    Floating32 as Floating32,
    Floating64 as Floating64,
    String as String,
    LargeString as LargeString,
)

# --- Geometry ---
from .models.data import (
    Point2d as Point2d,
    Point3d as Point3d,
    Vector2d as Vector2d,
    Vector3d as Vector3d,
    Vector4d as Vector4d,
    Quaternion as Quaternion,
    Pose as Pose,
    Transform as Transform,
)

# --- Dynamics & Kinematics ---
from .models.data import (
    ForceTorque as ForceTorque,
    Acceleration as Acceleration,
    Velocity as Velocity,
    MotionState as MotionState,
)

# --- Other Data Types ---
from .models.data import ROI as ROI

# --- Main Query classes ---
from .models.query import (
    Query as Query,
    QueryTopic as QueryTopic,
    QuerySequence as QuerySequence,
    QueryOntologyCatalog as QueryOntologyCatalog,
)

# --- Query Responses ---
from .models.query import (
    QueryResponseItemSequence as QueryResponseItemSequence,
    QueryResponseItemTopic as QueryResponseItemTopic,
    TimestampRange as TimestampRange,
    QueryResponseItem as QueryResponseItem,
    QueryResponse as QueryResponse,
)

# --- Enums ---
from .enum import (
    SerializationFormat as SerializationFormat,
    SequenceStatus as SequenceStatus,
    OnErrorPolicy as OnErrorPolicy,
)

from .logging_config import (
    get_logger as get_logger,
    setup_sdk_logging as setup_sdk_logging,
)

__all__ = [
    # Client
    "MosaicoClient",
    # Logging
    "get_logger",
    "setup_sdk_logging",
    # Handlers
    "SequenceHandler",
    "SequenceWriter",
    "SequenceDataStreamer",
    "TopicHandler",
    "TopicWriter",
    "TopicDataStreamer",
    "SystemInfo",
    # Core Models
    "BaseModel",
    "Serializable",
    "Header",
    "Time",
    "Message",
    "HeaderMixin",
    "CovarianceMixin",
    "VarianceMixin",
    # Sensors
    "CameraInfo",
    "GPS",
    "GPSStatus",
    "NMEASentence",
    "Image",
    "ImageFormat",
    "CompressedImage",
    "IMU",
    "Magnetometer",
    "RobotJoint",
    # Base Types
    "Boolean",
    "Integer8",
    "Integer16",
    "Integer32",
    "Integer64",
    "Unsigned8",
    "Unsigned16",
    "Unsigned32",
    "Unsigned64",
    "Floating16",
    "Floating32",
    "Floating64",
    "String",
    "LargeString",
    # Geometry
    "Point2d",
    "Point3d",
    "Vector2d",
    "Vector3d",
    "Vector4d",
    "Quaternion",
    "Pose",
    "Transform",
    # Dynamics & Kinematics
    "ForceTorque",
    "Acceleration",
    "Velocity",
    "MotionState",
    # Other
    "ROI",
    # Query
    "Query",
    "QueryTopic",
    "QuerySequence",
    "QueryOntologyCatalog",
    "QueryResponseItemSequence",
    "QueryResponseItemTopic",
    "TimestampRange",
    "QueryResponseItem",
    "QueryResponse",
    # Enums
    "SerializationFormat",
    "SequenceStatus",
    "OnErrorPolicy",
]


# --- Set up the top-level logger for the SDK ---

from logging import NullHandler

logging_config = get_logger()
logging_config.addHandler(NullHandler())
