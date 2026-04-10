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

# --- Enums ---
from .enum import (
    OnErrorPolicy as OnErrorPolicy,
    SequenceStatus as SequenceStatus,
    SerializationFormat as SerializationFormat,
    SessionLevelErrorPolicy as SessionLevelErrorPolicy,
    SessionStatus as SessionStatus,
    TopicLevelErrorPolicy as TopicLevelErrorPolicy,
    TopicWriterStatus as TopicWriterStatus,
)

# --- Handlers ---
from .handlers import (
    SequenceDataStreamer as SequenceDataStreamer,
    SequenceHandler as SequenceHandler,
    SequenceUpdater as SequenceUpdater,
    SequenceWriter as SequenceWriter,
    TopicDataStreamer as TopicDataStreamer,
    TopicHandler as TopicHandler,
    TopicWriter as TopicWriter,
)
from .logging_config import (
    get_logger as get_logger,
    setup_sdk_logging as setup_sdk_logging,
)

# --- Core Models ---
from .models import (
    BaseModel as BaseModel,
    CovarianceMixin as CovarianceMixin,
    Message as Message,
    MosaicoField as MosaicoField,
    MosaicoType as MosaicoType,
    Serializable as Serializable,
    VarianceMixin as VarianceMixin,
)

# --- Base Types ---
# --- Geometry ---
# --- Dynamics & Kinematics ---
# --- Other Data Types ---
from .models.data import (
    ROI as ROI,
    Acceleration as Acceleration,
    Boolean as Boolean,
    Floating16 as Floating16,
    Floating32 as Floating32,
    Floating64 as Floating64,
    ForceTorque as ForceTorque,
    Integer8 as Integer8,
    Integer16 as Integer16,
    Integer32 as Integer32,
    Integer64 as Integer64,
    LargeString as LargeString,
    MotionState as MotionState,
    Point2d as Point2d,
    Point3d as Point3d,
    Pose as Pose,
    Quaternion as Quaternion,
    String as String,
    Transform as Transform,
    Unsigned8 as Unsigned8,
    Unsigned16 as Unsigned16,
    Unsigned32 as Unsigned32,
    Unsigned64 as Unsigned64,
    Vector2d as Vector2d,
    Vector3d as Vector3d,
    Vector4d as Vector4d,
    Velocity as Velocity,
)

# --- Platform ---
from .models.platform import (
    Sequence as Sequence,
    Session as Session,
    Topic as Topic,
)

# --- Main Query classes ---
# --- Query Responses ---
from .models.query import (
    Query as Query,
    QueryOntologyCatalog as QueryOntologyCatalog,
    QueryResponse as QueryResponse,
    QueryResponseItem as QueryResponseItem,
    QueryResponseItemSequence as QueryResponseItemSequence,
    QueryResponseItemTopic as QueryResponseItemTopic,
    QuerySequence as QuerySequence,
    QueryTopic as QueryTopic,
    TimestampRange as TimestampRange,
)

# --- Sensors ---
from .models.sensors import (
    GPS as GPS,
    IMU as IMU,
    CameraInfo as CameraInfo,
    CompressedImage as CompressedImage,
    GPSStatus as GPSStatus,
    Image as Image,
    ImageFormat as ImageFormat,
    Magnetometer as Magnetometer,
    NMEASentence as NMEASentence,
    Pressure as Pressure,
    Range as Range,
    RobotJoint as RobotJoint,
    Temperature as Temperature,
)

# --- Types ---
from .types import Time as Time

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
    "SequenceUpdater",
    "TopicHandler",
    "TopicWriter",
    "TopicDataStreamer",
    # Core Models
    "BaseModel",
    "Serializable",
    "Time",
    "Message",
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
    "Pressure",
    "Temperature",
    "Range",
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
    "SessionStatus",
    "SequenceStatus",
    "SessionLevelErrorPolicy",
    "TopicLevelErrorPolicy",
    "TopicWriterStatus",
    # Platform
    "Sequence",
    "Session",
    "Topic",
    # Mosaico Type & MosaicoField
    "MosaicoField",
    "MosaicoType",
]


# --- Set up the top-level logger for the SDK ---

from logging import NullHandler

logging_config = get_logger()
logging_config.addHandler(NullHandler())
