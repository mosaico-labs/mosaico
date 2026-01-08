"""
Serialization and Registry Module.

This module defines the `Serializable` base class, which serves as the root for all
specific ontology data types (e.g., IMU, Image, Odometry).

It implements a **Registry/Factory Pattern**:
1.  **Auto-Registration**: Any subclass defined in the code is automatically registered
    via `__init_subclass__`.
2.  **Factory Creation**: The `.create()` method instantiates specific subclasses based
    on a string tag.
3.  **Query Capability**: It injects query proxies allowing users to write `IMU.Q.acc_x > 0`.
"""

from typing import Optional, Type, Dict, List, ClassVar
import pyarrow as pa

from mosaicolabs.enum import SerializationFormat
from mosaicolabs.helpers import camel_to_snake

from .base_model import BaseModel

from .query.generation.api import _QueryableModel
from .query.expressions import _QueryCatalogExpression
from .internal.pyarrow_mapper import PyarrowFieldMapper
from .internal.helpers import _fix_empty_dicts


# --- Private Registry ---
# Global dictionary mapping string tags (e.g., "imu") to class types.
_SENSOR_REGISTRY: Dict[str, Type["Serializable"]] = {}


class Serializable(BaseModel, _QueryableModel):
    """
    Base class for all ontology data payloads, transmitted to the platform.


    Attributes:
        __serialization_format__ (ClassVar): Hints to the writer how to batch this data
                                             (e.g., by Bytes for Images, by Count for Telemetry),
                                             and for the data platform to understand what compression
                                             apply and how to store and index data.
        __ontology_tag__ (ClassVar): The unique string identifier for this class in the registry.
        __class_type__ (ClassVar): A reference to the concrete class itself.
    """

    # --- Factory/Metadata Attributes ---

    # Defaults to 'Default' SerializationFormat.
    # Heavy data types (like Images) should override this to 'Image' (Bytes-based batching).
    __serialization_format__: ClassVar[SerializationFormat] = (
        SerializationFormat.Default
    )

    # Unique tag. If None, it is auto-generated from the class name (CamelCase -> snake_case).
    __ontology_tag__: ClassVar[Optional[str]] = None

    # Reference to the actual subclass.
    __class_type__: ClassVar[Type["Serializable"]]

    def __init_subclass__(cls, **kwargs):
        """
        Metaclass hook for automatic registration.

        When a developer defines `class MySensor(Serializable):`, this method is called.
        It:
        1. Validates that a PyArrow schema is defined.
        2. Generates or assigns a unique ontology tag.
        3. Registers the class in the global `_SENSOR_REGISTRY`.
        4. Injects the Query Proxy (`.Q`) for data querying support.

        Raises:
            ValueError: If a tag collision occurs in the registry.
            AttributeError: If `__msco_pyarrow_struct__` is missing.
        """
        super().__init_subclass__(**kwargs)

        # Schema Validation
        if not hasattr(cls, "__msco_pyarrow_struct__") or not isinstance(
            cls.__msco_pyarrow_struct__, pa.StructType
        ):
            raise AttributeError(
                "Classes for Data Ontology must have a pyarrow '__msco_pyarrow_struct__' attribute."
            )

        # Tag Generation
        tag = cls.__ontology_tag__ or camel_to_snake(cls.__name__)
        cls.__ontology_tag__ = tag
        cls.__class_type__ = cls

        # Registration
        if tag in _SENSOR_REGISTRY:
            raise ValueError(
                f"Duplicate ontology tag '{tag}' detected "
                f"(already registered for '{_SENSOR_REGISTRY[tag].__name__}')"
            )
        _SENSOR_REGISTRY[tag] = cls

        # Query Proxy Injection
        # Enables syntax like: MySensor.Q.field_name > value
        _QueryableModel._inject_query_proxy(
            cls,
            mapper=PyarrowFieldMapper(),
            query_expression_type=_QueryCatalogExpression,
            query_prefix=None,
        )

    # --- Factory Methods ---

    @classmethod
    def create(cls, tag: str, *args, **kwargs) -> "Serializable":
        """
        Factory method to instantiate a specific ontology object by tag.

        Args:
            tag (str): The unique tag of the ontology (e.g., "imu", "gps").
            *args: Positional arguments for the ontology constructor.
            **kwargs: Keyword arguments for the ontology constructor.

        Returns:
            Serializable: An instance of the requested subclass.

        Raises:
            ValueError: If the tag is not found in the registry.
        """
        if tag not in _SENSOR_REGISTRY:
            raise ValueError(
                f"No ontology registered with tag '{tag}'. "
                f"Available tags: {list(_SENSOR_REGISTRY.keys())}"
            )

        # Clean up potential artifacts from Parquet deserialization (e.g., None as empty structs)
        fixed_kwargs = _fix_empty_dicts(kwargs) if kwargs else {}

        # Instantiate
        return _SENSOR_REGISTRY[tag](*args, **fixed_kwargs)

    # --- Registry Helper Methods ---

    @classmethod
    def list_registered(cls) -> List[str]:
        """
        Returns a list of all available ontology tags.
        """
        return list(_SENSOR_REGISTRY.keys())

    @classmethod
    def is_registered(cls, tag: str) -> bool:
        """
        Checks if a tag is registered.

        Args:
            tag (str): The tag to check.

        Returns:
            bool: True if registered.
        """
        return tag in _SENSOR_REGISTRY.keys()

    @classmethod
    def get_class_type(cls, tag: str) -> Optional[Type["Serializable"]]:
        """
        Retrieves the Python class type associated with a tag.

        Args:
            tag (str): The ontology tag.

        Returns:
            Optional[Type[Serializable]]: The class type, or None if not found.
        """
        if not cls.is_registered(tag):
            return None
        return _SENSOR_REGISTRY[tag].__class_type__

    @classmethod
    def get_ontology_tag(
        cls, class_type_name: str, case_sensitive: bool = True
    ) -> Optional[str]:
        """
        Reverse lookup: finds a tag given a class name.

        Args:
            class_type_name (str): The name of the class (e.g., "IMU").
            case_sensitive (bool): Whether to perform case-sensitive matching.

        Returns:
            Optional[str]: The tag, or None if the class is not found.
        """
        class_type_name_cmp = (
            class_type_name if case_sensitive else class_type_name.lower()
        )

        return next(
            (
                sens.__ontology_tag__
                for sens in _SENSOR_REGISTRY.values()
                if (
                    sens.__class_type__.__name__
                    if case_sensitive
                    else sens.__class_type__.__name__.lower()
                )
                == class_type_name_cmp
            ),
            None,
        )

    @classmethod
    def ontology_tag(cls) -> str:
        """
        Instance/Class method to get the tag of the current class.

        Raises:
            Exception: If the class was not properly initialized via __init_subclass__.
        """
        if not hasattr(cls, "__ontology_tag__") or cls.__ontology_tag__ is None:
            raise Exception(
                f"class '{cls.__name__}' has no '__ontology_tag__' attribute. Initialization failed."
            )
        return cls.__ontology_tag__
