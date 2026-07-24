"""
Serialization and Registry Module.

This module defines the `Serializable` base class, which serves as the root for all
specific ontology data types (e.g., IMU, Image, Odometry).

It implements a **Registry/Factory Pattern**:
1.  **Auto-Registration**: Any subclass defined in the code is automatically registered
    via `__pydantic_init_subclass__`.
2.  **Factory Creation**: The `._create()` method instantiates specific subclasses based
    on a string tag.
3.  **Query Capability**: It injects query proxies allowing users to write `IMU.Q.acc_x > 0`.
"""

import hashlib
from enum import Enum
from typing import (
    Annotated,
    Any,
    ClassVar,
    Dict,
    List,
    Optional,
    Type,
    Union,
    get_args,
    get_origin,
)

import pyarrow as pa

from mosaicolabs.enum import SerializationFormat

from ...query.generation.api import _QueryProxyMixin
from .base_model import BaseModel
from .internal.helpers import _fix_empty_dicts, encode_to_dict
from .internal.pyarrow_mapper import PyarrowFieldMapper
from .types import BASE_MAPPING, REMAPPED_PYARROW_TYPES

# --- Private Registry ---
# Global dictionary mapping string tags (e.g., "IMU") to class types.
_SCHEMA_REGISTRY: Dict[str, Type["Serializable"]] = {}
SCHEMA_ID_LEN = 10  # 40 bits


def _canonicalize_arrow_type(schema_struct: pa.StructType) -> pa.StructType:
    """
    Normalizes **first level only** of Arrow schemas that are logically
    equivalent but physically distinct (e.g. `pa.string`, `pa.string_view`),
    so ontologies are compared on logical shape rather than physical
    representation.

    This matters because the query engine reading data back (e.g. DataFusion's
    Parquet reader) may return `pa.string_view`/`pa.binary_view` for a column
    the SDK declared as `pa.string`/`pa.binary`.
    Without normalization, the same logical schema would hash differently depending
    on which variant a given read happened to produce, causing a correctly modeled
    ontology to be misidentified as an unrecognized (`Unmodeled`) schema variant.
    """

    remapped_field = []
    for field in schema_struct:
        if field.type in REMAPPED_PYARROW_TYPES:
            remapped_type = REMAPPED_PYARROW_TYPES[field.type]
        else:
            remapped_type = field.type

        remapped_field.append(
            pa.field(
                field.name,
                remapped_type,
                nullable=field.nullable,
                metadata=field.metadata,
            )
        )

    return pa.struct(remapped_field)


def _compute_schema_fingerprint(struct: pa.StructType) -> str:
    """
    Computes a short, deterministic fingerprint identifying a PyArrow struct schema.

    Two structurally identical schemas produce the same fingerprint. The fingerprint
    is used internally to distinguish different schema shapes associated with the
    same ontology tag within a process (e.g. when multiple ontology versions of the
    same dynamically resolved, unmodeled data type are encountered).

    Fields are normalized via `_canonicalize_arrow_type` before hashing, so the
    fingerprint reflects logical schema shape rather than whichever
    physically-distinct type variant (e.g. `pa.string` vs. `pa.string_view`) a
    particular read happened to produce.

    The SHA-1 digest is intentionally truncated to 10 hexadecimal characters (40 bits)
    to keep the fingerprint compact. Given the expected number of distinct schema
    versions, the probability of an accidental collision is negligible for this use
    case.
    """
    canonical = _canonicalize_arrow_type(struct)
    return hashlib.sha1(str(canonical).encode("utf-8")).hexdigest()[:SCHEMA_ID_LEN]


class Serializable(BaseModel, _QueryProxyMixin):
    """
    The base class for all Mosaico ontology data payloads.

    This class serves as the root for every sensor and data type in the Mosaico ecosystem.
    By inheriting from `Serializable`, data models are automatically compatible with the platform's storage,
    querying, and serialization engines.

    ### Dynamic Attributes Injection
    When you define a subclass, several key attributes are automatically managed or required.
    Understanding these is essential for customizing how your data is treated by the platform:

    * **`__serialization_format__`**:
        Determines the batching strategy and storage optimization.
        * **Role**: It tells the `SequenceWriter` whether to flush data based on byte size (optimal for heavy data like `Images`) or record count (optimal for light telemetry like `IMU`).
        * **Default**: `SerializationFormat.Default`.

    * **`__ontology_tag__`**:
        The string identifier for the class as known to the Mosaico platform (e.g., `"imu"`, `"gps_raw"`).
        * **Role**: This is the tag sent to the server (topic creation) and embedded in `.Q` query paths; it's what [`ontology_tag()`][mosaicolabs.models.core.Serializable.ontology_tag] returns.
        * **Generation**: If not explicitly provided, it is auto-generated by converting the class name from `CamelCase` to `snake_case`.
        * **Note**: Unlike a Python registry key, this value is *not* guaranteed unique per class — see `__registry_key__` below.

    * **`__registry_key__`**:
        The internal key this class is stored under in the SDK's local class registry.
        * **Role**: Guarantees a collision-free lookup key per Python class, independent of `__ontology_tag__`. Defaults to `__ontology_tag__` itself, so for every hand-authored class the two are identical.
        * **When they differ**: dynamically-resolved (`Unmodeled`) classes can end up sharing one `__ontology_tag__` across multiple schema shapes (e.g. two versions of the same message type). In that case, only the first-seen schema keeps `__registry_key__ == __ontology_tag__`; subsequent schema variants get a distinct, fingerprint-suffixed `__registry_key__` while still reporting the *same* `__ontology_tag__` to the platform.

    * **`__class_type__`**:
        A reference to the concrete class itself.
        * **Role**: Injected during initialization to facilitate polymorphic instantiation and safe type-checking when extracting data from a [`Message`][mosaicolabs.models.core.Message].

    ### Requirements for Custom Ontologies
    To create a valid custom ontology, your subclass must:

    1.  Inherit from `Serializable`.
    2.  Define the attributes using [`MosaicoType`][mosaicolabs.models.core.MosaicoType] and [`MosaicoField`][mosaicolabs.models.core.MosaicoField]

    Tip: Automatic Registration
        Any subclass of `Serializable` is automatically registered in the global Mosaico registry upon definition. This enables the use of the factory methods and the `.Q` query proxy immediately.
    """

    # --- Factory/Metadata Attributes ---

    # Defaults to 'Default' SerializationFormat.
    # Heavy data types (like Images) should override this to 'Image' (Bytes-based batching).
    __serialization_format__: ClassVar[SerializationFormat] = (
        SerializationFormat.Default
    )

    # Tag as known to the platform. If None, it is auto-generated from the class name
    # (CamelCase -> snake_case). Not guaranteed unique per class - see __registry_key__.
    __ontology_tag__: ClassVar[Optional[str]] = None

    # The SDK-local registry key for this class. Defaults to __ontology_tag__ if None;
    # only diverges from it for dynamically-resolved schema variants sharing one tag.
    __registry_key__: ClassVar[Optional[str]] = None

    # Reference to the actual subclass.
    __class_type__: ClassVar[Type["Serializable"]]

    __skip_schema_generation__: ClassVar[bool] = False
    __skip_query_proxy_ingestion__: ClassVar[bool] = False

    # Deterministic fingerprint of `__msco_pyarrow_struct__`, computed automatically
    # upon registration. Lets callers detect when a tag is associated with more
    # than one schema shape without needing a full structural comparison.
    __schema_fingerprint__: ClassVar[str] = ""

    # Consume schema generation flag
    def __init_subclass__(
        cls,
        *,
        skip_schema_generation: bool = False,
        skip_query_proxy_ingestion: bool = False,
        **kwargs,
    ):
        """
        Initializes subclasses of ``Serializable`` and processes class-definition
        options used during subclass creation.

        This hook consumes the ``skip_schema_generation`` and `skip_query_proxy_ingestion`
        keyword arguments passed in the subclass declaration (e.g. ``class MyModel(Serializable,
        skip_schema_generation=True):``) and stores it as a class attribute for
        later use by ``__pydantic_init_subclass__``. Any remaining keyword arguments
        are forwarded to the superclass implementation.

        Args:
            skip_schema_generation: If ``True``, disables automatic arrow schema
                generation for the subclass during Pydantic subclass initialization.
            skip_query_proxy_ingestion: If ``True``, disables automatic .Q query
                proxy ingestion in the subclass during Pydantic subclass initialization.
            **kwargs: Additional keyword arguments forwarded to the superclass.
        """
        cls.__skip_schema_generation__ = skip_schema_generation
        cls.__skip_query_proxy_ingestion__ = skip_query_proxy_ingestion
        super().__init_subclass__(**kwargs)

    @classmethod
    def __pydantic_init_subclass__(cls, **kwargs):
        """
        Registers the subclass and injects query capabilities.

        This method is called automatically when a new ontology class is defined.
        It performs schema validation, tag generation, and global registration.

        Raises:
            AttributeError: If `__msco_pyarrow_struct__` is missing or invalid.
            ValueError: If the generated or assigned tag collides with an existing one.
        """
        if not cls.__skip_schema_generation__:
            cls.__msco_pyarrow_struct__ = cls._build_ontology_struct(cls)

        cls.__schema_fingerprint__ = _compute_schema_fingerprint(
            cls.__msco_pyarrow_struct__
        )

        # TODO: check if is it correct call super here.
        super().__pydantic_init_subclass__(**kwargs)

        # Tag Generation
        tag = cls.__ontology_tag__ or cls.__name__
        cls.__ontology_tag__ = tag
        cls.__class_type__ = cls

        # Registry Key Generation: defaults to the ontology tag itself. Only
        # dynamically-resolved schema variants explicitly set __registry_key__
        # ahead of time to a distinct, fingerprint-suffixed value.
        registry_key = cls.__registry_key__ or tag
        cls.__registry_key__ = registry_key

        # Registration
        if registry_key in _SCHEMA_REGISTRY:
            raise ValueError(
                f"Duplicate ontology registry key '{registry_key}' detected "
                f"(already registered for '{_SCHEMA_REGISTRY[registry_key].__name__}')"
            )
        _SCHEMA_REGISTRY[registry_key] = cls

        # Query Proxy Injection
        # Enables syntax like: MySensor.Q.field_name > value
        if not cls.__skip_query_proxy_ingestion__:
            _QueryProxyMixin._inject_query_proxy(
                cls,
                mapper=PyarrowFieldMapper(),
                query_prefix=None,
            )

    def _encode(self):
        return {
            name: encode_to_dict(value) for name, value in self.model_dump().items()
        }

    # --- Factory Methods ---

    @classmethod
    def _decode(cls, *args, **kwargs) -> "Serializable":
        """
        Factory method to decode a specific ontology object via Arrow raw data

        Args:
            *args: Positional arguments for the subclass constructor.
            **kwargs: Keyword arguments for the subclass constructor.

        Returns:
            An instance of the corresponding `Serializable` subclass.
        """
        # Clean up potential artifacts from Parquet deserialization (e.g., None as empty structs)
        fixed_kwargs = _fix_empty_dicts(kwargs) if kwargs else {}

        # Instantiate
        return cls(*args, **fixed_kwargs)

    # --- Registry Helper Methods ---

    @classmethod
    def _list_registered(cls) -> List[str]:
        """Returns a list of all currently registered ontology tags."""
        return list(_SCHEMA_REGISTRY.keys())

    @classmethod
    def _is_registered(cls, tag: str) -> bool:
        """
        Checks if a tag is registered.

        Args:
            tag (str): The tag to check.

        Returns:
            bool: True if registered.
        """
        return tag in _SCHEMA_REGISTRY.keys()

    @classmethod
    def _get_class_type(cls, tag: str) -> Optional[Type["Serializable"]]:
        """
        Retrieves the concrete Python class type associated with a specific tag.

        Args:
            tag: The unique ontology identifier.

        Returns:
            The Python class type if found, otherwise `None`.
        """
        if not cls._is_registered(tag):
            return None
        return _SCHEMA_REGISTRY[tag].__class_type__

    @classmethod
    def _build_ontology_struct(cls, model_class: type[BaseModel]) -> pa.StructType:
        """
        Recursively converts a Pydantic model into a PyArrow StructType.

        This method iterates through the fields of a Pydantic model, resolves their
        types into PyArrow equivalents, and preserves field descriptions as
        PyArrow field metadata.

        Args:
            model_class: The Pydantic model class to convert.

        Returns:
            A pa.StructType representing the schema of the Pydantic model.
        """

        cached_struct = model_class.__dict__.get("__msco_pyarrow_struct__")
        if cached_struct and len(cached_struct) > 0:
            return cached_struct

        pa_fields = []
        for field_name, field_info in model_class.model_fields.items():
            annotation = field_info.annotation
            metadata = field_info.metadata[0] if field_info.metadata else None
            pa_type = cls._resolve_type(annotation, metadata)

            # Get the nullable of json_schema_extra if MosaicoField is used otherwise "required" attribute from pydantic
            nullable = (
                bool(
                    field_info.json_schema_extra.get(
                        "nullable", not field_info.is_required()
                    )
                )
                if field_info.json_schema_extra
                else not field_info.is_required()
            )
            metadata = None

            if field_info.description:
                metadata = {b"description": field_info.description.encode("utf-8")}

            pa_fields.append(
                pa.field(field_name, pa_type, nullable=nullable, metadata=metadata)
            )
        return pa.struct(pa_fields)

    @classmethod
    def _resolve_type(
        cls, base_type: Any, suggested_type: Optional[Any] = None
    ) -> pa.DataType:
        """
        Maps Python/Pydantic types to their corresponding PyArrow DataType.

        This method handles nested structures, including:
        * Primitive types (via BASE_MAPPING).
        * Complex types (List, Dict/Map).
        * Pydantic models (nested structs).
        * Enums (resolved to their base primitive type).
        * Unions/Optionals (supports single-type Unions with None).
        * Annotated types (extracts metadata for type hinting).

        Args:
            base_type: The Python type or type hint to resolve.
            suggested_type: An optional pre-defined PyArrow DataType provided
                via Annotated metadata.

        Returns:
            The resolved pa.DataType.

        Raises:
            NotImplementedError: If a Union contains more than one non-None type.
            ValueError: If the type cannot be mapped to a known PyArrow type.
        """
        if isinstance(suggested_type, pa.DataType):
            return suggested_type

        origin = get_origin(base_type)
        args = get_args(base_type)

        if origin is Annotated:
            return cls._resolve_type(args[0], args[1])

        # Optional / Union resolver
        if origin is Union:
            non_none = [a for a in args if a is not type(None)]
            if len(non_none) == 1:
                return cls._resolve_type(non_none[0])
            raise NotImplementedError(
                f"Union with multiple types is not supported: {args}"
            )

        # Dict resolver
        if origin is dict:
            inner_type_1 = args[0] if args else Any
            inner_type_2 = args[1] if args else Any
            resolved_inner_type_1 = cls._resolve_type(inner_type_1)
            resolved_inner_type_2 = cls._resolve_type(inner_type_2)
            return pa.map_(resolved_inner_type_1, resolved_inner_type_2)

        # List resolver
        if origin is list:
            inner_type = args[0] if args else Any
            resolved_inner_type = cls._resolve_type(inner_type)
            return pa.list_(resolved_inner_type)

        # type is a pydantic model
        type_to_check = origin if origin is not None else base_type
        if isinstance(type_to_check, type) and issubclass(type_to_check, BaseModel):
            return cls._build_ontology_struct(type_to_check)

        # fallback
        base_primitive = origin if origin is not None else base_type
        base_pa = BASE_MAPPING.get(base_primitive)

        if base_pa is None:
            # Enum resolver
            if isinstance(base_primitive, type) and issubclass(base_primitive, Enum):
                base_pa = next(
                    (
                        b
                        for b in base_primitive.__mro__
                        if b not in (base_primitive, Enum, object)
                        and not issubclass(b, Enum)
                    ),
                    None,
                )

                return cls._resolve_type(base_pa)
            else:
                raise ValueError(
                    f"{cls.__name__}: Base mapping not found for {base_primitive}. Any is not supported."
                )

        return base_pa

    # --- Public Registry APIs ---

    @classmethod
    def is_registered(cls) -> bool:
        """
        Checks if a class is registered.

        Returns:
            bool: True if registered.
        """
        if not hasattr(cls, "__registry_key__") or cls.__registry_key__ is None:
            return False
        return cls._is_registered(cls.__registry_key__)

    @classmethod
    def ontology_tag(cls) -> str:
        """
        Retrieves the unique identifier (tag) for the current ontology class, automatically generated during class definition.

        This method provides the string key used by the Mosaico platform to identify and route
        specific data types within the ontology registry. It abstracts
        away the internal naming conventions, ensuring that you always use the correct
        identifier for queries and serialization.

        Returns:
            The registered string tag for this class (e.g., `"imu"`, `"gps"`).

        Raises:
            Exception: If the class was not properly initialized via `__pydantic_init_subclass__`.

        Hint: **Practical Application: Topic Filtering**
            This method is particularly useful when constructing [`QueryTopic`][mosaicolabs.query.builders.QueryTopic]
            requests. By using the convenience method [`QueryTopic.with_ontology_tag()`][mosaicolabs.query.builders.QueryTopic.with_ontology_tag],
            you can filter topics by data type without hardcoding strings that might change.

            Example:
                ```python
                from mosaicolabs import MosaicoClient, Topic, IMU, QueryTopic

                with MosaicoClient.connect("localhost", 6726) as client:
                    # Filter for a specific data value (using constructor)
                    qresponse = client.query(
                        QueryTopic(
                            Topic.with_ontology_tag(IMU.ontology_tag()),
                        )
                    )

                    # Inspect the response
                    if qresponse is not None:
                        # Results are automatically grouped by Sequence for easier data management
                        for item in qresponse:
                            print(f"Sequence: {item.sequence.name}")
                            print(f"Topics: {[topic.name for topic in item.topics]}")
                ```
        """
        if not hasattr(cls, "__ontology_tag__") or cls.__ontology_tag__ is None:
            raise Exception(
                f"class '{cls.__name__}' has no '__ontology_tag__' attribute. Initialization failed."
            )
        return cls.__ontology_tag__
