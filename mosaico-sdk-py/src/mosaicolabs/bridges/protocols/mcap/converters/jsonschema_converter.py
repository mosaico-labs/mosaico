import json
from typing import Any, ClassVar, Tuple, Type, TypeVar

import pyarrow as pa

from mcap.records import Schema

from ..base_converter import McapSchemaConverter
from ..registry import McapSchemaRegistry


@McapSchemaRegistry.register
class JsonschemaSchemaConverter(McapSchemaConverter):
    """Converts jsonschema message descriptors into PyArrow types.

    Only a subset of JSON Schema is understood: ``object``/``properties`` and
    ``array``/``items`` (both arbitrarily nested), and the scalar types ``number``,
    ``integer``, ``boolean``, ``string``, ``null``. Schema composition keywords
    (``$ref``, ``oneOf``, ``anyOf``, ``allOf``, ``const``) and a ``"type"``
    expressed as a list (e.g. the common ``["string", "null"]`` nullable pattern)
    are not supported and will raise. ``enum`` and ``additionalProperties`` are
    silently ignored since they have no direct PyArrow equivalent.
    """

    # Jsonschema supported keys and value
    TYPE_KEY: ClassVar[str] = "type"
    ARRAY_ITEMS_KEY: ClassVar[str] = "items"
    PROPERTIES_KEY: ClassVar[str] = "properties"

    OBJECT_VALUE: ClassVar[str] = "object"
    ARRAY_VALUE: ClassVar[str] = "array"

    # One jsonschema scalar value -> one Arrow type.
    # Refer here for jsonschema types
    _JSONSCHEMA_2_PYARROW_TYPE: ClassVar[dict[str, pa.DataType]] = {
        "number": pa.float64(),
        "integer": pa.int64(),
        "boolean": pa.bool_(),
        "string": pa.string(),
        "null": pa.null(),
    }

    SUPPORTED_ENCODINGS: ClassVar[Tuple[str, ...]] = ("jsonschema",)

    T = TypeVar("T")

    @classmethod
    def _extract_key(
        cls,
        tag_key: str,
        field_value: dict[str, Any],
        expected_tag_type: Type[T],
    ) -> Any:
        """Fetch and type-check a single key out of a jsonschema field descriptor.

        Args:
            tag_key: The jsonschema key to look up (e.g. ``"type"``, ``"items"``).
            field_value: The jsonschema field descriptor to look the key up in.
            expected_tag_type: The type the value stored under ``tag_key`` must be an
                instance of (e.g. ``str`` for ``"type"``, ``dict`` for ``"items"``).

        Returns:
            The value stored under ``tag_key``.

        Raises:
            RuntimeError: If ``tag_key`` is missing, falsy (e.g. an empty dict or
                string), or not an instance of ``expected_tag_type``.
        """

        tag_value = field_value.get(tag_key)

        if tag_value is None or not isinstance(tag_value, expected_tag_type):
            raise RuntimeError(
                f"Impossible to get {tag_key} from jsonschema for type {expected_tag_type.__name__}. Contained keys are {field_value.keys()}"
            )

        return tag_value

    @classmethod
    def _base_type(cls, field_value: dict[str, Any]) -> pa.DataType:
        """Resolve the PyArrow type for a jsonschema field descriptor.

        Recurses into ``_object_to_struct`` for ``"object"`` types and into
        ``pa.list_`` for ``"array"`` types (resolving the ``"items"`` descriptor
        through this same method, so arrays of arrays of arbitrary depth work),
        otherwise looks the scalar type up in ``_JSONSCHEMA_2_PYARROW_TYPE``.

        Args:
            field_value: The jsonschema field descriptor to resolve, expected to hold
                a ``"type"`` key.

        Returns:
            The corresponding ``pa.DataType``.

        Raises:
            RuntimeError: If ``field_value`` (or, for arrays, its ``"items"``) has no
                valid ``"type"``/``"items"`` key.
            KeyError: If a scalar type is not present in ``_JSONSCHEMA_2_PYARROW_TYPE``.
        """

        jsonschema_type = cls._extract_key(cls.TYPE_KEY, field_value, str)

        if jsonschema_type == cls.OBJECT_VALUE:
            properties = cls._extract_key(cls.PROPERTIES_KEY, field_value, dict)
            return cls._object_to_struct(properties)

        if jsonschema_type == cls.ARRAY_VALUE:
            items = cls._extract_key(cls.ARRAY_ITEMS_KEY, field_value, dict)
            return pa.list_(cls._base_type(items))

        return cls._JSONSCHEMA_2_PYARROW_TYPE[jsonschema_type]

    @classmethod
    def _field_to_arrow(cls, field_name, field_value: dict[str, Any]) -> pa.Field:
        """Convert one jsonschema ``properties`` entry into a PyArrow field.

        Every resulting field is marked ``nullable=True`` regardless of the source
        schema's ``required`` list.

        Args:
            field_name: The property name, used as-is as the PyArrow field name.
            field_value: The jsonschema descriptor for this property.

        Returns:
            A ``pa.Field`` named ``field_name`` with the resolved (nullable) type.

        Raises:
            RuntimeError: If ``field_value`` has no valid ``"type"`` (or, for nested
                arrays/objects, if a descriptor further down lacks one).
            KeyError: If a resolved scalar type isn't present in
                ``_JSONSCHEMA_2_PYARROW_TYPE``.
        """

        return pa.field(field_name, cls._base_type(field_value), nullable=True)

    @classmethod
    def _object_to_struct(cls, properties: dict[str, Any]) -> pa.StructType:
        """Convert a jsonschema ``properties`` mapping into a ``pa.StructType``.

        Args:
            properties: Mapping of property name to its jsonschema field descriptor,
                as found under an ``"object"`` schema's ``"properties"`` key.

        Returns:
            A ``pa.StructType`` holding one (nullable) field per entry in ``properties``,
            in iteration order.

        Raises:
            RuntimeError: If any property descriptor is missing a valid ``"type"``.
            KeyError: If any property resolves to an unsupported scalar type.
        """

        return pa.struct(
            [
                cls._field_to_arrow(field_name, field_value)
                for field_name, field_value in properties.items()
            ]
        )

    @classmethod
    def _convert(cls, mcap_schema: Schema) -> pa.StructType:
        """Abstract class implementation of McapSchemaConverter exploited when
        converting MCAPs using mcap library.

        Args:
            mcap_schema: The MCAP ``Schema`` record whose ``data`` holds the raw,
                UTF-8/JSON-encoded jsonschema bytes to convert.

        Returns:
            A ``pa.StructType`` mirroring the schema's top-level ``properties``.
        """

        return cls.convert_jsonschema(mcap_schema.data)

    @classmethod
    def convert_jsonschema(cls, jsonschema_bytes: bytes) -> pa.StructType:
        """Converts a jsonschema MCAP schema into a pa.StructType. Use this when
        you know at build time that you are dealing with jsonschema encoding and
        you don't want to pass through the McapSchemaRegistry returning a specialisation
        of McapSchemaConverter depending on the passed encoding at runtime.

        Args:
            jsonschema_bytes: Raw JSON-encoded jsonschema document (as bytes), expected
                to be a top-level ``"object"`` schema with a ``"properties"`` key.

        Returns:
            A ``pa.StructType`` with one (nullable) field per entry in the schema's
            top-level ``"properties"``.

        Raises:
            json.JSONDecodeError: If ``jsonschema_bytes`` is not valid JSON.
            KeyError: If the decoded schema has no top-level ``"properties"`` key, or
                any property resolves to an unsupported type.
            RuntimeError: If any property descriptor is missing a valid ``"type"``.
        """

        jsonschema_dict = json.loads(jsonschema_bytes)

        # First level properties should always be present for a valid jsonschema
        # otherwise it is an object with no properties and therefore no pyarrow schema
        if (
            not isinstance(jsonschema_dict, dict)
            or cls.PROPERTIES_KEY not in jsonschema_dict
        ):
            return pa.struct()  # FIXME: should this raise or return None?

        return cls._object_to_struct(jsonschema_dict[cls.PROPERTIES_KEY])
