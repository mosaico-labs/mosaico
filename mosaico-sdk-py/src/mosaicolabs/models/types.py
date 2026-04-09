from types import EllipsisType
from typing import Annotated, Any, Dict, Optional, Type

import pyarrow as pa
from pydantic import Field

BASE_MAPPING: Dict[Type, pa.DataType] = {
    int: pa.int64(),
    float: pa.float64(),
    str: pa.string(),
    bool: pa.bool_(),
    bytes: pa.binary(),
}


class MosaicoType:
    """
    Collection of ``Annotated`` type aliases mapping Python primitives to
    their PyArrow counterparts.

    Each class attribute is an ``Annotated[PythonType, pa.DataType]`` alias.
    When used as a field annotation in a ``Serializable`` subclass, the
    embedded ``pa.DataType`` is extracted by ``_build_ontology_struct`` at
    class-definition time to derive the ``__msco_pyarrow_struct__``
    automatically — no manual schema declaration required.

    For Arrow types not covered by the built-in aliases, fall back to a raw
    ``Annotated[T, pa.SomeType()]`` annotation; the schema builder resolves
    it transparently.

    Scalar aliases:


    | Alias                        | Python type | Arrow type            |
    |------------------------------|-------------|-----------------------|
    | ``MosaicoType.uint8``        | ``int``     | ``pa.uint8()``        |
    | ``MosaicoType.int8``         | ``int``     | ``pa.int8()``         |
    | ``MosaicoType.uint16``       | ``int``     | ``pa.uint16()``       |
    | ``MosaicoType.int16``        | ``int``     | ``pa.int16()``        |
    | ``MosaicoType.uint32``       | ``int``     | ``pa.uint32()``       |
    | ``MosaicoType.int32``        | ``int``     | ``pa.int32()``        |
    | ``MosaicoType.uint64``       | ``int``     | ``pa.uint64()``       |
    | ``MosaicoType.int64``        | ``int``     | ``pa.int64()``        |
    | ``MosaicoType.float16``      | ``float``   | ``pa.float16()``      |
    | ``MosaicoType.float32``      | ``float``   | ``pa.float32()``      |
    | ``MosaicoType.float64``      | ``float``   | ``pa.float64()``      |
    | ``MosaicoType.bool``         | ``bool``    | ``pa.bool_()``        |
    | ``MosaicoType.string``       | ``str``     | ``pa.string()``       |
    | ``MosaicoType.large_string`` | ``str``     | ``pa.large_string()`` |
    | ``MosaicoType.binary``       | ``bytes``   | ``pa.binary()``       |
    | ``MosaicoType.large_binary`` | ``bytes``   | ``pa.large_binary()`` |


    """

    uint8 = Annotated[int, pa.uint8()]
    int8 = Annotated[int, pa.int8()]

    uint16 = Annotated[int, pa.uint16()]
    int16 = Annotated[int, pa.int16()]

    uint32 = Annotated[int, pa.uint32()]
    int32 = Annotated[int, pa.int32()]

    uint64 = Annotated[int, pa.uint64()]
    int64 = Annotated[int, pa.int64()]

    float16 = Annotated[float, pa.float16()]
    float32 = Annotated[float, pa.float32()]
    float64 = Annotated[float, pa.float64()]

    binary = Annotated[bytes, pa.binary()]
    large_binary = Annotated[bytes, pa.large_binary()]

    bool = Annotated[bool, pa.bool_()]

    string = Annotated[str, pa.string()]
    large_string = Annotated[str, pa.large_string()]

    @staticmethod
    def annotate(py_type: Type, pa_type: pa.DataType) -> Annotated:
        """
        Creates a type metadata binding between a Python type and a Pyarrow DataType.

        This method uses Python's `Annotated` to wrap a standard type with specific
        Pyarrow schema information. This allows Pydantic models to correctly
        serialize and deserialize data into the desired Pyarrow format.

        Args:
            py_type (Type): The native Python type (e.g., int, str, or a Pydantic model).
            pa_type (pa.DataType): The corresponding Pyarrow data type or logical type.

        Returns:
            Annotated: A type hint containing the Python type and Pyarrow metadata.

        """
        return Annotated[py_type, pa_type]

    @staticmethod
    def list_(source_type: Any, list_size: Optional[int] = None) -> Any:
        """
        Build an ``Annotated[list, pa.list_(...)]`` type alias for list fields.

        Accepts either a ``MosaicoType`` alias (i.e. any type carrying
        ``__metadata__`` with a ``pa.DataType``) or a raw Python primitive
        present in ``BASE_MAPPING`` (``int``, ``float``, ``str``, ``bool``,
        ``bytes``).

        Args:
            source_type: A ``MosaicoType`` alias or a raw Python primitive type
                whose PyArrow equivalent is defined in ``BASE_MAPPING``.
            list_size: If provided, produces a fixed-size Arrow list
                (``pa.list_(type, list_size)``). If ``None``, produces a
                variable-length Arrow list (``pa.list_(type)``).

        Returns:
            An ``Annotated[list, pa.ListType]`` alias ready to be used as a
            field annotation in a ``Serializable`` subclass.

        Raises:
            ValueError: If ``source_type`` does not resolve to a valid
                ``pa.DataType``.
        """
        from .serializable import Serializable

        if isinstance(source_type, type) and issubclass(source_type, Serializable):
            pa_type = source_type._build_ontology_struct(source_type)
        else:
            pa_type = (
                source_type.__metadata__[0]
                if hasattr(source_type, "__metadata__")
                else BASE_MAPPING.get(source_type)
            )

        if not isinstance(pa_type, pa.DataType) and not isinstance(
            pa_type, pa.StructType
        ):
            raise ValueError(
                f"Expected a valid pyarrow data/struct type for {source_type}."
            )

        arrow_list_type = (
            pa.list_(pa_type, list_size) if list_size else pa.list_(pa_type)
        )
        return Annotated[list, arrow_list_type]


def MosaicoField(
    nullable: bool = False,
    default: Any | EllipsisType = ...,
    description: Optional[str] = None,
    **kwargs: Any,
) -> Any:
    """
    Factory for Pydantic ``FieldInfo`` instances carrying Mosaico-specific
    Arrow metadata.

    Acts as a drop-in replacement for ``pydantic.Field`` within
    ``Serializable`` subclasses. The ``nullable`` flag is stored in
    ``json_schema_extra`` and consumed by ``_build_ontology_struct`` when
    deriving the ``__msco_pyarrow_struct__`` at class-definition time.

    Args:
        nullable: Whether the corresponding Arrow field should be declared
            as nullable in the generated ``pa.struct``. Defaults to
            ``False``.
        default: Default value for the field. Use ``...`` (the default) to
            mark the field as required. Any other value makes the field
            optional and sets its fallback.
        description: Human-readable description of the field, forwarded to
            Pydantic and surfaced in the JSON Schema output.
        **kwargs: Additional keyword arguments forwarded verbatim to
            ``pydantic.Field``.

    Returns:
        A ``pydantic.FieldInfo`` instance with Mosaico nullability metadata
        attached.
    """

    return Field(
        default=default,
        description=description,
        json_schema_extra={"nullable": nullable if default is not None else True},
        **kwargs,
    )
