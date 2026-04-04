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
    def list_(source_type: Any, list_size: Optional[int] = None) -> Any:

        pa_type = (
            source_type.__metadata__[0]
            if hasattr(source_type, "__metadata__")
            else BASE_MAPPING.get(source_type)
        )

        if not isinstance(pa_type, pa.DataType):
            raise ValueError("Expected a valid pyarrow data type for source_type.")

        arrow_list_type = (
            pa.list_(pa_type, list_size) if list_size else pa.list_(pa_type)
        )
        return Annotated[list, arrow_list_type]


def MosaicoField(
    nullable: bool = False,
    default: Any | EllipsisType = ...,
    description: Optional[str] = None,
    **kwargs,
) -> Any:

    return Field(
        default=default,
        description=description,
        json_schema_extra={"nullable": nullable},
        **kwargs,
    )
