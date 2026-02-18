"""
Base Model Module.

This module defines the foundational class for all data models within the SDK.
It bridges the gap between Pydantic (used for runtime data validation and Pythonic interaction)
and PyArrow (used for efficient wire transport and columnar storage).
"""

import pyarrow as pa
import pydantic


class BaseModel(pydantic.BaseModel):
    """
    The root base class for SDK data models.

    It inherits from `pydantic.BaseModel` to provide runtime type checking and
    initialization logic. It adds a hook for defining the corresponding
    PyArrow structure (`__msco_pyarrow_struct__`), enabling the SDK to auto-generate
    Flight schemas.

    Note:
        This class has been added mainly for wrapping pydantic, toward future
        implementation where other fields mapping and checks are implemented
    """

    # A class-level attribute defining the PyArrow struct schema for this model.
    # Subclasses must override this to define their specific serialization layout.
    __msco_pyarrow_struct__ = pa.struct([])
    pass
