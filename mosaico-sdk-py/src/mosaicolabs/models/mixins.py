"""
Header Mixin Module.

This module provides `HeaderMixin`, a helper class used to inject standard
header fields into ontology models via composition.
"""

from typing import List, Optional
import pyarrow as pa

from .base_model import BaseModel
from .header import Header

# ---- HeaderMixin ----


class HeaderMixin(BaseModel):
    """
    A Mixin that adds a `header` field to any data model inheriting from it.

    **Mechanism:**
    It uses the `__init_subclass__` hook to inspect the child class's existing
    PyArrow struct, appends the 'header' field definition, and updates the struct.
    This ensures that the PyArrow schema matches the Pydantic fields.
    """

    header: Optional[Header] = None

    def __init_subclass__(cls, **kwargs):
        """
        Automatically updates the child class's PyArrow schema to include 'header'.
        """
        super().__init_subclass__(**kwargs)

        # Define the PyArrow field definition for the header
        _HEADER_FIELD = pa.field(
            "header",
            Header.__msco_pyarrow_struct__,
            nullable=True,
            metadata={"description": "The standard metadata header (optional)."},
        )

        # Retrieve existing schema fields from the child class
        current_pa_fields = []
        if hasattr(cls, "__msco_pyarrow_struct__") and isinstance(
            cls.__msco_pyarrow_struct__, pa.StructType
        ):
            current_pa_fields = list(cls.__msco_pyarrow_struct__)

        # Collision Check
        existing_pa_names = [f.name for f in current_pa_fields]
        if "header" in existing_pa_names:
            raise ValueError(
                f"Class '{cls.__name__}' has conflicting 'header' schema key."
            )

        # Append and Update
        new_fields = current_pa_fields + [_HEADER_FIELD]
        cls.__msco_pyarrow_struct__ = pa.struct(new_fields)


# ---- CovarianceMixin ----


class CovarianceMixin(BaseModel):
    """
    A Mixin that adds optional `covariance` and `covariance_type` fields to data models.
    Useful for sensors like IMUs or Odometry that provide uncertainty measurements.
    """

    covariance: Optional[List[float]] = None
    covariance_type: Optional[int] = None

    def __init_subclass__(cls, **kwargs):
        """
        Automatically updates the child class's PyArrow schema to include covariance fields.
        """
        super().__init_subclass__(**kwargs)

        # Define the fields to inject
        _FIELDS = [
            pa.field(
                "covariance",
                pa.list_(value_type=pa.float64()),
                nullable=True,
                metadata={
                    "description": "The covariance matrix (flattened) of the data."
                },
            ),
            pa.field(
                "covariance_type",
                pa.int16(),
                nullable=True,
                metadata={
                    "description": "Enum integer representing the covariance parameterization."
                },
            ),
        ]

        # Retrieve existing schema fields
        current_pa_fields = []
        if hasattr(cls, "__msco_pyarrow_struct__") and isinstance(
            cls.__msco_pyarrow_struct__, pa.StructType
        ):
            current_pa_fields = list(cls.__msco_pyarrow_struct__)

        # Collision Check
        existing_pa_names = [f.name for f in current_pa_fields]
        if "covariance" in existing_pa_names or "covariance_type" in existing_pa_names:
            raise ValueError(
                f"Class '{cls.__name__}' has conflicting 'covariance' or 'covariance_type' schema keys."
            )

        # Append and Update
        new_fields = current_pa_fields + _FIELDS
        cls.__msco_pyarrow_struct__ = pa.struct(new_fields)


# ---- VarianceMixin ----


class VarianceMixin(BaseModel):
    """
    A Mixin that adds optional `variance` and `variance_type` fields to data models.
    Useful for sensors with 1-dimensional uncertain data.
    """

    variance: Optional[float] = None
    variance_type: Optional[int] = None

    def __init_subclass__(cls, **kwargs):
        """
        Automatically updates the child class's PyArrow schema to include variance fields.
        """
        super().__init_subclass__(**kwargs)

        # Define the fields to inject
        _FIELDS = [
            pa.field(
                "variance",
                pa.float64(),
                nullable=True,
                metadata={"description": "The variance of the data."},
            ),
            pa.field(
                "variance_type",
                pa.int16(),
                nullable=True,
                metadata={
                    "description": "Enum integer representing the variance parameterization."
                },
            ),
        ]

        # Retrieve existing schema fields
        current_pa_fields = []
        if hasattr(cls, "__msco_pyarrow_struct__") and isinstance(
            cls.__msco_pyarrow_struct__, pa.StructType
        ):
            current_pa_fields = list(cls.__msco_pyarrow_struct__)

        # Collision Check
        existing_pa_names = [f.name for f in current_pa_fields]
        if "variance" in existing_pa_names or "variance_type" in existing_pa_names:
            raise ValueError(
                f"Class '{cls.__name__}' has conflicting 'variance' or 'variance_type' schema keys."
            )

        # Append and Update
        new_fields = current_pa_fields + _FIELDS
        cls.__msco_pyarrow_struct__ = pa.struct(new_fields)
