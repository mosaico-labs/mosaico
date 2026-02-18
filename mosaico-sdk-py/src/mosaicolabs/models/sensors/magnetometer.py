"""
Magnetometer Ontology Module.

Defines the data structure for magnetic field sensors.
"""

import pyarrow as pa

from ..mixins import HeaderMixin
from ..serializable import Serializable
from ..data import Vector3d


class Magnetometer(Serializable, HeaderMixin):
    """
    Magnetic field measurement data.

    This class represents the magnetic field measurements from a magnetometer sensor.

    Attributes:
        magnetic_field: Magnetic field vector [mx, my, mz] in microTesla.
        header: Standard metadata providing temporal and spatial reference.

    ### Querying with the **`.Q` Proxy**
    This class is fully queryable via the **`.Q` proxy**. You can filter magnetometer data based
    on magnetic field values within a [`QueryOntologyCatalog`][mosaicolabs.models.query.builders.QueryOntologyCatalog].

    Example:
        ```python
        from mosaicolabs import MosaicoClient, Magnetometer, QueryOntologyCatalog

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter for magnetic field values within a specific range
            qresponse = client.query(
                QueryOntologyCatalog(Magnetometer.Q.magnetic_field.x.between(-100, 100))
            )

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {[topic.name for topic in item.topics]}")

            # Filter for a specific component value and extract the first and last occurrence times
            qresponse = client.query(
                QueryOntologyCatalog(Magnetometer.Q.magnetic_field.x.between(-100, 100), include_timestamp_range=True)
            )

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {{topic.name:
                                [topic.timestamp_range.start, topic.timestamp_range.end]
                                for topic in item.topics}}")
        ```
    """

    # --- Schema Definition ---
    __msco_pyarrow_struct__ = pa.struct(
        [
            pa.field(
                "magnetic_field",
                Vector3d.__msco_pyarrow_struct__,
                nullable=False,
                metadata={
                    "description": "Magnetic field vector [mx, my, mz] in microTesla."
                },
            ),
        ]
    )

    magnetic_field: Vector3d
    """
    Magnetic field vector [mx, my, mz] in microTesla.

    ### Querying with the **`.Q` Proxy**
    The magnetic field vector is queryable via the `magnetic_field` field.

    | Field Access Path | Queryable Type | Supported Operators |
    | :--- | :--- | :--- |
    | `Magnetometer.Q.magnetic_field.x` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    | `Magnetometer.Q.magnetic_field.y` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    | `Magnetometer.Q.magnetic_field.z` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |

    Example:
        ```python
        from mosaicolabs import MosaicoClient, Magnetometer, QueryOntologyCatalog

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter for magnetic field values within a specific range
            qresponse = client.query(
                QueryOntologyCatalog(Magnetometer.Q.magnetic_field.x.between(-100, 100))
            )

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {[topic.name for topic in item.topics]}")
            
            # Filter for a specific component value and extract the first and last occurrence times
            qresponse = client.query(
                QueryOntologyCatalog(Magnetometer.Q.magnetic_field.x.between(-100, 100), include_timestamp_range=True)
            )

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {{topic.name:
                                [topic.timestamp_range.start, topic.timestamp_range.end]
                                for topic in item.topics}}")
        ```
    """
