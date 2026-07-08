"""
Magnetometer Ontology Module.

Defines the data structure for magnetic field sensors.
"""

from ..core import MosaicoField, Serializable
from ..data import HeaderMixin, Vector3d


class Magnetometer(
    Serializable,
    HeaderMixin,  # Adds Header support
):
    """
    Magnetic field measurement data.

    This class represents the magnetic field measurements from a magnetometer sensor.

    Attributes:
        magnetic_field: Magnetic field vector [mx, my, mz] in microTesla.
        header (optional[Header]): Optional heading containing measurement metadata

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

            # FIXME: Add here example for timestamp exytraction and clustering
        ```
    """

    magnetic_field: Vector3d = MosaicoField(
        description="Magnetic field vector [mx, my, mz] in microTesla."
    )
    """
    Magnetic field vector [mx, my, mz] in microTesla.

    ### Querying with the **`.Q` Proxy**
    The magnetic field vector is queryable via the `magnetic_field` field.

    | Field Access Path | Queryable Type | Supported Operators |
    | :--- | :--- | :--- |
    | `Magnetometer.Q.magnetic_field.x` | `Numeric` | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    | `Magnetometer.Q.magnetic_field.y` | `Numeric` | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    | `Magnetometer.Q.magnetic_field.z` | `Numeric` | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |

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
            
            # FIXME: Add here example for timestamp exytraction and clustering
        ```
    """
