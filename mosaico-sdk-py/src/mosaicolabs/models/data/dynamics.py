"""
This module defines specialized ontology structures for representing physical dynamics, specifically linear forces and rotational moments (torques).

The primary structure, [`ForceTorque`][mosaicolabs.models.data.dynamics.ForceTorque], implements a standard "Wrench" representation.
These models are designed to be assigned to the `data` field of a [`Message`][mosaicolabs.models.Message] for transmission to the platform.

**Key Features:**
* **Wrench Representation**: Combines 3D linear force and 3D rotational torque into a single, synchronized state.
* **Uncertainty Quantification**: Inherits from [`CovarianceMixin`][mosaicolabs.models.mixins.CovarianceMixin] to support $6 \times 6$ covariance matrices, allowing for the transmission of sensor noise characteristics or estimation confidence.
"""

import pyarrow as pa

from ..serializable import Serializable
from ..mixins import HeaderMixin, CovarianceMixin
from .geometry import Vector3d


class ForceTorque(
    Serializable,  # Adds Registry/Factory logic
    HeaderMixin,  # Adds Timestamp/Frame info
    CovarianceMixin,  # Adds Covariance matrix support
):
    """
    Represents a Wrench (Force and Torque) applied to a rigid body.

    The `ForceTorque` class is used to describe the total mechanical action (wrench)
    acting on a body at a specific reference point. By combining
    linear force and rotational torque, it provides a complete description of
    dynamics for simulation and telemetry.

    Attributes:
        force: A `Vector3d` representing the linear force vector in Newtons ($N$).
        torque: A `Vector3d` representing the rotational moment vector in Newton-meters (Nm).
        header: Optional metadata header providing temporal and spatial context.
        covariance: Optional flattened 6x6 composed covariance matrix representing
            the uncertainty of the force-torque measurement.
        covariance_type: Enum integer representing the parameterization of the
            covariance matrix.

    Note: Unit Standards
        To ensure platform-wide consistency, all force components should be
        specified in **Newtons** and torque in **Newton-meters**.

    ### Querying with the **`.Q` Proxy**
    This class fields are queryable when constructing a [`QueryOntologyCatalog`][mosaicolabs.models.query.builders.QueryOntologyCatalog]
    via the **`.Q` proxy**. Check the fields documentation for detailed description.

    Example:
        ```python
        from mosaicolabs import MosaicoClient, ForceTorque, QueryOntologyCatalog

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter ForceTorques with force X-component AND torque Z-component
            qresponse = client.query(
                QueryOntologyCatalog(ForceTorque.Q.force.x.gt(5.0))
                    .with_expression(ForceTorque.Q.torque.z.lt(10))
            )

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {[topic.name for topic in item.topics]}")

            # Filter for a specific data value and extract the first and last occurrence times
            qresponse = client.query(
                QueryOntologyCatalog(ForceTorque.Q.force.x.gt(5.0), include_timestamp_range=True)
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

    __msco_pyarrow_struct__ = pa.struct(
        [
            pa.field(
                "force",
                Vector3d.__msco_pyarrow_struct__,
                nullable=False,
                metadata={"description": "3D linear force vector"},
            ),
            pa.field(
                "torque",
                Vector3d.__msco_pyarrow_struct__,
                nullable=False,
                metadata={"description": "3D torque vector"},
            ),
        ]
    )

    force: Vector3d
    """
    3D linear force vector

    ### Querying with the **`.Q` Proxy**
    Force components are queryable through the `force` field prefix.

    | Field Access Path | Queryable Type | Supported Operators |
    | :--- | :--- | :--- |
    | `ForceTorque.Q.force.x` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    | `ForceTorque.Q.force.y` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    | `ForceTorque.Q.force.z` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |

    Example:
        ```python
        from mosaicolabs import MosaicoClient, ForceTorque, QueryOntologyCatalog

        with MosaicoClient.connect("localhost", 6726) as client:
            # Find where the linear X-force exceeds 50N 
            qresponse = client.query(QueryOntologyCatalog(ForceTorque.Q.force.x.gt(50.0)))

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {[topic.name for topic in item.topics]}")

            # Filter for a specific data value and extract the first and last occurrence times
            qresponse = client.query(
                QueryOntologyCatalog(ForceTorque.Q.force.x.gt(5.0), include_timestamp_range=True)
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

    torque: Vector3d
    """
    3D torque vector

    ### Querying with the **`.Q` Proxy**
    Torque components are queryable through the `torque` field prefix.

    | Field Access Path | Queryable Type | Supported Operators |
    | :--- | :--- | :--- |
    | `ForceTorque.Q.torque.x` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    | `ForceTorque.Q.torque.y` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    | `ForceTorque.Q.torque.z` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |

    Example:
        ```python
        from mosaicolabs import MosaicoClient, ForceTorque, QueryOntologyCatalog

        with MosaicoClient.connect("localhost", 6726) as client:
            # Find where the linear Y-torque is small
            qresponse = client.query(QueryOntologyCatalog(ForceTorque.Q.torque.y.lt(0.02)))

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {[topic.name for topic in item.topics]}")

            # Filter for a specific data value and extract the first and last occurrence times
            qresponse = client.query(
                QueryOntologyCatalog(ForceTorque.Q.torque.y.gt(5.0), include_timestamp_range=True)
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
