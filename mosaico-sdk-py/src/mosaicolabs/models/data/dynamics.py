"""
This module defines specialized ontology structures for representing physical dynamics, specifically linear forces and rotational moments (torques).

The primary structure, [`ForceTorque`][mosaicolabs.models.data.dynamics.ForceTorque], implements a standard "Wrench" representation.
These models are designed to be assigned to the `data` field of a [`Message`][mosaicolabs.models.core.Message] for transmission to the platform.

**Key Features:**
* **Wrench Representation**: Combines 3D linear force and 3D rotational torque into a single, synchronized state.
* **Uncertainty Quantification**: Inherits from [`CovarianceMixin`][mosaicolabs.models.data.CovarianceMixin] to support $6 \times 6$ covariance matrices, allowing for the transmission of sensor noise characteristics or estimation confidence.
"""

from ..core import MosaicoField, MosaicoType, Serializable
from .geometry import Vector3d
from .mixins import CovarianceMixin, HeaderMixin


class ForceTorque(
    Serializable,  # Adds Registry/Factory logic
    CovarianceMixin,  # Adds Covariance matrix support
    HeaderMixin,  # Adds header support
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
        covariance: Optional flattened 6x6 composed covariance matrix representing
            the uncertainty of the force-torque measurement.
        covariance_type: Enum integer representing the parameterization of the
            covariance matrix.
        header (optional[Header]): Optional heading containing measurement metadata

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

                    # Clusterize all topics within the sequence to extract the time intervals
                    clusters_dict = item.clusterize_all()

                    # Since clusterize_all() used default clustering_dt_ns, each topic will have
                    # just one cluster representing the first and last moment the query was satisfied
                    for t_name, clusters in clusters_dict.items():
                        print(f"{t_name}:\\n", "\\n".join(f"{cluster}" for cluster in clusters))
        ```
    """

    force: Vector3d = MosaicoField(description="3D linear force.")
    """
    3D linear force vector

    ### Querying with the **`.Q` Proxy**
    Force components are queryable through the `force` field prefix.

    | Field Access Path | Queryable Type | Supported Operators |
    | :--- | :--- | :--- |
    | `ForceTorque.Q.force.x` | `Numeric` | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    | `ForceTorque.Q.force.y` | `Numeric` | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    | `ForceTorque.Q.force.z` | `Numeric` | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |

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

                    # Clusterize all topics within the sequence to extract the time intervals
                    clusters_dict = item.clusterize_all()

                    # Since clusterize_all() used default clustering_dt_ns, each topic will have
                    # just one cluster representing the first and last moment the query was satisfied
                    for t_name, clusters in clusters_dict.items():
                        print(f"{t_name}:\\n", "\\n".join(f"{cluster}" for cluster in clusters))
        ```
    """

    torque: Vector3d = MosaicoField(description="3D torque vector.")
    """
    3D torque vector

    ### Querying with the **`.Q` Proxy**
    Torque components are queryable through the `torque` field prefix.

    | Field Access Path | Queryable Type | Supported Operators |
    | :--- | :--- | :--- |
    | `ForceTorque.Q.torque.x` | `Numeric` | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    | `ForceTorque.Q.torque.y` | `Numeric` | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    | `ForceTorque.Q.torque.z` | `Numeric` | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |

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

                    # Clusterize all topics within the sequence to extract the time intervals
                    clusters_dict = item.clusterize_all()

                    # Since clusterize_all() used default clustering_dt_ns, each topic will have
                    # just one cluster representing the first and last moment the query was satisfied
                    for t_name, clusters in clusters_dict.items():
                        print(f"{t_name}:\\n", "\\n".join(f"{cluster}" for cluster in clusters))
        ```
    """


class Inertia(
    Serializable,
    HeaderMixin,  # Adds header support
):
    """
    Inertia properties of a rigid body.

    Includes mass, center of mass, and inertia tensor.

    Attributes:
        mass: Mass of the object.
        center_of_mass: Center of mass position.
        inertia: Inertia tensor (flattened 3x3 matrix).
        header (optional[Header]): Optional heading containing measurement metadata

    ### Querying with the **`.Q` Proxy**
    This class is fully queryable via the **`.Q` proxy**. `mass` and `center_of_mass` are
    scalar/composite fields queryable directly, while `inertia` is a fixed-size list
    (`list_size=6`) queryable via `all()`, `any()` or index access `[i]` followed by the
    contained type supported operations.
    """

    mass: MosaicoType.float64 = MosaicoField(description="Mass of the object.")
    """
    Mass of the object.

    ### Querying with the **`.Q` Proxy**
    The mass field is queryable.

    | Field Access Path | Queryable Type | Supported Operators |
    | :--- | :--- | :--- |
    | `Inertia.Q.mass` | Numeric | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    """

    center_of_mass: Vector3d = MosaicoField(description="Center of mass of the object.")
    """
    Center of mass of the object.

    ### Querying with the **`.Q` Proxy**
    The center of mass is queryable via its components.

    | Field Access Path | Queryable Type | Supported Operators |
    | :--- | :--- | :--- |
    | `Inertia.Q.center_of_mass.x` | Numeric | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    | `Inertia.Q.center_of_mass.y` | Numeric | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    | `Inertia.Q.center_of_mass.z` | Numeric | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    """

    inertia: MosaicoType.list_(MosaicoType.float64, list_size=6) = MosaicoField(
        description="Inertia tensor components [ixx, ixy, ixz, iyy, iyz, izz]."
    )
    """
    Inertia tensor represented by its 6 unique components [ixx, ixy, ixz, iyy, iyz, izz].

    ### Querying with the **`.Q` Proxy**
    The inertia tensor components are queryable via the `inertia` field. Since it represents a
    list of values, use `all()`, `any()` or index access `[i]` to narrow down to the list
    element and compose a correct expression.
        - Inertia.Q.inertia.all()       -> invalid expression
        - Inertia.Q.inertia.gt(1)       -> invalid expression
        - Inertia.Q.inertia.all().gt(1) -> valid expression

    | Field Access Path | Queryable Type | Supported Operators |
    | :--- | :--- | :--- |
    | `Inertia.Q.inertia.all()` | `Numeric` | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    | `Inertia.Q.inertia.any()` | `Numeric` | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    | `Inertia.Q.inertia.[i]` | `Numeric` | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |

    Example:
        ```python
        from mosaicolabs import MosaicoClient, QueryOntologyCatalog, Inertia

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter for bodies with a specific Ixx moment of inertia (index 0)
            qresponse = client.query(
                QueryOntologyCatalog(Inertia.Q.inertia[0].between([-1.0, 1.0]))
            )

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {[topic.name for topic in item.topics]}")

                    # Clusterize all topics within the sequence to extract the time intervals
                    clusters_dict = item.clusterize_all()

                    # Since clusterize_all() used default clustering_dt_ns, each topic will have
                    # just one cluster representing the first and last moment the query was satisfied
                    for t_name, clusters in clusters_dict.items():
                        print(f"{t_name}:\\n", "\\n".join(f"{cluster}" for cluster in clusters))
        ```









    """
