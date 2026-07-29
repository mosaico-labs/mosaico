"""
Robot State Module.

Defines the `RobotJoint` model for capturing the state (position, velocity, effort)
of a robot's actuators.
"""

from ..core import MosaicoField, MosaicoType, Serializable
from ..data import HeaderMixin


class RobotJoint(
    Serializable,
    HeaderMixin,  # Adds Header support
):
    """
    Snapshot of robot joint states.

    Arrays must be index-aligned (e.g., names[0] corresponds to positions[0]).

    Attributes:
        names: Names of the different robot joints
        positions: Positions ([rad] or [m]) of the different robot joints
        velocities: Velocities ([rad/s] or [m/s]) of the different robot joints
        efforts: Efforts ([N] or [N/m]) applied to the different robot joints
        header (optional[Header]): Optional heading containing measurement metadata

    ### Querying with the **`.Q` Proxy**
    This class is fully queryable via the **`.Q` proxy**. You can filter robot joint data based
    on thresholds values within a [`QueryOntologyCatalog`][mosaicolabs.query.builders.QueryOntologyCatalog].
    Since `names`, `positions`, `velocities` and `efforts` are lists of values, use `all()`,
    `any()` or index access `[i]` to narrow down to the list element and compose a correct
    expression.
    """

    names: MosaicoType.list_(MosaicoType.string) = MosaicoField(
        description="Names of the different robot joints"
    )
    """
    Names of the different robot joints

    ### Querying with the **`.Q` Proxy**
    The names value is queryable via the `names` field. Since it represents a list of values,
    use `all()`, `any()` or index access `[i]` to narrow down to the list element and compose a
    correct expression.
        - RobotJoint.Q.names.all()          -> invalid expression
        - RobotJoint.Q.names.eq("joint1")   -> invalid expression
        - RobotJoint.Q.names.any().eq("joint1") -> valid expression

    | Field Access Path | Queryable Type | Supported Operators |
    | :--- | :--- | :--- |
    | `RobotJoint.Q.names.all()` | `String` | `.eq()`, `.match()`, `.in_()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.between()`, `.outside()` |
    | `RobotJoint.Q.names.any()` | `String` | `.eq()`, `.match()`, `.in_()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.between()`, `.outside()` |
    | `RobotJoint.Q.names.[i]` | `String` | `.eq()`, `.match()`, `.in_()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.between()`, `.outside()` |

    Example:
        ```python
        from mosaicolabs import MosaicoClient, QueryOntologyCatalog, RobotJoint

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter for robots that report a specific joint name
            qresponse = client.query(
                QueryOntologyCatalog(RobotJoint.Q.names.any().eq("shoulder_pan_joint"))
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

    positions: MosaicoType.list_(MosaicoType.float64) = MosaicoField(
        description="Positions ([rad] or [m]) of the different robot joints"
    )
    """
    Positions ([rad] or [m]) of the different robot joints

    ### Querying with the **`.Q` Proxy**
    The positions value is queryable via the `positions` field. Since it represents a list of
    values, use `all()`, `any()` or index access `[i]` to narrow down to the list element and
    compose a correct expression.
        - RobotJoint.Q.positions.all()       -> invalid expression
        - RobotJoint.Q.positions.gt(1)       -> invalid expression
        - RobotJoint.Q.positions.all().gt(1) -> valid expression

    | Field Access Path | Queryable Type | Supported Operators |
    | :--- | :--- | :--- |
    | `RobotJoint.Q.positions.all()` | `Numeric` | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()`, `.outside()` |
    | `RobotJoint.Q.positions.any()` | `Numeric` | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()`, `.outside()` |
    | `RobotJoint.Q.positions.[i]` | `Numeric` | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()`, `.outside()` |

    Example:
        ```python
        from mosaicolabs import MosaicoClient, QueryOntologyCatalog, RobotJoint

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter for robots with at least one joint beyond a position limit
            qresponse = client.query(
                QueryOntologyCatalog(RobotJoint.Q.positions.any().gt(3.14))
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

    velocities: MosaicoType.list_(MosaicoType.float64) = MosaicoField(
        description="Velocities ([rad/s] or [m/s]) of the different robot joints"
    )
    """
    Velocities ([rad/s] or [m/s]) of the different robot joints

    ### Querying with the **`.Q` Proxy**
    The velocities value is queryable via the `velocities` field. Since it represents a list of
    values, use `all()`, `any()` or index access `[i]` to narrow down to the list element and
    compose a correct expression.
        - RobotJoint.Q.velocities.all()       -> invalid expression
        - RobotJoint.Q.velocities.gt(1)       -> invalid expression
        - RobotJoint.Q.velocities.all().gt(1) -> valid expression

    | Field Access Path | Queryable Type | Supported Operators |
    | :--- | :--- | :--- |
    | `RobotJoint.Q.velocities.all()` | `Numeric` | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()`, `.outside()` |
    | `RobotJoint.Q.velocities.any()` | `Numeric` | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()`, `.outside()` |
    | `RobotJoint.Q.velocities.[i]` | `Numeric` | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()`, `.outside()` |

    Example:
        ```python
        from mosaicolabs import MosaicoClient, QueryOntologyCatalog, RobotJoint

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter for robots with at least one fast-moving joint
            qresponse = client.query(
                QueryOntologyCatalog(RobotJoint.Q.velocities.any().gt(2.0))
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

    efforts: MosaicoType.list_(MosaicoType.float64) = MosaicoField(
        description="Efforts ([N] or [N*m]) applied to the different robot joints"
    )
    """
    Efforts ([N] or [N*m]) applied to the different robot joints

    ### Querying with the **`.Q` Proxy**
    The efforts value is queryable via the `efforts` field. Since it represents a list of
    values, use `all()`, `any()` or index access `[i]` to narrow down to the list element and
    compose a correct expression.
        - RobotJoint.Q.efforts.all()       -> invalid expression
        - RobotJoint.Q.efforts.gt(1)       -> invalid expression
        - RobotJoint.Q.efforts.all().gt(1) -> valid expression

    | Field Access Path | Queryable Type | Supported Operators |
    | :--- | :--- | :--- |
    | `RobotJoint.Q.efforts.all()` | `Numeric` | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()`, `.outside()` |
    | `RobotJoint.Q.efforts.any()` | `Numeric` | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()`, `.outside()` |
    | `RobotJoint.Q.efforts.[i]` | `Numeric` | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()`, `.outside()` |

    Example:
        ```python
        from mosaicolabs import MosaicoClient, QueryOntologyCatalog, RobotJoint

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter for robots with at least one joint under high load
            qresponse = client.query(
                QueryOntologyCatalog(RobotJoint.Q.efforts.any().gt(50.0))
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
