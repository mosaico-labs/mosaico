"""
LiDAR Ontology Model.

This module defines the LiDAR ontology model, which represents a 3D point cloud
obtained from a LiDAR sensor.
"""

from typing import Optional

from ..core import MosaicoField, MosaicoType, Serializable
from ..data import HeaderMixin


class Lidar(
    Serializable,
    HeaderMixin,  # Adds Header support
):
    """
    LiDAR Ontology.

    This model represents a 3D point cloud acquired from a LiDAR sensor.
    Each field is a flat list whose *i*-th element corresponds to the *i*-th point
    in the scan. All lists within a single instance are therefore guaranteed to have
    the same length.

    Attributes:
        x: X coordinates of each point in meters.
        y: Y coordinates of each point in meters.
        z: Z coordinates of each point in meters.
        intensity: Strength of the returned signal for each point (optional).
        reflectivity: Surface reflectivity per point (optional).
        beam_id: Laser beam index (ring / channel / line) that fired each point (optional).
        range: Distance from the sensor origin to each point in meters (optional).
        near_ir: Near-infrared ambient light reading per point, useful as a noise/ambient
            estimate (optional).
        azimuth: Azimuth angle in radians for each point (optional).
        elevation: Elevation angle in radians for each point (optional).
        confidence: Per-point validity or confidence flags as a manufacturer-specific
            bitmask (optional).
        return_type: Single/dual return classification, manufacturer-specific (optional).
        point_timestamp: Per-point acquisition time offset from the scan start,
            in seconds (optional).

    ### Querying with the **`.Q` Proxy**
    This class is fully queryable via the **`.Q` proxy**. You can filter Lidar data based
    on thresholds values within a [`QueryOntologyCatalog`][mosaicolabs.query.builders.QueryOntologyCatalog].
    Expressions entailing lists of values can be queried using any between `all()`, `any()`
    or index access `[i]` followed by the contained type supported operations.

    Example:
        ```python
        from mosaicolabs import MosaicoClient, QueryTopic
        from mosaicolabs.models.futures import Lidar

        with MosaicoClient.connect("localhost", 6726) as client:
            # Fetch all sequences that contain at least one Lidar topic
            qresponse = client.query(QueryTopic().with_ontology_tag(Lidar.ontology_tag()))

            if qresponse is not None:

                for item in qresponse.items:
                    print(f"Sequence: {item.name}")
                    print(f"Topics:   {[topic.name for topic in item.topics]}")
        ```
    """

    x: MosaicoType.list_(MosaicoType.float32) = MosaicoField(
        description="x coordinates in meters"
    )
    """
    X coordinates of each point in the cloud, in meters.

    ### Querying with the **`.Q` Proxy**
    The X cordinates value are queryable via the `x` field. Since it represents a list of values,
    use `all()`, `any()` or index access `[i]` to narrow down to the list element and compose a
    correct expression. 
        - Lidar.Q.x.all()       -> invalid expression 
        - Lidar.Q.x.gt(1)       -> invalid expression 
        - Lidar.Q.x.all().gt(1) -> valid expression 

    | Field Access Path | Queryable Type | Supported Operators |
    | :--- | :--- | :--- |
    | `Lidar.Q.x.all()` | `Numeric` | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    | `Lidar.Q.x.any()` | `Numeric` | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    | `Lidar.Q.x.[i]` | `Numeric` | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |

    Example:
        ```python
        from mosaicolabs import MosaicoClient, QueryOntologyCatalog
        from mosaicolabs.models.futures import Lidar

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter for Lidar values on X within a specific range
            qresponse = client.query(
                QueryOntologyCatalog(Lidar.Q.x.all().between([-1.0, 1.0]))
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

    y: MosaicoType.list_(MosaicoType.float32) = MosaicoField(
        description="y coordinates in meters"
    )
    """
    Y coordinates of each point in the cloud, in meters.

    ### Querying with the **`.Q` Proxy**
    The Y cordinates value are queryable via the `y` field. Since it represents a list of values,
    use `all()`, `any()` or index access `[i]` to narrow down to the list element and compose a
    correct expression. 
        - Lidar.Q.y.all()       -> invalid expression 
        - Lidar.Q.y.gt(1)       -> invalid expression 
        - Lidar.Q.y.all().gt(1) -> valid expression 

    | Field Access Path | Queryable Type | Supported Operators |
    | :--- | :--- | :--- |
    | `Lidar.Q.y.all()` | `Numeric` | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    | `Lidar.Q.y.any()` | `Numeric` | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    | `Lidar.Q.y.[i]` | `Numeric` | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |

    Example:
        ```python
        from mosaicolabs import MosaicoClient, QueryOntologyCatalog
        from mosaicolabs.models.futures import Lidar

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter for Lidar values on Y within a specific range
            qresponse = client.query(
                QueryOntologyCatalog(Lidar.Q.y.all().between([-1.0, 1.0]))
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

    z: MosaicoType.list_(MosaicoType.float32) = MosaicoField(
        description="z coordinates in meters"
    )
    """
    Z coordinates of each point in the cloud, in meters.

    ### Querying with the **`.Q` Proxy**
    The Z cordinates value are queryable via the `y` field. Since it represents a list of values,
    use `all()`, `any()` or index access `[i]` to narrow down to the list element and compose a
    correct expression. 
        - Lidar.Q.z.all()       -> invalid expression 
        - Lidar.Q.z.gt(1)       -> invalid expression 
        - Lidar.Q.z.all().gt(1) -> valid expression 

    | Field Access Path | Queryable Type | Supported Operators |
    | :--- | :--- | :--- |
    | `Lidar.Q.z.all()` | `Numeric` | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    | `Lidar.Q.z.any()` | `Numeric` | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    | `Lidar.Q.z.[i]` | `Numeric` | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |

    Example:
        ```python
        from mosaicolabs import MosaicoClient, QueryOntologyCatalog
        from mosaicolabs.models.futures import Lidar

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter for Lidar values on Z within a specific range
            qresponse = client.query(
                QueryOntologyCatalog(Lidar.Q.z.all().between([-1.0, 1.0]))
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

    intensity: Optional[MosaicoType.list_(MosaicoType.float32)] = MosaicoField(
        default=None,
        description="Surface reflectivity per point.",
    )
    """
    Strength of the returned laser signal for each point.

    ### Querying with the **`.Q` Proxy**
    The intensity value is queryable via the `intensity` field. Since it represents a list of
    values, use `all()`, `any()` or index access `[i]` to narrow down to the list element and
    compose a correct expression.
        - Lidar.Q.intensity.all()       -> invalid expression
        - Lidar.Q.intensity.gt(1)       -> invalid expression
        - Lidar.Q.intensity.all().gt(1) -> valid expression

    | Field Access Path | Queryable Type | Supported Operators |
    | :--- | :--- | :--- |
    | `Lidar.Q.intensity.all()` | `Numeric` | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    | `Lidar.Q.intensity.any()` | `Numeric` | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    | `Lidar.Q.intensity.[i]` | `Numeric` | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |

    Example:
        ```python
        from mosaicolabs import MosaicoClient, QueryOntologyCatalog
        from mosaicolabs.models.futures import Lidar

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter for Lidar points with at least one strong return
            qresponse = client.query(
                QueryOntologyCatalog(Lidar.Q.intensity.any().gt(200.0))
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

    reflectivity: Optional[MosaicoType.list_(MosaicoType.uint16)] = MosaicoField(
        default=None,
        description="Surface reflectivity per point.",
    )
    """
    Surface reflectivity per point.

    Encodes the estimated reflectance of the surface that produced each return,
    independently of the distance. Manufacturer-specific scaling applies.

    ### Querying with the **`.Q` Proxy**
    The reflectivity value is queryable via the `reflectivity` field. Since it represents a list
    of values, use `all()`, `any()` or index access `[i]` to narrow down to the list element and
    compose a correct expression.
        - Lidar.Q.reflectivity.all()       -> invalid expression
        - Lidar.Q.reflectivity.gt(1)       -> invalid expression
        - Lidar.Q.reflectivity.all().gt(1) -> valid expression

    | Field Access Path | Queryable Type | Supported Operators |
    | :--- | :--- | :--- |
    | `Lidar.Q.reflectivity.all()` | `Numeric` | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    | `Lidar.Q.reflectivity.any()` | `Numeric` | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    | `Lidar.Q.reflectivity.[i]` | `Numeric` | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |

    Example:
        ```python
        from mosaicolabs import MosaicoClient, QueryOntologyCatalog
        from mosaicolabs.models.futures import Lidar

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter for Lidar points with high surface reflectivity
            qresponse = client.query(
                QueryOntologyCatalog(Lidar.Q.reflectivity.any().geq(200))
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

    beam_id: Optional[MosaicoType.list_(MosaicoType.uint16)] = MosaicoField(
        default=None,
        description="Laser beam index (ring / channel / line) that fired each point.",
    )
    """
    Laser beam index (ring / channel / line) that fired each point.

    Identifies which physical emitter in the sensor array produced the return.
    Equivalent to the `ring` field commonly found in ROS `PointCloud2` messages
    from multi-beam sensors such as Velodyne or Ouster.

    ### Querying with the **`.Q` Proxy**
    The beam id value is queryable via the `beam_id` field. Since it represents a list of values,
    use `all()`, `any()` or index access `[i]` to narrow down to the list element and compose a
    correct expression.
        - Lidar.Q.beam_id.all()       -> invalid expression
        - Lidar.Q.beam_id.gt(1)       -> invalid expression
        - Lidar.Q.beam_id.all().gt(1) -> valid expression

    | Field Access Path | Queryable Type | Supported Operators |
    | :--- | :--- | :--- |
    | `Lidar.Q.beam_id.all()` | `Numeric` | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    | `Lidar.Q.beam_id.any()` | `Numeric` | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    | `Lidar.Q.beam_id.[i]` | `Numeric` | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |

    Example:
        ```python
        from mosaicolabs import MosaicoClient, QueryOntologyCatalog
        from mosaicolabs.models.futures import Lidar

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter for Lidar points fired by a specific beam
            qresponse = client.query(
                QueryOntologyCatalog(Lidar.Q.beam_id.any().eq(0))
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

    range: Optional[MosaicoType.list_(MosaicoType.float32)] = MosaicoField(
        default=None,
        description="Distance from the sensor origin to each point, in meters.",
    )
    """
    Distance from the sensor origin to each point, in meters.

    Represents the raw radial distance along the beam axis, before projection
    onto Cartesian coordinates. Not always provided by all sensor drivers.

    ### Querying with the **`.Q` Proxy**
    The range value is queryable via the `range` field. Since it represents a list of values,
    use `all()`, `any()` or index access `[i]` to narrow down to the list element and compose a
    correct expression.
        - Lidar.Q.range.all()       -> invalid expression
        - Lidar.Q.range.gt(1)       -> invalid expression
        - Lidar.Q.range.all().gt(1) -> valid expression

    | Field Access Path | Queryable Type | Supported Operators |
    | :--- | :--- | :--- |
    | `Lidar.Q.range.all()` | `Numeric` | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    | `Lidar.Q.range.any()` | `Numeric` | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    | `Lidar.Q.range.[i]` | `Numeric` | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |

    Example:
        ```python
        from mosaicolabs import MosaicoClient, QueryOntologyCatalog
        from mosaicolabs.models.futures import Lidar

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter for Lidar points within 50 meters of the sensor
            qresponse = client.query(
                QueryOntologyCatalog(Lidar.Q.range.all().leq(50.0))
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

    near_ir: Optional[MosaicoType.list_(MosaicoType.float32)] = MosaicoField(
        default=None,
        description="Near-infrared ambient light reading per point.",
    )
    """
    Near-infrared ambient light reading per point.

    Captured passively by the sensor between laser pulses. Useful as a proxy
    for ambient illumination or for filtering sun-noise artefacts.
    Exposed as the `ambient` channel in Ouster drivers.

    ### Querying with the **`.Q` Proxy**
    The near-infrared value is queryable via the `near_ir` field. Since it represents a list of
    values, use `all()`, `any()` or index access `[i]` to narrow down to the list element and
    compose a correct expression.
        - Lidar.Q.near_ir.all()       -> invalid expression
        - Lidar.Q.near_ir.gt(1)       -> invalid expression
        - Lidar.Q.near_ir.all().gt(1) -> valid expression

    | Field Access Path | Queryable Type | Supported Operators |
    | :--- | :--- | :--- |
    | `Lidar.Q.near_ir.all()` | `Numeric` | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    | `Lidar.Q.near_ir.any()` | `Numeric` | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    | `Lidar.Q.near_ir.[i]` | `Numeric` | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |

    Example:
        ```python
        from mosaicolabs import MosaicoClient, QueryOntologyCatalog
        from mosaicolabs.models.futures import Lidar

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter for Lidar points captured under strong ambient IR light
            qresponse = client.query(
                QueryOntologyCatalog(Lidar.Q.near_ir.any().gt(500.0))
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

    azimuth: Optional[MosaicoType.list_(MosaicoType.float32)] = MosaicoField(
        default=None,
        description="Horizontal (azimuth) angle of each point in radians.",
    )
    """
    Horizontal (azimuth) angle of each point in radians.

    ### Querying with the **`.Q` Proxy**
    The azimuth value is queryable via the `azimuth` field. Since it represents a list of values,
    use `all()`, `any()` or index access `[i]` to narrow down to the list element and compose a
    correct expression.
        - Lidar.Q.azimuth.all()       -> invalid expression
        - Lidar.Q.azimuth.gt(1)       -> invalid expression
        - Lidar.Q.azimuth.all().gt(1) -> valid expression

    | Field Access Path | Queryable Type | Supported Operators |
    | :--- | :--- | :--- |
    | `Lidar.Q.azimuth.all()` | `Numeric` | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    | `Lidar.Q.azimuth.any()` | `Numeric` | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    | `Lidar.Q.azimuth.[i]` | `Numeric` | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |

    Example:
        ```python
        from mosaicolabs import MosaicoClient, QueryOntologyCatalog
        from mosaicolabs.models.futures import Lidar

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter for Lidar points within a specific azimuth range
            qresponse = client.query(
                QueryOntologyCatalog(Lidar.Q.azimuth.all().between([-1.57, 1.57]))
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

    elevation: Optional[MosaicoType.list_(MosaicoType.float32)] = MosaicoField(
        default=None,
        description="Vertical (elevation) angle of each point in radians.",
    )
    """
    Vertical (elevation) angle of each point in radians.

    ### Querying with the **`.Q` Proxy**
    The elevation value is queryable via the `elevation` field. Since it represents a list of
    values, use `all()`, `any()` or index access `[i]` to narrow down to the list element and
    compose a correct expression.
        - Lidar.Q.elevation.all()       -> invalid expression
        - Lidar.Q.elevation.gt(1)       -> invalid expression
        - Lidar.Q.elevation.all().gt(1) -> valid expression

    | Field Access Path | Queryable Type | Supported Operators |
    | :--- | :--- | :--- |
    | `Lidar.Q.elevation.all()` | `Numeric` | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    | `Lidar.Q.elevation.any()` | `Numeric` | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    | `Lidar.Q.elevation.[i]` | `Numeric` | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |

    Example:
        ```python
        from mosaicolabs import MosaicoClient, QueryOntologyCatalog
        from mosaicolabs.models.futures import Lidar

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter for Lidar points within a specific elevation range
            qresponse = client.query(
                QueryOntologyCatalog(Lidar.Q.elevation.all().between([-0.26, 0.26]))
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

    confidence: Optional[MosaicoType.list_(MosaicoType.uint8)] = MosaicoField(
        default=None,
        description="Per-point validity or confidence flags.",
    )
    """
    Per-point validity or confidence flags.
    
    Stored as a manufacturer-specific bitmask (equivalent to the `tag` or
    `flags` fields in Ouster point clouds). Individual bits may signal
    saturated returns, calibration issues, or other quality indicators.

    ### Querying with the **`.Q` Proxy**
    The confidence value is queryable via the `confidence` field. Since it represents a list of
    values, use `all()`, `any()` or index access `[i]` to narrow down to the list element and
    compose a correct expression.
        - Lidar.Q.confidence.all()       -> invalid expression
        - Lidar.Q.confidence.gt(1)       -> invalid expression
        - Lidar.Q.confidence.all().gt(1) -> valid expression

    | Field Access Path | Queryable Type | Supported Operators |
    | :--- | :--- | :--- |
    | `Lidar.Q.confidence.all()` | `Numeric` | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    | `Lidar.Q.confidence.any()` | `Numeric` | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    | `Lidar.Q.confidence.[i]` | `Numeric` | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |

    Example:
        ```python
        from mosaicolabs import MosaicoClient, QueryOntologyCatalog
        from mosaicolabs.models.futures import Lidar

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter for Lidar points with at least one low-confidence flag
            qresponse = client.query(
                QueryOntologyCatalog(Lidar.Q.confidence.any().lt(10))
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

    return_type: Optional[MosaicoType.list_(MosaicoType.uint8)] = MosaicoField(
        default=None,
        description="Single/dual return classification per point.",
    )
    """
    Single/dual return classification per point.
    
    Indicates whether a point originates from the first return, last return,
    strongest return, etc. Encoding is manufacturer-specific.

    ### Querying with the **`.Q` Proxy**
    The return type value is queryable via the `return_type` field. Since it represents a list
    of values, use `all()`, `any()` or index access `[i]` to narrow down to the list element and
    compose a correct expression.
        - Lidar.Q.return_type.all()       -> invalid expression
        - Lidar.Q.return_type.gt(1)       -> invalid expression
        - Lidar.Q.return_type.all().gt(1) -> valid expression

    | Field Access Path | Queryable Type | Supported Operators |
    | :--- | :--- | :--- |
    | `Lidar.Q.return_type.all()` | `Numeric` | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    | `Lidar.Q.return_type.any()` | `Numeric` | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    | `Lidar.Q.return_type.[i]` | `Numeric` | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |

    Example:
        ```python
        from mosaicolabs import MosaicoClient, QueryOntologyCatalog
        from mosaicolabs.models.futures import Lidar

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter for Lidar points with a specific return classification
            qresponse = client.query(
                QueryOntologyCatalog(Lidar.Q.return_type.any().eq(1))
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

    point_timestamp: Optional[MosaicoType.list_(MosaicoType.float64)] = MosaicoField(
        default=None,
        description="Per-point acquisition time offset from the scan start, in seconds.",
    )
    """
    Per-point acquisition time offset from the scan start, in seconds.
    
    Allows precise temporal localisation of individual points within a single
    sweep, which is important for motion-distortion correction during
    point-cloud registration.

    ### Querying with the **`.Q` Proxy**
    The point timestamp value is queryable via the `point_timestamp` field. Since it represents
    a list of values, use `all()`, `any()` or index access `[i]` to narrow down to the list
    element and compose a correct expression.
        - Lidar.Q.point_timestamp.all()       -> invalid expression
        - Lidar.Q.point_timestamp.gt(1)       -> invalid expression
        - Lidar.Q.point_timestamp.all().gt(1) -> valid expression

    | Field Access Path | Queryable Type | Supported Operators |
    | :--- | :--- | :--- |
    | `Lidar.Q.point_timestamp.all()` | `Numeric` | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    | `Lidar.Q.point_timestamp.any()` | `Numeric` | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    | `Lidar.Q.point_timestamp.[i]` | `Numeric` | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |

    Example:
        ```python
        from mosaicolabs import MosaicoClient, QueryOntologyCatalog
        from mosaicolabs.models.futures import Lidar

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter for Lidar points acquired within the first 10ms of the sweep
            qresponse = client.query(
                QueryOntologyCatalog(Lidar.Q.point_timestamp.all().leq(0.01))
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
