"""
Radar Ontology Model.

This module defines the Radar ontology model, which represents a sparse point cloud
of detections obtained from a Radar sensor.
"""

from typing import Optional

from ..core import MosaicoField, MosaicoType, Serializable
from ..data import HeaderMixin


class Radar(
    Serializable,
    HeaderMixin,  # Adds Header support
):
    """
    Radar Ontology.

    This model represents a set of detections acquired from a Radar sensor. Each
    detection corresponds to a target or a reflection point in the sensor's field
    of view, characterised by its position, optional velocity, and signal-quality
    metrics.

    Each field is a flat list whose *i*-th element corresponds to the *i*-th
    detection in the scan.

    Unlike a LiDAR, Radar detections are inherently sparse and carry
    additional electromagnetic attributes such as Radar Cross Section (RCS),
    Signal-to-Noise Ratio (SNR), and Doppler velocity, which are not available
    from purely optical sensors.

    Attributes:
        x: X coordinates of each detection in meters.
        y: Y coordinates of each detection in meters.
        z: Z coordinates of each detection in meters.
        range: Radial distance from the sensor origin to each detection in meters (optional).
        azimuth: Azimuth angle in radians for each detection (optional).
        elevation: Elevation angle in radians for each detection (optional).
        rcs: Radar Cross Section of each detection in dBm (optional).
        snr: Signal-to-Noise Ratio of each detection in dB (optional).
        doppler_velocity: Doppler radial velocity of each detection in m/s (optional).
        vx: X component of the velocity of each detection in m/s (optional).
        vy: Y component of the velocity of each detection in m/s (optional).
        vx_comp: Ego-motion-compensated X velocity of each detection in m/s (optional).
        vy_comp: Ego-motion-compensated Y velocity of each detection in m/s (optional).
        ax: X component of the acceleration of each detection in m/s² (optional).
        ay: Y component of the acceleration of each detection in m/s² (optional).
        radial_speed: Radial speed of each detection in m/s (optional).

    ### Querying with the **`.Q` Proxy**
    This class is fully queryable via the **`.Q` proxy**. You can filter Radar data based
    on thresholds values within a [`QueryOntologyCatalog`][mosaicolabs.models.query.builders.QueryOntologyCatalog].
    Expressions entailing lists of values can be queried using any between `all()`, `any()`
    or index access `[i]` followed by the contained type supported operations.

    Example:
        ```python
        from mosaicolabs import MosaicoClient, QueryTopic
        from mosaicolabs.models.futures import Radar

        with MosaicoClient.connect("localhost", 6726) as client:
            # Fetch all sequences that contain at least one Radar topic
            qresponse = client.query(QueryTopic().with_ontology_tag(Radar.ontology_tag()))

            if qresponse is not None:

                for item in qresponse.items:
                    print(f"Sequence: {item.name}")
                    print(f"Topics:   {[topic.name for topic in item.topics]}")
        ```
    """

    x: MosaicoType.list_(MosaicoType.float32) = MosaicoField(
        description="x coordinates in meters."
    )
    """
    X coordinates of each detection, in meters.

    ### Querying with the **`.Q` Proxy**
    The X coordinates value are queryable via the `x` field. Since it represents a list of
    values, use `all()`, `any()` or index access `[i]` to narrow down to the list element and
    compose a correct expression.
        - Radar.Q.x.all()       -> invalid expression
        - Radar.Q.x.gt(1)       -> invalid expression
        - Radar.Q.x.all().gt(1) -> valid expression

    | Field Access Path | Queryable Type | Supported Operators |
    | :--- | :--- | :--- |
    | `Radar.Q.x.all()` | `Numeric` | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    | `Radar.Q.x.any()` | `Numeric` | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    | `Radar.Q.x.[i]` | `Numeric` | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |

    Example:
        ```python
        from mosaicolabs import MosaicoClient, QueryOntologyCatalog
        from mosaicolabs.models.futures import Radar

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter for Radar values on X within a specific range
            qresponse = client.query(
                QueryOntologyCatalog(Radar.Q.x.all().between([-1.0, 1.0]))
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
        description="y coordinates in meters."
    )
    """
    Y coordinates of each detection, in meters.

    ### Querying with the **`.Q` Proxy**
    The Y coordinates value are queryable via the `y` field. Since it represents a list of
    values, use `all()`, `any()` or index access `[i]` to narrow down to the list element and
    compose a correct expression.
        - Radar.Q.y.all()       -> invalid expression
        - Radar.Q.y.gt(1)       -> invalid expression
        - Radar.Q.y.all().gt(1) -> valid expression

    | Field Access Path | Queryable Type | Supported Operators |
    | :--- | :--- | :--- |
    | `Radar.Q.y.all()` | `Numeric` | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    | `Radar.Q.y.any()` | `Numeric` | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    | `Radar.Q.y.[i]` | `Numeric` | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |

    Example:
        ```python
        from mosaicolabs import MosaicoClient, QueryOntologyCatalog
        from mosaicolabs.models.futures import Radar

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter for Radar values on Y within a specific range
            qresponse = client.query(
                QueryOntologyCatalog(Radar.Q.y.all().between([-1.0, 1.0]))
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
        description="z coordinates in meters."
    )
    """
    Z coordinates of each detection, in meters.

    ### Querying with the **`.Q` Proxy**
    The Z coordinates value are queryable via the `z` field. Since it represents a list of
    values, use `all()`, `any()` or index access `[i]` to narrow down to the list element and
    compose a correct expression.
        - Radar.Q.z.all()       -> invalid expression
        - Radar.Q.z.gt(1)       -> invalid expression
        - Radar.Q.z.all().gt(1) -> valid expression

    | Field Access Path | Queryable Type | Supported Operators |
    | :--- | :--- | :--- |
    | `Radar.Q.z.all()` | `Numeric` | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    | `Radar.Q.z.any()` | `Numeric` | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    | `Radar.Q.z.[i]` | `Numeric` | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |

    Example:
        ```python
        from mosaicolabs import MosaicoClient, QueryOntologyCatalog
        from mosaicolabs.models.futures import Radar

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter for Radar values on Z within a specific range
            qresponse = client.query(
                QueryOntologyCatalog(Radar.Q.z.all().between([-1.0, 1.0]))
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
        default=None, description="radial distance in meters."
    )
    """
    Radial distance from the sensor origin to each detection, in meters.

    Represents the straight-line distance along the beam axis.

    ### Querying with the **`.Q` Proxy**
    The range value is queryable via the `range` field. Since it represents a list of values,
    use `all()`, `any()` or index access `[i]` to narrow down to the list element and compose a
    correct expression.
        - Radar.Q.range.all()       -> invalid expression
        - Radar.Q.range.gt(1)       -> invalid expression
        - Radar.Q.range.all().gt(1) -> valid expression

    | Field Access Path | Queryable Type | Supported Operators |
    | :--- | :--- | :--- |
    | `Radar.Q.range.all()` | `Numeric` | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    | `Radar.Q.range.any()` | `Numeric` | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    | `Radar.Q.range.[i]` | `Numeric` | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |

    Example:
        ```python
        from mosaicolabs import MosaicoClient, QueryOntologyCatalog
        from mosaicolabs.models.futures import Radar

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter for Radar detections within 100 meters of the sensor
            qresponse = client.query(
                QueryOntologyCatalog(Radar.Q.range.all().leq(100.0))
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
        default=None, description="azimuth angle in radians."
    )
    """
    Horizontal (azimuth) angle of each detection in radians.

    Measured in the sensor's horizontal plane, typically from 0 to 2π,
    with 0 aligned to the sensor's forward axis.

    ### Querying with the **`.Q` Proxy**
    The azimuth value is queryable via the `azimuth` field. Since it represents a list of
    values, use `all()`, `any()` or index access `[i]` to narrow down to the list element and
    compose a correct expression.
        - Radar.Q.azimuth.all()       -> invalid expression
        - Radar.Q.azimuth.gt(1)       -> invalid expression
        - Radar.Q.azimuth.all().gt(1) -> valid expression

    | Field Access Path | Queryable Type | Supported Operators |
    | :--- | :--- | :--- |
    | `Radar.Q.azimuth.all()` | `Numeric` | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    | `Radar.Q.azimuth.any()` | `Numeric` | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    | `Radar.Q.azimuth.[i]` | `Numeric` | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |

    Example:
        ```python
        from mosaicolabs import MosaicoClient, QueryOntologyCatalog
        from mosaicolabs.models.futures import Radar

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter for Radar detections within a specific azimuth range
            qresponse = client.query(
                QueryOntologyCatalog(Radar.Q.azimuth.all().between([-1.57, 1.57]))
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
        default=None, description="elevation angle in radians."
    )
    """
    Vertical (elevation) angle of each detection in radians.

    Measured from the sensor's horizontal plane; positive values point upward.

    ### Querying with the **`.Q` Proxy**
    The elevation value is queryable via the `elevation` field. Since it represents a list of
    values, use `all()`, `any()` or index access `[i]` to narrow down to the list element and
    compose a correct expression.
        - Radar.Q.elevation.all()       -> invalid expression
        - Radar.Q.elevation.gt(1)       -> invalid expression
        - Radar.Q.elevation.all().gt(1) -> valid expression

    | Field Access Path | Queryable Type | Supported Operators |
    | :--- | :--- | :--- |
    | `Radar.Q.elevation.all()` | `Numeric` | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    | `Radar.Q.elevation.any()` | `Numeric` | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    | `Radar.Q.elevation.[i]` | `Numeric` | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |

    Example:
        ```python
        from mosaicolabs import MosaicoClient, QueryOntologyCatalog
        from mosaicolabs.models.futures import Radar

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter for Radar detections within a specific elevation range
            qresponse = client.query(
                QueryOntologyCatalog(Radar.Q.elevation.all().between([-0.26, 0.26]))
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

    rcs: Optional[MosaicoType.list_(MosaicoType.float32)] = MosaicoField(
        default=None, description="radar cross section in dBm."
    )
    """
    Radar Cross Section (RCS) of each detection, in dBm.

    Quantifies the effective scattering area of the target as seen by the
    sensor. Higher values typically correspond to larger or more reflective
    objects. Useful for target classification and false-positive filtering.

    ### Querying with the **`.Q` Proxy**
    The RCS value is queryable via the `rcs` field. Since it represents a list of values, use
    `all()`, `any()` or index access `[i]` to narrow down to the list element and compose a
    correct expression.
        - Radar.Q.rcs.all()       -> invalid expression
        - Radar.Q.rcs.gt(1)       -> invalid expression
        - Radar.Q.rcs.all().gt(1) -> valid expression

    | Field Access Path | Queryable Type | Supported Operators |
    | :--- | :--- | :--- |
    | `Radar.Q.rcs.all()` | `Numeric` | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    | `Radar.Q.rcs.any()` | `Numeric` | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    | `Radar.Q.rcs.[i]` | `Numeric` | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |

    Example:
        ```python
        from mosaicolabs import MosaicoClient, QueryOntologyCatalog
        from mosaicolabs.models.futures import Radar

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter for Radar detections with a large radar cross section
            qresponse = client.query(
                QueryOntologyCatalog(Radar.Q.rcs.any().gt(10.0))
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

    snr: Optional[MosaicoType.list_(MosaicoType.float32)] = MosaicoField(
        default=None, description="signal to noise ratio in dB."
    )
    """
    Signal-to-Noise Ratio (SNR) of each detection, in dB.

    Indicates the quality of the received echo relative to background noise.
    Low-SNR detections are generally less reliable and may be filtered out
    during object-level processing.

    ### Querying with the **`.Q` Proxy**
    The SNR value is queryable via the `snr` field. Since it represents a list of values, use
    `all()`, `any()` or index access `[i]` to narrow down to the list element and compose a
    correct expression.
        - Radar.Q.snr.all()       -> invalid expression
        - Radar.Q.snr.gt(1)       -> invalid expression
        - Radar.Q.snr.all().gt(1) -> valid expression

    | Field Access Path | Queryable Type | Supported Operators |
    | :--- | :--- | :--- |
    | `Radar.Q.snr.all()` | `Numeric` | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    | `Radar.Q.snr.any()` | `Numeric` | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    | `Radar.Q.snr.[i]` | `Numeric` | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |

    Example:
        ```python
        from mosaicolabs import MosaicoClient, QueryOntologyCatalog
        from mosaicolabs.models.futures import Radar

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter for Radar detections with a low signal-to-noise ratio
            qresponse = client.query(
                QueryOntologyCatalog(Radar.Q.snr.all().lt(5.0))
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

    doppler_velocity: Optional[MosaicoType.list_(MosaicoType.float32)] = MosaicoField(
        default=None, description="doppler velocity in m/s."
    )
    """
    Doppler radial velocity of each detection, in m/s.

    Represents the component of the target's velocity along the sensor's
    line of sight, derived directly from the frequency shift of the returned
    signal. Positive values conventionally indicate motion away from the sensor.

    ### Querying with the **`.Q` Proxy**
    The doppler velocity value is queryable via the `doppler_velocity` field. Since it
    represents a list of values, use `all()`, `any()` or index access `[i]` to narrow down to
    the list element and compose a correct expression.
        - Radar.Q.doppler_velocity.all()       -> invalid expression
        - Radar.Q.doppler_velocity.gt(1)       -> invalid expression
        - Radar.Q.doppler_velocity.all().gt(1) -> valid expression

    | Field Access Path | Queryable Type | Supported Operators |
    | :--- | :--- | :--- |
    | `Radar.Q.doppler_velocity.all()` | `Numeric` | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    | `Radar.Q.doppler_velocity.any()` | `Numeric` | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    | `Radar.Q.doppler_velocity.[i]` | `Numeric` | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |

    Example:
        ```python
        from mosaicolabs import MosaicoClient, QueryOntologyCatalog
        from mosaicolabs.models.futures import Radar

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter for Radar detections moving away from the sensor
            qresponse = client.query(
                QueryOntologyCatalog(Radar.Q.doppler_velocity.any().gt(0.0))
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

    vx: Optional[MosaicoType.list_(MosaicoType.float32)] = MosaicoField(
        default=None, description="x velocity in m/s."
    )
    """
    X component of the estimated velocity of each detection, in m/s.

    Expressed in the sensor frame. This is a Cartesian decomposition of the
    target velocity, as opposed to the purely radial ``doppler_velocity``.

    ### Querying with the **`.Q` Proxy**
    The X velocity value is queryable via the `vx` field. Since it represents a list of values,
    use `all()`, `any()` or index access `[i]` to narrow down to the list element and compose a
    correct expression.
        - Radar.Q.vx.all()       -> invalid expression
        - Radar.Q.vx.gt(1)       -> invalid expression
        - Radar.Q.vx.all().gt(1) -> valid expression

    | Field Access Path | Queryable Type | Supported Operators |
    | :--- | :--- | :--- |
    | `Radar.Q.vx.all()` | `Numeric` | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    | `Radar.Q.vx.any()` | `Numeric` | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    | `Radar.Q.vx.[i]` | `Numeric` | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |

    Example:
        ```python
        from mosaicolabs import MosaicoClient, QueryOntologyCatalog
        from mosaicolabs.models.futures import Radar

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter for Radar detections moving fast along X
            qresponse = client.query(
                QueryOntologyCatalog(Radar.Q.vx.any().gt(5.0))
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

    vy: Optional[MosaicoType.list_(MosaicoType.float32)] = MosaicoField(
        default=None, description="y velocity in m/s."
    )
    """
    Y component of the estimated velocity of each detection, in m/s.

    Expressed in the sensor frame. See ``vx`` for further context.

    ### Querying with the **`.Q` Proxy**
    The Y velocity value is queryable via the `vy` field. Since it represents a list of values,
    use `all()`, `any()` or index access `[i]` to narrow down to the list element and compose a
    correct expression.
        - Radar.Q.vy.all()       -> invalid expression
        - Radar.Q.vy.gt(1)       -> invalid expression
        - Radar.Q.vy.all().gt(1) -> valid expression

    | Field Access Path | Queryable Type | Supported Operators |
    | :--- | :--- | :--- |
    | `Radar.Q.vy.all()` | `Numeric` | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    | `Radar.Q.vy.any()` | `Numeric` | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    | `Radar.Q.vy.[i]` | `Numeric` | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |

    Example:
        ```python
        from mosaicolabs import MosaicoClient, QueryOntologyCatalog
        from mosaicolabs.models.futures import Radar

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter for Radar detections moving fast along Y
            qresponse = client.query(
                QueryOntologyCatalog(Radar.Q.vy.any().gt(5.0))
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

    vx_comp: Optional[MosaicoType.list_(MosaicoType.float32)] = MosaicoField(
        default=None, description="x compensated velocity in m/s."
    )
    """
    Ego-motion-compensated X velocity of each detection, in m/s.

    Obtained by subtracting the host vehicle's own velocity from ``vx``,
    yielding the detection's absolute velocity in the world frame along the
    X axis.

    ### Querying with the **`.Q` Proxy**
    The compensated X velocity value is queryable via the `vx_comp` field. Since it represents
    a list of values, use `all()`, `any()` or index access `[i]` to narrow down to the list
    element and compose a correct expression.
        - Radar.Q.vx_comp.all()       -> invalid expression
        - Radar.Q.vx_comp.gt(1)       -> invalid expression
        - Radar.Q.vx_comp.all().gt(1) -> valid expression

    | Field Access Path | Queryable Type | Supported Operators |
    | :--- | :--- | :--- |
    | `Radar.Q.vx_comp.all()` | `Numeric` | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    | `Radar.Q.vx_comp.any()` | `Numeric` | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    | `Radar.Q.vx_comp.[i]` | `Numeric` | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |

    Example:
        ```python
        from mosaicolabs import MosaicoClient, QueryOntologyCatalog
        from mosaicolabs.models.futures import Radar

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter for Radar detections that are stationary in the world frame
            qresponse = client.query(
                QueryOntologyCatalog(Radar.Q.vx_comp.all().between([-0.5, 0.5]))
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

    vy_comp: Optional[MosaicoType.list_(MosaicoType.float32)] = MosaicoField(
        default=None, description="y compensated velocity in m/s."
    )
    """
    Ego-motion-compensated Y velocity of each detection, in m/s.

    Analogous to ``vx_comp`` along the Y axis. See ``vx_comp`` for further context.

    ### Querying with the **`.Q` Proxy**
    The compensated Y velocity value is queryable via the `vy_comp` field. Since it represents
    a list of values, use `all()`, `any()` or index access `[i]` to narrow down to the list
    element and compose a correct expression.
        - Radar.Q.vy_comp.all()       -> invalid expression
        - Radar.Q.vy_comp.gt(1)       -> invalid expression
        - Radar.Q.vy_comp.all().gt(1) -> valid expression

    | Field Access Path | Queryable Type | Supported Operators |
    | :--- | :--- | :--- |
    | `Radar.Q.vy_comp.all()` | `Numeric` | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    | `Radar.Q.vy_comp.any()` | `Numeric` | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    | `Radar.Q.vy_comp.[i]` | `Numeric` | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |

    Example:
        ```python
        from mosaicolabs import MosaicoClient, QueryOntologyCatalog
        from mosaicolabs.models.futures import Radar

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter for Radar detections that are stationary in the world frame
            qresponse = client.query(
                QueryOntologyCatalog(Radar.Q.vy_comp.all().between([-0.5, 0.5]))
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

    ax: Optional[MosaicoType.list_(MosaicoType.float32)] = MosaicoField(
        default=None, description="x acceleration in m/s^2."
    )
    """
    X component of the estimated acceleration of each detection, in m/s².

    Available only on sensors that track detections across multiple scans and
    report per-point kinematic state (e.g. high-level object-list outputs).

    ### Querying with the **`.Q` Proxy**
    The X acceleration value is queryable via the `ax` field. Since it represents a list of
    values, use `all()`, `any()` or index access `[i]` to narrow down to the list element and
    compose a correct expression.
        - Radar.Q.ax.all()       -> invalid expression
        - Radar.Q.ax.gt(1)       -> invalid expression
        - Radar.Q.ax.all().gt(1) -> valid expression

    | Field Access Path | Queryable Type | Supported Operators |
    | :--- | :--- | :--- |
    | `Radar.Q.ax.all()` | `Numeric` | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    | `Radar.Q.ax.any()` | `Numeric` | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    | `Radar.Q.ax.[i]` | `Numeric` | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |

    Example:
        ```python
        from mosaicolabs import MosaicoClient, QueryOntologyCatalog
        from mosaicolabs.models.futures import Radar

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter for Radar detections that are accelerating hard along X
            qresponse = client.query(
                QueryOntologyCatalog(Radar.Q.ax.any().gt(3.0))
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

    ay: Optional[MosaicoType.list_(MosaicoType.float32)] = MosaicoField(
        default=None, description="y acceleration in m/s^2."
    )
    """
    Y component of the estimated acceleration of each detection, in m/s².

    Analogous to ``ax`` along the Y axis. See ``ax`` for further context.

    ### Querying with the **`.Q` Proxy**
    The Y acceleration value is queryable via the `ay` field. Since it represents a list of
    values, use `all()`, `any()` or index access `[i]` to narrow down to the list element and
    compose a correct expression.
        - Radar.Q.ay.all()       -> invalid expression
        - Radar.Q.ay.gt(1)       -> invalid expression
        - Radar.Q.ay.all().gt(1) -> valid expression

    | Field Access Path | Queryable Type | Supported Operators |
    | :--- | :--- | :--- |
    | `Radar.Q.ay.all()` | `Numeric` | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    | `Radar.Q.ay.any()` | `Numeric` | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    | `Radar.Q.ay.[i]` | `Numeric` | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |

    Example:
        ```python
        from mosaicolabs import MosaicoClient, QueryOntologyCatalog
        from mosaicolabs.models.futures import Radar

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter for Radar detections that are accelerating hard along Y
            qresponse = client.query(
                QueryOntologyCatalog(Radar.Q.ay.any().gt(3.0))
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

    radial_speed: Optional[MosaicoType.list_(MosaicoType.float32)] = MosaicoField(
        default=None, description="radial speed in m/s."
    )
    """
    Radial speed of each detection, in m/s.

    Represents the magnitude of the velocity component along the line of sight,
    without sign convention. Distinct from ``doppler_velocity``, which may carry
    a directional sign depending on the sensor's convention.

    ### Querying with the **`.Q` Proxy**
    The radial speed value is queryable via the `radial_speed` field. Since it represents a
    list of values, use `all()`, `any()` or index access `[i]` to narrow down to the list
    element and compose a correct expression.
        - Radar.Q.radial_speed.all()       -> invalid expression
        - Radar.Q.radial_speed.gt(1)       -> invalid expression
        - Radar.Q.radial_speed.all().gt(1) -> valid expression

    | Field Access Path | Queryable Type | Supported Operators |
    | :--- | :--- | :--- |
    | `Radar.Q.radial_speed.all()` | `Numeric` | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    | `Radar.Q.radial_speed.any()` | `Numeric` | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    | `Radar.Q.radial_speed.[i]` | `Numeric` | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |

    Example:
        ```python
        from mosaicolabs import MosaicoClient, QueryOntologyCatalog
        from mosaicolabs.models.futures import Radar

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter for Radar detections with a high radial speed
            qresponse = client.query(
                QueryOntologyCatalog(Radar.Q.radial_speed.any().gt(10.0))
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
