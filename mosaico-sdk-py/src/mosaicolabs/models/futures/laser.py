"""
Laser Scan Ontology Models.

This module defines ontology models for 2D laser range finders.
Two concrete variants are provided, each targeting a specific
return mode:
- `LaserScan`: Single return scanners that record one range measurement
  per beam (e.g. SICK LMS, Hokuyo URG/UST, Velodyne VLP in 2D mode).
- `MultiEchoLaserScan`: Multi-echo scanners that record several
  range returns per beam, capturing overlapping or semi-transparent surfaces
  (e.g. SICK LMS 5xx multi-echo, Hokuyo UXM).

"""

from typing import Generic, List, Optional, TypeVar

from mosaicolabs.models import BaseModel, Serializable

from .internal.laser_helper import _build_struct

SingleRange = List[float]
"""Type alias for a single-return range array: one distance value per beam."""

MultiRange = List[List[float]]
"""Type alias for a multi-echo range array: a list of distance values per beam."""

T = TypeVar("T", bound=List)


class _LaserScanBase(BaseModel, Generic[T]):
    """
    Internal generic base model shared by laser scan ontologies.

    Encodes the scan geometry, timing metadata, and range and intensity arrays that are common to both single-return and multi-echo laser scanners.
    The type parameter ``T`` determines the shape of the ``ranges`` and ``intensities`` arrays:

    * ``T = SingleRange`` (``List[float]``) for single-return scanners one distance value per beam.
    * ``T = MultiRange`` (``List[List[float]]``) for multi-echo scanners a list of distance values per beam.

    **This class is not intended to be instantiated directly**. Use one of the concrete subclasses: [`LaserScan`][mosaicolabs.models.futures.LaserScan] or [`MultiEchoLaserScan`][mosaicolabs.models.futures.MultiEchoLaserScan].

    Attributes:
        angle_min: Start angle of the scan in radians.
        angle_max: End angle of the scan in radians.
        angle_increment: Angular step between consecutive beams in radians.
        time_increment: Time elapsed between consecutive beam measurements
            in seconds.
        scan_time: Total duration of one full scan in seconds.
        range_min: Minimum valid range value in meters; measurements below
            this threshold should be discarded.
        range_max: Maximum valid range value in meters; measurements above
            this threshold should be discarded.
        ranges: Range measurements for each beam. Shape depends on ``T``.
        intensities: Intensity measurements for each beam, co-indexed with
            ``ranges`` (optional). Shape depends on ``T``.

    ### Querying with the **`.Q` Proxy**
    Scalar fields on this model are fully queryable via the **`.Q` proxy**.
    List-typed fields (``ranges``, ``intensities``) are **not queryable**.

    | Field Access Path | Queryable Type | Supported Operators |
    | :--- | :--- | :--- |
    | `LaserScan.Q.angle_min` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    | `LaserScan.Q.angle_max` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    | `LaserScan.Q.angle_increment` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    | `LaserScan.Q.time_increment` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    | `LaserScan.Q.scan_time` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    | `LaserScan.Q.range_min` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    | `LaserScan.Q.range_max` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |

    Example:
        ```python
            from mosaicolabs import MosaicoClient, QueryOntologyCatalog
            from mosaicolabs.model.futures import LaserScan

            with MosaicoClient.connect("localhost", 6726) as client:
                # Find scans with a wide field of view and a long maximum range
                qresponse = client.query(
                    QueryOntologyCatalog(LaserScan.Q.range_max.gt(30.0))
                    .with_expression(LaserScan.Q.angle_max.geq(3.14)),
                )

                # Inspect the response
                if qresponse is not None:
                    for item in qresponse:
                        print(f"Sequence: {item.sequence.name}")
                        print(f"Topics: {[topic.name for topic in item.topics]}")

                # Same query, also extracting the first and last occurrence times
                qresponse = client.query(
                    QueryOntologyCatalog(LaserScan.Q.range_max.gt(30.0), include_timestamp_range=True)
                    .with_expression(LaserScan.Q.angle_max.geq(3.14)),
                )

                if qresponse is not None:
                    for item in qresponse:
                        print(f"Sequence: {item.sequence.name}")
                        print(f"Topics: {{topic.name: "
                              f"[topic.timestamp_range.start, topic.timestamp_range.end] "
                              f"for topic in item.topics}}")
        ```
    """

    @classmethod
    def __pydantic_init_subclass__(cls, **kwargs):
        super().__pydantic_init_subclass__(**kwargs)

        ranges_field = cls.model_fields.get("ranges")

        if (
            ranges_field
            and ranges_field.annotation
            and ranges_field.annotation is not T
        ):
            cls.__msco_pyarrow_struct__ = _build_struct(ranges_field.annotation)

    angle_min: float
    """
    Start angle of the scan in radians.

    Defines the angular position of the first beam in the sweep.Together with ``angle_max`` and ``angle_increment``, it fully characterises the angular coverage of the scan.


    ### Querying with the **`.Q` Proxy**

    | Field Access Path | Queryable Type | Supported Operators |
    | :--- | :--- | :--- |
    | `LaserScan.Q.angle_min` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |

    Example:
        ```python
            from mosaicolabs import MosaicoClient, QueryOntologyCatalog
            from mosaicolabs.model.futures import LaserScan

            with MosaicoClient.connect("localhost", 6726) as client:
                qresponse = client.query(
                    QueryOntologyCatalog(LaserScan.Q.angle_min.geq(-3.14))
                )

                if qresponse is not None:
                    for item in qresponse:
                        print(f"Sequence: {item.sequence.name}")
                        print(f"Topics: {[topic.name for topic in item.topics]}")
        ```
    """

    angle_max: float
    """
    End angle of the scan in radians.

    Defines the angular position of the last beam in the sweep.
    The total field of view of the scanner is ``angle_max - angle_min``.

    ### Querying with the **`.Q` Proxy**

    | Field Access Path | Queryable Type | Supported Operators |
    | :--- | :--- | :--- |
    | `LaserScan.Q.angle_max` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |

    Example:
        ```python
            from mosaicolabs import MosaicoClient, QueryOntologyCatalog
            from mosaicolabs.model.futures import LaserScan

            with MosaicoClient.connect("localhost", 6726) as client:
                qresponse = client.query(
                    QueryOntologyCatalog(LaserScan.Q.angle_max.geq(3.14))
                )

                if qresponse is not None:
                    for item in qresponse:
                        print(f"Sequence: {item.sequence.name}")
                        print(f"Topics: {[topic.name for topic in item.topics]}")
        ```

    """

    angle_increment: float
    """
    Angular step between consecutive beams in radians.

    The number of beams in a sweep can be derived as ``round((angle_max - angle_min) / angle_increment) + 1``.
    A negative value indicates a clockwise scan direction.

    ### Querying with the **`.Q` Proxy**

    | Field Access Path | Queryable Type | Supported Operators |
    | :--- | :--- | :--- |
    | `LaserScan.Q.angle_increment` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |

    Example:
        ```python
            from mosaicolabs import MosaicoClient, QueryOntologyCatalog
            from mosaicolabs.model.futures import LaserScan

            with MosaicoClient.connect("localhost", 6726) as client:
                # Find high-resolution scans (small angular step)
                qresponse = client.query(
                    QueryOntologyCatalog(LaserScan.Q.angle_increment.lt(0.01))
                )

                if qresponse is not None:
                    for item in qresponse:
                        print(f"Sequence: {item.sequence.name}")
                        print(f"Topics: {[topic.name for topic in item.topics]}")
        ```
    """

    time_increment: float
    """
    Time elapsed between consecutive beam measurements, in seconds.
    
    If the scanner is moving, this will be used in interpoling position of 3D points.

    ### Querying with the **`.Q` Proxy**

    | Field Access Path | Queryable Type | Supported Operators |
    | :--- | :--- | :--- |
    | `LaserScan.Q.time_increment` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |

    Example:
        ```python
            from mosaicolabs import MosaicoClient, QueryOntologyCatalog
            from mosaicolabs.model.futures import LaserScan

            with MosaicoClient.connect("localhost", 6726) as client:
                qresponse = client.query(
                    QueryOntologyCatalog(LaserScan.Q.time_increment.lt(0.0001))
                )

                if qresponse is not None:
                    for item in qresponse:
                        print(f"Sequence: {item.sequence.name}")
                        print(f"Topics: {[topic.name for topic in item.topics]}")
        ```
    """

    scan_time: float
    """
    Time between scans in seconds.

    ### Querying with the **`.Q` Proxy**

    | Field Access Path | Queryable Type | Supported Operators |
    | :--- | :--- | :--- |
    | `LaserScan.Q.scan_time` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |

    Example:
        ```python
            from mosaicolabs import MosaicoClient, QueryOntologyCatalog
            from mosaicolabs.model.futures import LaserScan

            with MosaicoClient.connect("localhost", 6726) as client:
                # Find sequences recorded at 10 Hz (scan_time ≈ 0.1 s)
                qresponse = client.query(
                    QueryOntologyCatalog(LaserScan.Q.scan_time.between([0.09, 0.11]))
                )

                if qresponse is not None:
                    for item in qresponse:
                        print(f"Sequence: {item.sequence.name}")
                        print(f"Topics: {[topic.name for topic in item.topics]}")
        ```
    """

    range_min: float
    """
    Minimum valid range value, in meters.

    Measurements strictly below this threshold are outside the sensor's
    reliable operating range and should be discarded or treated as invalid
    during downstream processing.

    ### Querying with the **`.Q` Proxy**

    | Field Access Path | Queryable Type | Supported Operators |
    | :--- | :--- | :--- |
    | `LaserScan.Q.range_min` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |

    Example:
        ```python
            from mosaicolabs import MosaicoClient, QueryOntologyCatalog
            from mosaicolabs.model.futures import LaserScan

            with MosaicoClient.connect("localhost", 6726) as client:
                qresponse = client.query(
                    QueryOntologyCatalog(LaserScan.Q.range_min.leq(0.1))
                )

                if qresponse is not None:
                    for item in qresponse:
                        print(f"Sequence: {item.sequence.name}")
                        print(f"Topics: {[topic.name for topic in item.topics]}")
        ```
    """

    range_max: float
    """
    Maximum valid range value, in meters.

    Measurements strictly above this threshold exceed the sensor's maximum
    detection distance and should be discarded or treated as invalid
    during downstream processing.

    ### Querying with the **`.Q` Proxy**

    | Field Access Path | Queryable Type | Supported Operators |
    | :--- | :--- | :--- |
    | `LaserScan.Q.range_max` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |

    Example:
        ```python
            from mosaicolabs import MosaicoClient, QueryOntologyCatalog
            from mosaicolabs.model.futures import LaserScan

            with MosaicoClient.connect("localhost", 6726) as client:
                # Find long-range scanner sessions
                qresponse = client.query(
                    QueryOntologyCatalog(LaserScan.Q.range_max.gt(30.0))
                )

                if qresponse is not None:
                    for item in qresponse:
                        print(f"Sequence: {item.sequence.name}")
                        print(f"Topics: {[topic.name for topic in item.topics]}")
        ```
    """

    ranges: T
    """
    Range measurements for each beam.

    * For `LaserScan` (``T = SingleRange``): a flat list of ``float`` values, one per beam, representing the measured distance in meters.
    * For `MultiEchoLaserScan` (``T = MultiRange``): a list of lists, where the *i*-th inner list contains all echo distances returned by the *i*-th beam, ordered from nearest to farthest. An empty inner list indicates no valid return for that beam.

    Values outside the ``[range_min, range_max]`` interval should be
    considered invalid.
    """

    intensities: Optional[T] = None
    """
    Intensity measurements for each beam, co-indexed with ``ranges`` (optional).

    Carries the signal amplitude of each returned echo. The structure mirrors
    that of ``ranges``: 

    * a flat list for single-return scans 
    * a list of lists for multi-echo scans 

    Scaling and units are manufacturer-dependent.
    
    Not all scanner models populate this field.
    """


class LaserScan(_LaserScanBase[SingleRange], Serializable):
    """
    Single-return 2D laser scan data.

    This model represents one sweep of a single-return laser range finder.
    Each beam yields exactly one range measurement, corresponding to the strongest or first detected echo.

    ``ranges`` and ``intensities`` are flat ``List[float]`` whose *i*-th element corresponds to the beam at angular position ``angle_min + i * angle_increment``.

    Attributes:
        angle_min: Start angle of the scan in radians.
        angle_max: End angle of the scan in radians.
        angle_increment: Angular step between consecutive beams in radians.
        time_increment: Time between consecutive beam measurements in seconds.
        scan_time: Total duration of one full scan in seconds.
        range_min: Minimum valid range threshold in meters.
        range_max: Maximum valid range threshold in meters.
        ranges: Measured distance per beam in meters.
        intensities: Signal amplitude per beam (optional).

    Note:
        List-typed fields are **not queryable** via the `.Q` proxy. The `.Q`
        proxy is not available on this model.

    ### Querying with the **`.Q` Proxy**
       Scalar fields are fully queryable via the **`.Q` proxy**.
       ``ranges`` and ``intensities`` are **not queryable**.

    | Field Access Path | Queryable Type | Supported Operators |
    | :--- | :--- | :--- |
    | `LaserScan.Q.angle_min` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    | `LaserScan.Q.angle_max` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    | `LaserScan.Q.angle_increment` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    | `LaserScan.Q.time_increment` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    | `LaserScan.Q.scan_time` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    | `LaserScan.Q.range_min` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    | `LaserScan.Q.range_max` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |

    Example:
        ```python
        from mosaicolabs import MosaicoClient, QueryOntologyCatalog
        from mosaicolabs.model.futures import LaserScan

        with MosaicoClient.connect("localhost", 6726) as client:
            # Find long-range, wide-FOV scans
            qresponse = client.query(
                QueryOntologyCatalog(LaserScan.Q.range_max.gt(30.0))
                    .with_expression(LaserScan.Q.angle_max.geq(3.14)),
            )

            if qresponse is not None:
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {[topic.name for topic in item.topics]}")

            # Same query, also extracting the first and last occurrence times
            qresponse = client.query(
                QueryOntologyCatalog(LaserScan.Q.range_max.gt(30.0), include_timestamp_range=True)
                    .with_expression(LaserScan.Q.angle_max.geq(3.14)),
            )

            if qresponse is not None:
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {{topic.name: "
                          f"[topic.timestamp_range.start, topic.timestamp_range.end] "
                          f"for topic in item.topics}}")
        ```
    """

    ranges: SingleRange
    """
    Range measurements for each beam.
    
    A flat list of ``float`` values, one per beam, representing the measured distance in meters.

    Values outside the ``[range_min, range_max]`` interval should be considered invalid.
    """

    intensities: Optional[SingleRange] = None
    """
    Intensity measurements for each beam (optional).
    
    A flat list of ``float`` values, carries the signal amplitude of each beam. 
    """


class MultiEchoLaserScan(_LaserScanBase[MultiRange], Serializable):
    """
    Multi-echo 2D laser scan data.

    This model represents one sweep of a multi-echo laser range finder.
    Multi-echo scanners record several range returns per beam, allowing the sensor to
    detect overlapping surfaces, semi-transparent objects such as vegetation
    or rain drops, and retroreflective targets simultaneously.

    ``ranges`` and ``intensities`` are ``List[List[float]]`` arrays
    where the *i*-th inner list contains all echo returns for the beam at
    angular position ``angle_min + i * angle_increment``, ordered from nearest
    to farthest. An empty inner list indicates no valid return for that beam.

    Attributes:
        angle_min: Start angle of the scan in radians.
        angle_max: End angle of the scan in radians.
        angle_increment: Angular step between consecutive beams in radians.
        time_increment: Time between consecutive beam measurements in seconds.
        scan_time: Total duration of one full scan in seconds.
        range_min: Minimum valid range threshold in meters.
        range_max: Maximum valid range threshold in meters.
        ranges: List of echo distances per beam in meters; may contain
            multiple returns per beam.
        intensities: List of echo amplitudes per beam, co-indexed with
            ``ranges`` (optional).

    Note:
        List-typed fields are **not queryable** via the `.Q` proxy. The `.Q`
        proxy is not available on this model.

    ### Querying with the **`.Q` Proxy**
    Scalar fields are fully queryable via the **`.Q` proxy**.
    ``ranges`` and ``intensities`` are **not queryable**.

    | Field Access Path | Queryable Type | Supported Operators |
    | :--- | :--- | :--- |
    | `MultiEchoLaserScan.Q.angle_min` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    | `MultiEchoLaserScan.Q.angle_max` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    | `MultiEchoLaserScan.Q.angle_increment` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    | `MultiEchoLaserScan.Q.time_increment` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    | `MultiEchoLaserScan.Q.scan_time` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    | `MultiEchoLaserScan.Q.range_min` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    | `MultiEchoLaserScan.Q.range_max` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |

    Example:
        ```python
            from mosaicolabs import MosaicoClient, QueryOntologyCatalog
            from mosaicolabs.model.futures import MultiEchoLaserScan

            with MosaicoClient.connect("localhost", 6726) as client:
                # Find long-range, wide-FOV multi-echo scans
                qresponse = client.query(
                    QueryOntologyCatalog(MultiEchoLaserScan.Q.range_max.gt(30.0))
                        .with_expression(MultiEchoLaserScan.Q.angle_max.geq(3.14)),
                )

                if qresponse is not None:
                    for item in qresponse:
                        print(f"Sequence: {item.sequence.name}")
                        print(f"Topics: {[topic.name for topic in item.topics]}")

                # Same query, also extracting the first and last occurrence times
                qresponse = client.query(
                    QueryOntologyCatalog(MultiEchoLaserScan.Q.range_max.gt(30.0), include_timestamp_range=True)
                        .with_expression(MultiEchoLaserScan.Q.angle_max.geq(3.14)),
                )

                if qresponse is not None:
                    for item in qresponse:
                        print(f"Sequence: {item.sequence.name}")
                        print(f"Topics: {{topic.name: "
                            f"[topic.timestamp_range.start, topic.timestamp_range.end] "
                            f"for topic in item.topics}}")
        ```
    """

    ranges: MultiRange
    """
    Range measurements for each beam.

    A list of lists, where the *i*-th inner list contains all echo distances returned by the
    *i*-th beam, ordered from nearest to farthest. An empty inner list indicates no valid return for that beam.
    
    Values outside the ``[range_min, range_max]`` interval should be considered invalid.
    """

    intensities: Optional[MultiRange] = None
    """
    Intensity measurements for each beam. (optional).

    A flat list of list of ``float`` value carries the signal amplitude of each returned echo.
    """
