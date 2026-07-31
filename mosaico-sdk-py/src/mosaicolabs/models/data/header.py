"""
Header Definitions.

This module defines the standard `Header` class used to provide metadata to ontology data.
"""

from typing import Optional

from ..core import MosaicoField, MosaicoType, Serializable
from .time import Time


class Header(Serializable):
    """
    A heading, typically associated with a sensor measurement

    It is composed of Optional fields depending on the type contained information in the
    sensor measurement.

    Attributes:
        timestamp: Time (seconds and nanoseconds) passed since the epoch (Unix time) or process start (clock time). It can be omitted if not available.
        frame_id: Reference frame name used for the measurement. It can be omitted if not available.
        sample_counter: Integer indicating the number of samples elapsed since process start. It can be omitted if not available.

    ### Querying with the **`.Q` Proxy**
    This class fields are queryable when constructing a [`QueryOntologyCatalog`][mosaicolabs.query.builders.QueryOntologyCatalog]
    via the **`.Q` proxy**. Check the fields documentation for detailed description.

    Example:
    ```python
    from mosaicolabs import MosaicoClient, Header, QueryOntologyCatalog

    with MosaicoClient.connect("localhost", 6726) as client:
        # Filter Header with time seconds-component AND time nanoseconds-component
        qresponse = client.query(
            QueryOntologyCatalog(Header.Q.timestamp.seconds.lt(20.0))
                .with_expression(Header.Q.timestamp.nanoseconds.gt(100000))
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

    timestamp: Optional[Time] = MosaicoField(
        nullable=True,
        default=None,
        description="Timestamp representing when the data has been measured",
    )
    """
    Time (seconds and nanoseconds) passed since the epoch (Unix time) or process start (clock time).

    ### Querying with the **`.Q` Proxy**
    Timestamp components are queryable through the `timestamp` field prefix.

    | Field Access Path | Queryable Type | Supported Operators |
    | :--- | :--- | :--- |
    | `Header.Q.timestamp.seconds` | `Numeric` | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()`, `.outside()` |
    | `Header.Q.timestamp.nanoseconds` | `Numeric` | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()`, `.outside()` |

    Example:
        ```python
        from mosaicolabs import MosaicoClient, Header, QueryOntologyCatalog
        
        with MosaicoClient.connect("localhost", 6726) as client:
            # Find headers where the timestamp exceeds 5 seconds
            qresponse = client.query(
                QueryOntologyCatalog(Header.Q.timestamp.seconds.gt(5.0))
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

    frame_id: Optional[MosaicoType.string] = MosaicoField(
        nullable=True,
        default=None,
        description="String representing the acquired data reference system name",
    )
    """
    String representing the acquired data reference system name. It may be None if it is unknown or the
    measurement does not support one (like an audio stream).

    ### Querying with the **`.Q` Proxy**
    Frame id component is queryable through the `frame_id` field prefix.

    | Field Access Path | Queryable Type | Supported Operators |
    | :--- | :--- | :--- |
    | `Header.Q.frame_id` | `String` | `.eq()`, `.match()`, `.in_()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.between()`, `.outside()` |

    Example:
        ```python
        from mosaicolabs import MosaicoClient, Header, QueryOntologyCatalog
        
        with MosaicoClient.connect("localhost", 6726) as client:
            # Find headers where reference system is base_link
            qresponse = client.query(
                QueryOntologyCatalog(Header.Q.frame_id.eq("base_link"))
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

    sample_counter: Optional[MosaicoType.uint64] = MosaicoField(
        nullable=True,
        default=None,
        description="An optional counter used to track how many samples have been processed. It needs to be monotonically increasing",
    )
    """
    Counter used to track how many samples have been processed by the sensor. 
    It should always be monotonically increasing.

    ### Querying with the **`.Q` Proxy**
    Sample counters component is queryable through the `sample_counter` field prefix.

    | Field Access Path | Queryable Type | Supported Operators |
    | :--- | :--- | :--- |
    | `Header.Q.sample_counter` | `Numeric` | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()`, `.outside()` |

    Example:
        ```python
        from mosaicolabs import MosaicoClient, Header, QueryOntologyCatalog
        
        with MosaicoClient.connect("localhost", 6726) as client:
            # Find headers where the sample counters exceeds 300-th sample
            qresponse = client.query(
                QueryOntologyCatalog(Header.Q.sample_counter.gt(300.0))
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
