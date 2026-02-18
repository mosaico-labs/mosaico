"""
This module provides specialized wrapper classes for standard Python primitive types, including Integers, Floating-point numbers, Booleans, and Strings.

In the Mosaico ecosystem, raw primitives cannot be transmitted directly because the platform requires structured metadata and explicit serialization schemas.
These wrappers elevate basic data types to "first-class citizens" of the messaging system by inheriting from [`Serializable`][mosaicolabs.models.serializable.Serializable]
and [`HeaderMixin`][mosaicolabs.models.mixins.HeaderMixin].

**Key Features:**
* **Standardized Metadata**: Every wrapped value includes a standard [`Header`][mosaicolabs.models.header.Header], enabling full traceability and temporal context (timestamps) for even the simplest data points.
* **Explicit Serialization**: Each class defines a precise `pyarrow.StructType` schema, ensuring consistent bit-width (e.g., 8-bit vs 64-bit) across the entire data platform.
* **Registry Integration**: Wrapped types are automatically registered in the Mosaico ontology, allowing them to be used in platform-side [Queries][mosaicolabs.comm.MosaicoClient.query].
"""

from typing import Any
import pyarrow as pa

from ..serializable import Serializable
from ..mixins import HeaderMixin


class Integer8(Serializable, HeaderMixin):
    """
    A wrapper for a signed 8-bit integer.

    Attributes:
        data: The underlying 8-bit integer value.
        header: An optional metadata header injected by `HeaderMixin`.

    ### Querying with the **`.Q` Proxy**
    The fields of this class are queryable when constructing a [`QueryOntologyCatalog`][mosaicolabs.models.query.builders.QueryOntologyCatalog]
    via the **`.Q` proxy**. Check the fields documentation for detailed description.

    Example:
        ```python
        from mosaicolabs import MosaicoClient, Integer8, QueryOntologyCatalog

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter for a specific data value
            qresponse = client.query(QueryOntologyCatalog(Integer8.Q.data.gt(123)))

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {[topic.name for topic in item.topics]}")

            # Filter for a specific data value and extract the first and last occurrence times
            qresponse = client.query(
                QueryOntologyCatalog(Integer8.Q.data.gt(123), include_timestamp_range=True)
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
                "data",
                pa.int8(),
                nullable=False,
                metadata={"description": "8-bit Integer data"},
            ),
        ]
    )
    data: int
    """
    The underlying integer value.
    
    ### Querying with the **`.Q` Proxy**
    This field is queryable when constructing a [`QueryOntologyCatalog`][mosaicolabs.models.query.builders.QueryOntologyCatalog] 
    via the **`.Q` proxy**.

    | Field Access Path | Queryable Type | Supported Operators |
    | :--- | :--- | :--- |
    | `Integer8.Q.data` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |

    Example:
        ```python
        from mosaicolabs import MosaicoClient, Integer8, QueryOntologyCatalog

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter for a specific data value
            qresponse = client.query(QueryOntologyCatalog(Integer8.Q.data.gt(-10)))

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {[topic.name for topic in item.topics]}")

            # Filter for a specific data value and extract the first and last occurrence times
            qresponse = client.query(
                QueryOntologyCatalog(Integer8.Q.data.gt(123), include_timestamp_range=True)
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


class Integer16(Serializable, HeaderMixin):
    """
    A wrapper for a signed 16-bit integer.

    Attributes:
        data: The underlying 16-bit integer value.
        header: An optional metadata header injected by `HeaderMixin`.

    ### Querying with the **`.Q` Proxy**
    The fields of this class are queryable when constructing a [`QueryOntologyCatalog`][mosaicolabs.models.query.builders.QueryOntologyCatalog]
    via the **`.Q` proxy**. Check the fields documentation for detailed description.

    Example:
        ```python
        from mosaicolabs import MosaicoClient, Integer16, QueryOntologyCatalog

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter for a specific data value
            qresponse = client.query(QueryOntologyCatalog(Integer16.Q.data.gt(123)))

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {[topic.name for topic in item.topics]}")

            # Filter for a specific data value and extract the first and last occurrence times
            qresponse = client.query(
                QueryOntologyCatalog(Integer16.Q.data.gt(123), include_timestamp_range=True)
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
                "data",
                pa.int16(),
                nullable=False,
                metadata={"description": "16-bit Integer data"},
            ),
        ]
    )
    data: int
    """
    The underlying integer value.
    
    ### Querying with the **`.Q` Proxy**
    This field is queryable when constructing a [`QueryOntologyCatalog`][mosaicolabs.models.query.builders.QueryOntologyCatalog] 
    via the **`.Q` proxy**.

    | Field Access Path | Queryable Type | Supported Operators |
    | :--- | :--- | :--- |
    | `Integer16.Q.data` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()`|

    Example:
        ```python
        from mosaicolabs import MosaicoClient, Integer16, QueryOntologyCatalog

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter for a specific data value
            qresponse = client.query(QueryOntologyCatalog(Integer16.Q.data.gt(-10)))

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {[topic.name for topic in item.topics]}")

            # Filter for a specific data value and extract the first and last occurrence times
            qresponse = client.query(
                QueryOntologyCatalog(Integer16.Q.data.gt(123), include_timestamp_range=True)
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


class Integer32(Serializable, HeaderMixin):
    """
    A wrapper for a signed 32-bit integer.

    Attributes:
        data: The underlying 32-bit integer value.
        header: An optional metadata header injected by `HeaderMixin`.

    ### Querying with the **`.Q` Proxy**
    The fields of this class are queryable when constructing a [`QueryOntologyCatalog`][mosaicolabs.models.query.builders.QueryOntologyCatalog]
    via the **`.Q` proxy**. Check the fields documentation for detailed description.

    Example:
        ```python
        from mosaicolabs import MosaicoClient, Integer32, QueryOntologyCatalog

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter for a specific data value
            qresponse = client.query(QueryOntologyCatalog(Integer32.Q.data.gt(123)))

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {[topic.name for topic in item.topics]}")

            # Filter for a specific data value and extract the first and last occurrence times
            qresponse = client.query(
                QueryOntologyCatalog(Integer32.Q.data.gt(123), include_timestamp_range=True)
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
                "data",
                pa.int32(),
                nullable=False,
                metadata={"description": "32-bit Integer data"},
            ),
        ]
    )
    data: int
    """
    The underlying integer value.
    
    ### Querying with the **`.Q` Proxy**
    This field is queryable when constructing a [`QueryOntologyCatalog`][mosaicolabs.models.query.builders.QueryOntologyCatalog] 
    via the **`.Q` proxy**.

    | Field Access Path | Queryable Type | Supported Operators |
    | :--- | :--- | :--- |
    | `Integer32.Q.data` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |

    Example:
        ```python
        from mosaicolabs import MosaicoClient, Integer32, QueryOntologyCatalog

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter for a specific data value
            qresponse = client.query(QueryOntologyCatalog(Integer32.Q.data.gt(-10)))

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {[topic.name for topic in item.topics]}")

            # Filter for a specific data value and extract the first and last occurrence times
            qresponse = client.query(
                QueryOntologyCatalog(Integer32.Q.data.gt(123), include_timestamp_range=True)
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


class Integer64(Serializable, HeaderMixin):
    """
    A wrapper for a signed 64-bit integer.

    Attributes:
        data: The underlying 64-bit integer value.
        header: An optional metadata header injected by `HeaderMixin`.ù

    ### Querying with the **`.Q` Proxy**
    The fields of this class are queryable when constructing a [`QueryOntologyCatalog`][mosaicolabs.models.query.builders.QueryOntologyCatalog]
    via the **`.Q` proxy**. Check the fields documentation for detailed description.

    Example:
        ```python
        from mosaicolabs import MosaicoClient, Integer64, QueryOntologyCatalog

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter for a specific data value
            qresponse = client.query(QueryOntologyCatalog(Integer64.Q.data.gt(123)))

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {[topic.name for topic in item.topics]}")

            # Filter for a specific data value and extract the first and last occurrence times
            qresponse = client.query(
                QueryOntologyCatalog(Integer64.Q.data.gt(123), include_timestamp_range=True)
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
                "data",
                pa.int64(),
                nullable=False,
                metadata={"description": "64-bit Integer data"},
            ),
        ]
    )
    data: int
    """
    The underlying integer value.
    
    ### Querying with the **`.Q` Proxy**
    This field is queryable when constructing a [`QueryOntologyCatalog`][mosaicolabs.models.query.builders.QueryOntologyCatalog] 
    via the **`.Q` proxy**.

    | Field Access Path | Queryable Type | Supported Operators |
    | :--- | :--- | :--- |
    | `Integer64.Q.data` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |

    Example:
        ```python
        from mosaicolabs import MosaicoClient, Integer64, QueryOntologyCatalog

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter for a specific data value
            qresponse = client.query(QueryOntologyCatalog(Integer64.Q.data.gt(123)))

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {[topic.name for topic in item.topics]}")

            # Filter for a specific data value and extract the first and last occurrence times
            qresponse = client.query(
                QueryOntologyCatalog(Integer64.Q.data.gt(123), include_timestamp_range=True)
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


class Unsigned8(Serializable, HeaderMixin):
    """
    A wrapper for an unsigned 8-bit integer.

    Attributes:
        data: The underlying unsigned 8-bit integer value.
        header: An optional metadata header injected by `HeaderMixin`.

    Raises:
        ValueError: If `data` is initialized with a negative value.

    ### Querying with the **`.Q` Proxy**
    The fields of this class are queryable when constructing a [`QueryOntologyCatalog`][mosaicolabs.models.query.builders.QueryOntologyCatalog]
    via the **`.Q` proxy**. Check the fields documentation for detailed description.

    Example:
        ```python
        from mosaicolabs import MosaicoClient, Unsigned8, QueryOntologyCatalog

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter for a specific data value
            qresponse = client.query(QueryOntologyCatalog(Unsigned8.Q.data.gt(123)))

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {[topic.name for topic in item.topics]}")

            # Filter for a specific data value and extract the first and last occurrence times
            qresponse = client.query(
                QueryOntologyCatalog(Unsigned8.Q.data.gt(123), include_timestamp_range=True)
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
                "data",
                pa.uint8(),
                nullable=False,
                metadata={"description": "8-bit Unsigned data"},
            ),
        ]
    )
    data: int
    """
    The underlying unsigned integer value.
    
    ### Querying with the **`.Q` Proxy**
    This field is queryable when constructing a [`QueryOntologyCatalog`][mosaicolabs.models.query.builders.QueryOntologyCatalog] 
    via the **`.Q` proxy**.

    | Field Access Path | Queryable Type | Supported Operators |
    | :--- | :--- | :--- |
    | `Unsigned8.Q.data` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |

    Example:
        ```python
        from mosaicolabs import MosaicoClient, Unsigned8, QueryOntologyCatalog

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter for a specific data value
            qresponse = client.query(QueryOntologyCatalog(Unsigned8.Q.data.leq(253)))

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {[topic.name for topic in item.topics]}")

            # Filter for a specific data value and extract the first and last occurrence times
            qresponse = client.query(
                QueryOntologyCatalog(Unsigned8.Q.data.gt(123), include_timestamp_range=True)
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

    def model_post_init(self, context: Any) -> None:
        """
        Validates that the input data is non-negative.

        Raises:
            ValueError: If data < 0.
        """
        super().model_post_init(context)
        if self.data < 0:
            raise ValueError("Integer must be unsigned")


class Unsigned16(Serializable, HeaderMixin):
    """
    A wrapper for an unsigned 16-bit integer.

    Attributes:
        data: The underlying unsigned 16-bit integer value.
        header: An optional metadata header injected by `HeaderMixin`.

    Raises:
        ValueError: If `data` is initialized with a negative value.

    ### Querying with the **`.Q` Proxy**
    The fields of this class are queryable when constructing a [`QueryOntologyCatalog`][mosaicolabs.models.query.builders.QueryOntologyCatalog]
    via the **`.Q` proxy**. Check the fields documentation for detailed description.

    Example:
        ```python
        from mosaicolabs import MosaicoClient, Unsigned16, QueryOntologyCatalog

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter for a specific data value
            qresponse = client.query(QueryOntologyCatalog(Unsigned16.Q.data.gt(123)))

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {[topic.name for topic in item.topics]}")

            # Filter for a specific data value and extract the first and last occurrence times
            qresponse = client.query(
                QueryOntologyCatalog(Unsigned16.Q.data.gt(123), include_timestamp_range=True)
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
                "data",
                pa.uint16(),
                nullable=False,
                metadata={"description": "16-bit Unsigned data"},
            ),
        ]
    )
    data: int
    """
    The underlying unsigned integer value.
    
    ### Querying with the **`.Q` Proxy**
    This field is queryable when constructing a [`QueryOntologyCatalog`][mosaicolabs.models.query.builders.QueryOntologyCatalog] 
    via the **`.Q` proxy**.

    | Field Access Path | Queryable Type | Supported Operators |
    | :--- | :--- | :--- |
    | `Unsigned16.Q.data` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |

    Example:
        ```python
        from mosaicolabs import MosaicoClient, Unsigned16, QueryOntologyCatalog

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter for a specific data value
            qresponse = client.query(QueryOntologyCatalog(Unsigned16.Q.data.eq(2)))

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {[topic.name for topic in item.topics]}")

            # Filter for a specific data value and extract the first and last occurrence times
            qresponse = client.query(
                QueryOntologyCatalog(Unsigned16.Q.data.gt(123), include_timestamp_range=True)
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

    def model_post_init(self, context: Any) -> None:
        """
        Validates that the input data is non-negative.

        Raises:
            ValueError: If data < 0.
        """
        super().model_post_init(context)
        if self.data < 0:
            raise ValueError("Integer must be unsigned")


class Unsigned32(Serializable, HeaderMixin):
    """
    A wrapper for an unsigned 32-bit integer.

    Attributes:
        data: The underlying unsigned 32-bit integer value.
        header: An optional metadata header injected by `HeaderMixin`.

    Raises:
        ValueError: If `data` is initialized with a negative value.

    ### Querying with the **`.Q` Proxy**
    The fields of this class are queryable when constructing a [`QueryOntologyCatalog`][mosaicolabs.models.query.builders.QueryOntologyCatalog]
    via the **`.Q` proxy**. Check the fields documentation for detailed description.

    Example:
        ```python
        from mosaicolabs import MosaicoClient, Unsigned32, QueryOntologyCatalog

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter for a specific data value
            qresponse = client.query(QueryOntologyCatalog(Unsigned32.Q.data.gt(123)))

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {[topic.name for topic in item.topics]}")

            # Filter for a specific data value and extract the first and last occurrence times
            qresponse = client.query(
                QueryOntologyCatalog(Unsigned32.Q.data.gt(123), include_timestamp_range=True)
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
                "data",
                pa.uint32(),
                nullable=False,
                metadata={"description": "32-bit Unsigned data"},
            ),
        ]
    )
    data: int
    """
    The underlying unsigned integer value.
    
    ### Querying with the **`.Q` Proxy**
    This field is queryable when constructing a [`QueryOntologyCatalog`][mosaicolabs.models.query.builders.QueryOntologyCatalog] 
    via the **`.Q` proxy**.

    | Field Access Path | Queryable Type | Supported Operators |
    | :--- | :--- | :--- |
    | `Unsigned32.Q.data` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |

    Example:
        ```python
        from mosaicolabs import MosaicoClient, Unsigned32, QueryOntologyCatalog

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter for a specific data value
            qresponse = client.query(QueryOntologyCatalog(Unsigned32.Q.data.gt(123)))

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {[topic.name for topic in item.topics]}")

            # Filter for a specific data value and extract the first and last occurrence times
            qresponse = client.query(
                QueryOntologyCatalog(Unsigned32.Q.data.gt(123), include_timestamp_range=True)
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

    def model_post_init(self, context: Any) -> None:
        """
        Validates that the input data is non-negative.

        Raises:
            ValueError: If data < 0.
        """
        super().model_post_init(context)
        if self.data < 0:
            raise ValueError("Integer must be unsigned")


class Unsigned64(Serializable, HeaderMixin):
    """
    A wrapper for an unsigned 64-bit integer.

    Attributes:
        data: The underlying unsigned 64-bit integer value.
        header: An optional metadata header injected by `HeaderMixin`.

    Raises:
        ValueError: If `data` is initialized with a negative value.

    ### Querying with the **`.Q` Proxy**
    The fields of this class are queryable when constructing a [`QueryOntologyCatalog`][mosaicolabs.models.query.builders.QueryOntologyCatalog]
    via the **`.Q` proxy**. Check the fields documentation for detailed description.

    Example:
        ```python
        from mosaicolabs import MosaicoClient, Unsigned64, QueryOntologyCatalog

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter for a specific data value
            qresponse = client.query(QueryOntologyCatalog(Unsigned64.Q.data.gt(123)))

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {[topic.name for topic in item.topics]}")

            # Filter for a specific data value and extract the first and last occurrence times
            qresponse = client.query(
                QueryOntologyCatalog(Unsigned64.Q.data.gt(123), include_timestamp_range=True)
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
                "data",
                pa.uint64(),
                nullable=False,
                metadata={"description": "64-bit Unsigned data"},
            ),
        ]
    )
    data: int
    """
    The underlying unsigned integer value.
    
    ### Querying with the **`.Q` Proxy**
    This field is queryable when constructing a [`QueryOntologyCatalog`][mosaicolabs.models.query.builders.QueryOntologyCatalog] 
    via the **`.Q` proxy**.

    | Field Access Path | Queryable Type | Supported Operators |
    | :--- | :--- | :--- |
    | `Unsigned64.Q.data` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |

    Example:
        ```python
        from mosaicolabs import MosaicoClient, Unsigned64, QueryOntologyCatalog

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter for a specific data value
            qresponse = client.query(QueryOntologyCatalog(Unsigned64.Q.data.gt(123)))

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {[topic.name for topic in item.topics]}")

            # Filter for a specific data value and extract the first and last occurrence times
            qresponse = client.query(
                QueryOntologyCatalog(Unsigned64.Q.data.gt(123), include_timestamp_range=True)
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

    def model_post_init(self, context: Any) -> None:
        """
        Validates that the input data is non-negative.

        Raises:
            ValueError: If data < 0.
        """
        super().model_post_init(context)
        if self.data < 0:
            raise ValueError("Integer must be unsigned")


class Floating16(Serializable, HeaderMixin):
    """
    A wrapper for a 16-bit single-precision floating-point number.

    Attributes:
        data: The underlying single-precision float.
        header: An optional metadata header injected by `HeaderMixin`.

    ### Querying with the **`.Q` Proxy**
    The fields of this class are queryable when constructing a [`QueryOntologyCatalog`][mosaicolabs.models.query.builders.QueryOntologyCatalog]
    via the **`.Q` proxy**. Check the fields documentation for detailed description.

    Example:
        ```python
        from mosaicolabs import MosaicoClient, Floating16, QueryOntologyCatalog

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter for a specific data value
            qresponse = client.query(QueryOntologyCatalog(Floating16.Q.data.gt(123.45)))

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {[topic.name for topic in item.topics]}")

            # Filter for a specific data value and extract the first and last occurrence times
            qresponse = client.query(
                QueryOntologyCatalog(Floating16.Q.data.gt(123.45), include_timestamp_range=True)
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
                "data",
                pa.float16(),
                nullable=False,
                metadata={"description": "16-bit Floating-point data"},
            ),
        ]
    )
    data: float
    """
    The underlying single-precision float.
    
    ### Querying with the **`.Q` Proxy**
    This field is queryable when constructing a [`QueryOntologyCatalog`][mosaicolabs.models.query.builders.QueryOntologyCatalog] 
    via the **`.Q` proxy**.

    | Field Access Path | Queryable Type | Supported Operators |
    | :--- | :--- | :--- |
    | `Floating16.Q.data` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |

    Example:
        ```python
        from mosaicolabs import MosaicoClient, Floating16, QueryOntologyCatalog

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter for a specific data value
            qresponse = client.query(QueryOntologyCatalog(Floating16.Q.data.leq(123.4)))

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {[topic.name for topic in item.topics]}")

            # Filter for a specific data value and extract the first and last occurrence times
            qresponse = client.query(
                QueryOntologyCatalog(Floating16.Q.data.gt(123.45), include_timestamp_range=True)
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


class Floating32(Serializable, HeaderMixin):
    """
    A wrapper for a 32-bit single-precision floating-point number.

    Attributes:
        data: The underlying single-precision float.
        header: An optional metadata header injected by `HeaderMixin`.

    ### Querying with the **`.Q` Proxy**
    The fields of this class are queryable when constructing a [`QueryOntologyCatalog`][mosaicolabs.models.query.builders.QueryOntologyCatalog]
    via the **`.Q` proxy**. Check the fields documentation for detailed description.

    Example:
        ```python
        from mosaicolabs import MosaicoClient, Floating32, QueryOntologyCatalog

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter for a specific data value
            qresponse = client.query(QueryOntologyCatalog(Floating32.Q.data.gt(123.45)))

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {[topic.name for topic in item.topics]}")

            # Filter for a specific data value and extract the first and last occurrence times
            qresponse = client.query(
                QueryOntologyCatalog(Floating32.Q.data.gt(123.45), include_timestamp_range=True)
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
                "data",
                pa.float32(),
                nullable=False,
                metadata={"description": "32-bit Floating-point data"},
            ),
        ]
    )
    data: float
    """
    The underlying single-precision float.
    
    ### Querying with the **`.Q` Proxy**
    This field is queryable when constructing a [`QueryOntologyCatalog`][mosaicolabs.models.query.builders.QueryOntologyCatalog] 
    via the **`.Q` proxy**.

    | Field Access Path | Queryable Type | Supported Operators |
    | :--- | :--- | :--- |
    | `Floating32.Q.data` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |

    Example:
        ```python
        from mosaicolabs import MosaicoClient, Floating32, QueryOntologyCatalog

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter for a specific data value
            qresponse = client.query(QueryOntologyCatalog(Floating32.Q.data.leq(123.4)))

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {[topic.name for topic in item.topics]}")

            # Filter for a specific data value and extract the first and last occurrence times
            qresponse = client.query(
                QueryOntologyCatalog(Floating32.Q.data.gt(123.45), include_timestamp_range=True)
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


class Floating64(Serializable, HeaderMixin):
    """
    A wrapper for a 64-bit single-precision floating-point number.

    Attributes:
        data: The underlying single-precision float.
        header: An optional metadata header injected by `HeaderMixin`.

    ### Querying with the **`.Q` Proxy**
    The fields of this class are queryable when constructing a [`QueryOntologyCatalog`][mosaicolabs.models.query.builders.QueryOntologyCatalog]
    via the **`.Q` proxy**. Check the fields documentation for detailed description.

    Example:
        ```python
        from mosaicolabs import MosaicoClient, Floating64, QueryOntologyCatalog

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter for a specific data value
            qresponse = client.query(QueryOntologyCatalog(Floating64.Q.data.gt(123.45)))

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {[topic.name for topic in item.topics]}")

            # Filter for a specific data value and extract the first and last occurrence times
            qresponse = client.query(
                QueryOntologyCatalog(Floating64.Q.data.gt(123.45), include_timestamp_range=True)
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
                "data",
                pa.float64(),
                nullable=False,
                metadata={"description": "64-bit Floating-point data"},
            ),
        ]
    )
    data: float
    """
    The underlying single-precision float.

    ### Querying with the **`.Q` Proxy**
    This field is queryable when constructing a [`QueryOntologyCatalog`][mosaicolabs.models.query.builders.QueryOntologyCatalog] 
    via the **`.Q` proxy**.

    | Field Access Path | Queryable Type | Supported Operators |
    | :--- | :--- | :--- |
    | `Floating64.Q.data` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |

    Example:
        ```python
        from mosaicolabs import MosaicoClient, Floating64, QueryOntologyCatalog

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter for a specific data value
            qresponse = client.query(QueryOntologyCatalog(Floating64.Q.data.leq(123.4)))

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {[topic.name for topic in item.topics]}")

            # Filter for a specific data value and extract the first and last occurrence times
            qresponse = client.query(
                QueryOntologyCatalog(Floating64.Q.data.gt(123.45), include_timestamp_range=True)
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


class Boolean(Serializable, HeaderMixin):
    """
    A wrapper for a standard boolean value.

    Attributes:
        data: The underlying boolean value.
        header: An optional metadata header injected by `HeaderMixin`.

    ### Querying with the **`.Q` Proxy**
    The fields of this class are queryable when constructing a [`QueryOntologyCatalog`][mosaicolabs.models.query.builders.QueryOntologyCatalog]
    via the **`.Q` proxy**. Check the fields documentation for detailed description.

    Example:
        ```python
        from mosaicolabs import MosaicoClient, Boolean, QueryOntologyCatalog

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter for a specific data value
            qresponse = client.query(QueryOntologyCatalog(Boolean.Q.data.eq(True)))

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {[topic.name for topic in item.topics]}")

            # Filter for a specific data value and extract the first and last occurrence times
            qresponse = client.query(
                QueryOntologyCatalog(Boolean.Q.data.eq(True), include_timestamp_range=True)
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
                "data",
                pa.bool_(),
                nullable=False,
                metadata={"description": "Boolean data"},
            ),
        ]
    )
    data: bool
    """
    The underlying boolean value.
    
    ### Querying with the **`.Q` Proxy**
    This field is queryable when constructing a [`QueryOntologyCatalog`][mosaicolabs.models.query.builders.QueryOntologyCatalog] 
    via the **`.Q` proxy**.

    | Field Access Path | Queryable Type | Supported Operators |
    | :--- | :--- | :--- |
    | `Boolean.Q.data` | `Bool` | `.eq()` |

    Example:
        ```python
        from mosaicolabs import MosaicoClient, Boolean, QueryOntologyCatalog

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter for a specific data value
            qresponse = client.query(QueryOntologyCatalog(Boolean.Q.data.eq(True)))

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {[topic.name for topic in item.topics]}")

            # Filter for a specific data value and extract the first and last occurrence times
            qresponse = client.query(
                QueryOntologyCatalog(Boolean.Q.data.eq(True), include_timestamp_range=True)
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


class String(Serializable, HeaderMixin):
    """
    A wrapper for a standard UTF-8 encoded string.

    Attributes:
        data: The underlying string data.
        header: An optional metadata header injected by `HeaderMixin`.

    ### Querying with the **`.Q` Proxy**
    The fields of this class are queryable when constructing a [`QueryOntologyCatalog`][mosaicolabs.models.query.builders.QueryOntologyCatalog]
    via the **`.Q` proxy**. Check the fields documentation for detailed description.

    Example:
        ```python
        from mosaicolabs import MosaicoClient, String, QueryOntologyCatalog

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter for a specific data value
            qresponse = client.query(QueryOntologyCatalog(String.Q.data.eq("hello")))

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {[topic.name for topic in item.topics]}")

            # Filter for a specific data value and extract the first and last occurrence times
            qresponse = client.query(
                QueryOntologyCatalog(String.Q.data.eq("hello"), include_timestamp_range=True)
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
                "data",
                pa.string(),
                nullable=False,
                metadata={"description": "String data"},
            ),
        ]
    )
    data: str
    """
    The underlying string data.
    
    ### Querying with the **`.Q` Proxy**
    This field is queryable when constructing a [`QueryOntologyCatalog`][mosaicolabs.models.query.builders.QueryOntologyCatalog] 
    via the **`.Q` proxy**.

    | Field Access Path | Queryable Type | Supported Operators |
    | :--- | :--- | :--- |
    | `String.Q.data` | `String` | `.eq()`, `.neq()`, `.match()`, `.in_()` |

    Example:
        ```python
        from mosaicolabs import MosaicoClient, String, QueryOntologyCatalog

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter for strings containing a specific log pattern
            qresponse = client.query(QueryOntologyCatalog(String.Q.data.match("[ERR]")))

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {[topic.name for topic in item.topics]}")

            # Filter for a specific data value and extract the first and last occurrence times
            qresponse = client.query(
                QueryOntologyCatalog(String.Q.data.eq("hello"), include_timestamp_range=True)
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


class LargeString(Serializable, HeaderMixin):
    """
    A wrapper for a Large UTF-8 encoded string.

    Use this class when string data is expected to exceed 2GB in size, necessitating
    the use of 64-bit offsets in the underlying PyArrow implementation.

    Attributes:
        data: The underlying large string data.
        header: An optional metadata header injected by `HeaderMixin`.

    ### Querying with the **`.Q` Proxy**
    The fields of this class are queryable when constructing a [`QueryOntologyCatalog`][mosaicolabs.models.query.builders.QueryOntologyCatalog]
    via the **`.Q` proxy**. Check the fields documentation for detailed description.

    Example:
        ```python
        from mosaicolabs import MosaicoClient, LargeString, QueryOntologyCatalog

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter for a specific data value
            qresponse = client.query(QueryOntologyCatalog(LargeString.Q.data.eq("hello")))

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {[topic.name for topic in item.topics]}")

            # Filter for a specific data value and extract the first and last occurrence times
            qresponse = client.query(
                QueryOntologyCatalog(LargeString.Q.data.eq("hello"), include_timestamp_range=True)
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
                "data",
                pa.large_string(),
                nullable=False,
                metadata={"description": "Large string data"},
            ),
        ]
    )
    data: str
    """
    The underlying large string data.

    ### Querying with the **`.Q` Proxy**
    This field is queryable when constructing a [`QueryOntologyCatalog`][mosaicolabs.models.query.builders.QueryOntologyCatalog] 
    via the **`.Q` proxy**.

    | Field Access Path | Queryable Type | Supported Operators |
    | :--- | :--- | :--- |
    | `LargeString.Q.data` | `String` | `.eq()`, `.neq()`, `.match()`, `.in_()` |

    Example:
        ```python
        from mosaicolabs import MosaicoClient, LargeString, QueryOntologyCatalog

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter for large strings containing a specific log pattern
            qresponse = client.query(QueryOntologyCatalog(LargeString.Q.data.match("CRITICAL_ERR_")))

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {[topic.name for topic in item.topics]}")

            # Filter for a specific data value and extract the first and last occurrence times
            qresponse = client.query(
                QueryOntologyCatalog(LargeString.Q.data.eq("hello"), include_timestamp_range=True)
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
