"""
Joy Ontology Module.

Represents joystick input state.
"""

from ..core import MosaicoField, MosaicoType, Serializable
from ..data import HeaderMixin


class Joy(
    Serializable,
    HeaderMixin,  # Adds Header support
):
    """
    Joystick input data.

    This class represents the state of a joystick, including axis values and button states.

    Attributes:
        axes: Continuous axis values (e.g., joystick positions).
        buttons: Discrete button states (pressed or not pressed).
        header (optional[Header]): Optional heading containing measurement metadata

    ### Querying with the **`.Q` Proxy**
    This class is fully queryable via the **`.Q` proxy**. You can filter joystick data based
    on thresholds values within a [`QueryOntologyCatalog`][mosaicolabs.models.query.builders.QueryOntologyCatalog].
    Since `axes` and `buttons` are lists of values, use `all()`, `any()` or index access `[i]`
    to narrow down to the list element and compose a correct expression.

    Example:
        ```python
        from mosaicolabs import MosaicoClient, QueryTopic, Joy

        with MosaicoClient.connect("localhost", 6726) as client:
            # Fetch all sequences that contain at least one Joy topic
            qresponse = client.query(QueryTopic().with_ontology_tag(Joy.ontology_tag()))

            if qresponse is not None:

                for item in qresponse.items:
                    print(f"Sequence: {item.name}")
                    print(f"Topics:   {[topic.name for topic in item.topics]}")
        ```
    """

    axes: MosaicoType.list_(MosaicoType.float32) = MosaicoField(
        description="The axes measurements from a joystick."
    )
    """
    Continuous axis values of the joystick.

    ### Querying with the **`.Q` Proxy**
    The axes value is queryable via the `axes` field. Since it represents a list of values, use
    `all()`, `any()` or index access `[i]` to narrow down to the list element and compose a
    correct expression.
        - Joy.Q.axes.all()       -> invalid expression
        - Joy.Q.axes.gt(1)       -> invalid expression
        - Joy.Q.axes.all().gt(1) -> valid expression

    | Field Access Path | Queryable Type | Supported Operators |
    | :--- | :--- | :--- |
    | `Joy.Q.axes.all()` | `Numeric` | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    | `Joy.Q.axes.any()` | `Numeric` | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    | `Joy.Q.axes.[i]` | `Numeric` | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |

    Example:
        ```python
        from mosaicolabs import MosaicoClient, QueryOntologyCatalog, Joy

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter for joystick samples with at least one axis pushed to the extreme
            qresponse = client.query(
                QueryOntologyCatalog(Joy.Q.axes.any().gt(0.9))
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

    buttons: MosaicoType.list_(MosaicoType.int32) = MosaicoField(
        description="The buttons measurements from a joystick."
    )
    """
    Discrete button states (1 = pressed, 0 = released).

    ### Querying with the **`.Q` Proxy**
    The buttons value is queryable via the `buttons` field. Since it represents a list of
    values, use `all()`, `any()` or index access `[i]` to narrow down to the list element and
    compose a correct expression.
        - Joy.Q.buttons.all()       -> invalid expression
        - Joy.Q.buttons.gt(1)       -> invalid expression
        - Joy.Q.buttons.all().gt(1) -> valid expression

    | Field Access Path | Queryable Type | Supported Operators |
    | :--- | :--- | :--- |
    | `Joy.Q.buttons.all()` | `Numeric` | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    | `Joy.Q.buttons.any()` | `Numeric` | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    | `Joy.Q.buttons.[i]` | `Numeric` | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |

    Example:
        ```python
        from mosaicolabs import MosaicoClient, QueryOntologyCatalog, Joy

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter for joystick samples with at least one button pressed
            qresponse = client.query(
                QueryOntologyCatalog(Joy.Q.buttons.any().eq(1))
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
