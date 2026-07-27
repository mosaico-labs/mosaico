"""

This module defines the fundamental building blocks for grids and maps representation, including grid cells, map metadata and occupancy grids.

"""

from ..core import MosaicoField, MosaicoType, Serializable
from ..data import HeaderMixin, Point3d, Pose, Time


class GridCells(
    Serializable,
    HeaderMixin,  # Adds Header support
):
    """
    Grid Cells data.

    This class represents the grid cells.

    Attributes:
        cell_width: A `MosaicoType.float32` that represents the width of each cell.
        cell_height: A `MosaicoType.float32` that represents the width of each cell.
        cells: A `MosaicoType.list_(Point2d)` that represents the center point of
            each cell.
        header (optional[Header]): Optional heading containing measurement metadata

    ### Querying with the **`.Q` Proxy**
    This class is fully queryable via the **`.Q` proxy**. You can filter grid cells data based
    on `cell_width`, `cell_height`, or `cells` field values within a [`QueryOntologyCatalog`][mosaicolabs.query.builders.QueryOntologyCatalog].

    Example:
        ```python
        from mosaicolabs import MosaicoClient, GridCells, QueryOntologyCatalog

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter for cell grid width field values within a specific range
            qresponse = client.query(
                QueryOntologyCatalog(GridCells.Q.cell_width.between(100, 200))
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

    cell_width: MosaicoType.float32 = MosaicoField(description="Width of each cell.")
    """
    Width of each cell.

    ### Querying with the **`.Q` Proxy**
    The grid cells width is queryable via the `cell_width` field.

    | Field Access Path | Queryable Type | Supported Operators |
    | :--- | :--- | :--- |
    | `GridCells.Q.cell_width` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()`, `.outside()` |
    
    
    Example:
        ```python
        from mosaicolabs import MosaicoClient, GridCells, QueryOntologyCatalog

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter for cell width within a specific range
            qresponse = client.query(
                QueryOntologyCatalog(GridCells.Q.cell_width.between([100, 200]))
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

    cell_height: MosaicoType.float32 = MosaicoField(description="Height of each cell.")
    """
    Height of each cell.

    ### Querying with the **`.Q` Proxy**
    The grid cells height is queryable via the `cell_height` field.

    | Field Access Path | Queryable Type | Supported Operators |
    | :--- | :--- | :--- |
    | `GridCells.Q.cell_height` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()`, `.outside()` |
    
    
    Example:
        ```python
        from mosaicolabs import MosaicoClient, GridCells, QueryOntologyCatalog

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter for cell width within a specific range
            qresponse = client.query(
                QueryOntologyCatalog(GridCells.Q.cell_height.between([100, 200]))
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

    cells: MosaicoType.list_(Point3d) = MosaicoField(
        description="The cell represented by a point at it's center."
    )
    """
    The cell represented by a point at it's center.

    ### Querying with the **`.Q` Proxy**
    The cells value is queryable via the `cells` field. Since it represents a list of `Point3d`
    values, use `all()`, `any()` or index access `[i]` to narrow down to the list element, then
    continue the expression with the contained `Point3d` field (`x`, `y` or `z`).
        - GridCells.Q.cells.all()         -> invalid expression
        - GridCells.Q.cells.x.gt(1)       -> invalid expression
        - GridCells.Q.cells.all().x.gt(1) -> valid expression

    | Field Access Path | Queryable Type | Supported Operators |
    | :--- | :--- | :--- |
    | `GridCells.Q.cells.all().x` | `Numeric` | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()`, `.outside()` |
    | `GridCells.Q.cells.any().x` | `Numeric` | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()`, `.outside()` |
    | `GridCells.Q.cells.[i].x` | `Numeric` | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()`, `.outside()` |

    Example:
        ```python
        from mosaicolabs import MosaicoClient, GridCells, QueryOntologyCatalog

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter for grids with at least one cell beyond a specific X-coordinate
            qresponse = client.query(
                QueryOntologyCatalog(GridCells.Q.cells.any().x.gt(500.0))
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


class MapMetadata(
    Serializable,
):
    """
    Represents metadata about the map, like it's width and height.
    Typically used in combination with OccupancyGrid

    Attributes:
        map_load_time: A `Time` representing the time at which the map has
            been loaded.
        resolution: A `MosaicoType.float32` representing the resolution
            of the map.
        width: A `MosaicoType.uint32` representing the number of cells that
            represent the width of the map.
        height: A `MosaicoType.uint32` representing the number of cells that
            represent the height of the map.
        origin: A `Pose` that represents where the map starts in the real world.

    ### Querying with the **`.Q` Proxy**
    This class fields are queryable when constructing a [`QueryOntologyCatalog`][mosaicolabs.query.builders.QueryOntologyCatalog]
    via the **`.Q` proxy**. Check the fields documentation for detailed description.

    Example:
        ```python
        from mosaicolabs import MosaicoClient, MapMetadata, QueryOntologyCatalog

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter MapMetadatas with width AND height
            qresponse = client.query(
                QueryOntologyCatalog(MapMetadata.Q.width.gt(100))
                .with_expression(MapMetadata.Q.height.lt(200))
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

    map_load_time: Time = MosaicoField(
        description="Time (in nanoseconds) at which the map has been loaded."
    )
    """
    Time (in nanoseconds) at which the map has been loaded.

    ### Querying with the **`.Q` Proxy**
    The map metadata time is queryable via the `map_load_time` field.

    | Field Access Path | Queryable Type | Supported Operators |
    | :--- | :--- | :--- |
    | `MapMetadata.Q.map_load_time` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()`, `.outside()` |
    
    
    Example:
        ```python
        from mosaicolabs import MosaicoClient, MapMetadata, QueryOntologyCatalog

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter for map_load_time in nanoseconds within a specific range
            qresponse = client.query(
                QueryOntologyCatalog(MapMetadata.Q.map_load_time.between([100000, 200000]))
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

    resolution: MosaicoType.float32 = MosaicoField(
        description="Resolution of the map [m/cell]."
    )
    """
    Resolution of the map.

    ### Querying with the **`.Q` Proxy**
    The map metadata resolution is queryable via the `resolution` field.

    | Field Access Path | Queryable Type | Supported Operators |
    | :--- | :--- | :--- |
    | `MapMetadata.Q.resolution` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()`, `.outside()` |
    
    Example:
        ```python
        from mosaicolabs import MosaicoClient, MapMetadata, QueryOntologyCatalog

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter for resolution within a specific range
            qresponse = client.query(
                QueryOntologyCatalog(MapMetadata.Q.resolution.between([100000, 200000]))
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

    width: MosaicoType.uint32 = MosaicoField(
        description="Number of cells representing the width of the map [cells]."
    )
    """
    Number of cells representing the width of the map.

    ### Querying with the **`.Q` Proxy**
    The map metadata width is queryable via the `width` field.

    | Field Access Path | Queryable Type | Supported Operators |
    | :--- | :--- | :--- |
    | `MapMetadata.Q.width` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()`, `.outside()` |
    
    Example:
        ```python
        from mosaicolabs import MosaicoClient, MapMetadata, QueryOntologyCatalog

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter for width within a specific range
            qresponse = client.query(
                QueryOntologyCatalog(MapMetadata.Q.width.between([10, 20]))
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

    height: MosaicoType.uint32 = MosaicoField(
        description="Number of cells representing the height of the map [cells]."
    )
    """
    Number of cells representing the height of the map.

    ### Querying with the **`.Q` Proxy**
    The map metadata height is queryable via the `height` field.

    | Field Access Path | Queryable Type | Supported Operators |
    | :--- | :--- | :--- |
    | `MapMetadata.Q.height` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()`, `.outside()` |
    
    Example:
        ```python
        from mosaicolabs import MosaicoClient, MapMetadata, QueryOntologyCatalog

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter for height within a specific range
            qresponse = client.query(
                QueryOntologyCatalog(MapMetadata.Q.height.between([10, 20]))
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

    origin: Pose = MosaicoField(description="Where the map starts in the real world.")
    """

    The origin of the map [m, m, rad]. This is the real-world pose of the
    bottom left corner of cell (0,0) in the map.

    ### Querying with the **`.Q` Proxy**
    The map metadata origin is queryable via the `origin` field.

    | Field Access Path | Queryable Type | Supported Operators |
    | :--- | :--- | :--- |
    | `MapMetadata.Q.origin.position.x` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()`, `.outside()` |
    | `MapMetadata.Q.origin.position.y` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()`, `.outside()` |
    | `MapMetadata.Q.origin.position.z` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()`, `.outside()` |
    | `MapMetadata.Q.origin.orientation.x` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()`, `.outside()` |
    | `MapMetadata.Q.origin.orientation.y` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()`, `.outside()` |
    | `MapMetadata.Q.origin.orientation.z` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()`, `.outside()` |
    | `MapMetadata.Q.origin.orientation.w` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()`, `.outside()` |

    Example:
        ```python
        from mosaicolabs import MosaicoClient, MapMetadata, QueryOntologyCatalog

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter map metadata where the object is beyond a specific X-coordinate
            qresponse = client.query(
                QueryOntologyCatalog(MapMetadata.Q.origin.position.x.gt(500.0))
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


class OccupancyGrid(
    Serializable,
    HeaderMixin,  # Adds Header support
):
    """
    Occupancy Grid data.

    This class represents the occupancy grid.

    Attributes:
        info: A `MapMetadata` describing the occupancy grid.
        data: A `MosaicoType.list_(MosaicoType.int8)` representing data contained in the occupancy grid.
        header (optional[Header]): Optional heading containing measurement metadata

    ### Querying with the **`.Q` Proxy**
    This class is fully queryable via the **`.Q` proxy**. You can filter occupancy grid data based
    on `info` or `data` field values within a [`QueryOntologyCatalog`][mosaicolabs.query.builders.QueryOntologyCatalog].

    Example:
        ```python
        from mosaicolabs import MosaicoClient, OccupancyGrid, QueryOntologyCatalog

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter for grid width field values within a specific range
            qresponse = client.query(
                QueryOntologyCatalog(OccupancyGrid.Q.info.width.between(-100, 100))
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

    info: MapMetadata = MosaicoField(
        description="Info about the map like it's width and height."
    )
    """
    Info about the map like it's width and height.

    ### Querying with the **`.Q` Proxy**
    The occupancy grid info is queryable via the `info` field.

    | Field Access Path | Queryable Type | Supported Operators |
    | :--- | :--- | :--- |
    | `OccupancyGrid.Q.info.map_load_time` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()`, `.outside()` |
    | `OccupancyGrid.Q.info.resolution` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()`, `.outside()` |
    | `OccupancyGrid.Q.info.width` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()`, `.outside()` |
    | `OccupancyGrid.Q.info.height` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()`, `.outside()` |
    | `OccupancyGrid.Q.info.origin.position.x` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()`, `.outside()` |
    | `OccupancyGrid.Q.info.origin.position.y` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()`, `.outside()` |
    | `OccupancyGrid.Q.info.origin.position.z` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()`, `.outside()` |
    | `OccupancyGrid.Q.info.origin.orientation.x` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()`, `.outside()` |
    | `OccupancyGrid.Q.info.origin.orientation.y` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()`, `.outside()` |
    | `OccupancyGrid.Q.info.origin.orientation.z` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()`, `.outside()` |
    | `OccupancyGrid.Q.info.origin.orientation.w` | `Numeric` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()`, `.outside()` |

    
    Example:
        ```python
        from mosaicolabs import MosaicoClient, OccupancyGrid, QueryOntologyCatalog

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter for time seconds within a specific range
            qresponse = client.query(
                QueryOntologyCatalog(OccupancyGrid.Q.info.map_load_time.between([100000, 200000]))
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

    data: MosaicoType.list_(MosaicoType.int8) = MosaicoField(
        description="Occupancy probability: 1 means occupied, 0 means unoccupied and -1 means unkown."
    )
    """

    The map data, in row-major order, starting with (0,0).
    Occupancy probabilities are in the range [0,100].  Unknown is -1.

    ### Querying with the **`.Q` Proxy**
    The data value is queryable via the `data` field. Since it represents a list of values, use
    `all()`, `any()` or index access `[i]` to narrow down to the list element and compose a
    correct expression.
        - OccupancyGrid.Q.data.all()       -> invalid expression
        - OccupancyGrid.Q.data.gt(1)       -> invalid expression
        - OccupancyGrid.Q.data.all().gt(1) -> valid expression

    | Field Access Path | Queryable Type | Supported Operators |
    | :--- | :--- | :--- |
    | `OccupancyGrid.Q.data.all()` | `Numeric` | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()`, `.outside()` |
    | `OccupancyGrid.Q.data.any()` | `Numeric` | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()`, `.outside()` |
    | `OccupancyGrid.Q.data.[i]` | `Numeric` | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()`, `.outside()` |

    Example:
        ```python
        from mosaicolabs import MosaicoClient, OccupancyGrid, QueryOntologyCatalog

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter for occupancy grids with at least one fully occupied cell
            qresponse = client.query(
                QueryOntologyCatalog(OccupancyGrid.Q.data.any().eq(100))
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
