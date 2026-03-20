"""
This example demonstrates how to query the MosaicoDB catalog for topics and sequences.
This example can be run after running the ingestion example (examples/ros_injection/main.py).

Ingest data via:
```bash
cd mosaico-sdk-py/src/examples && poetry run python ros_injection/main.py
```

Then, query catalogs via:
```bash
poetry run python query_catalogs.py
```

"""

from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from mosaicolabs import (
    IMU,
    MosaicoClient,
    QueryOntologyCatalog,
    QueryTopic,
)

from .config import MOSAICO_HOST, MOSAICO_PORT

# Initialize Rich Console for beautiful terminal output
console = Console()


def query_by_topic_name_match(client: MosaicoClient, test_num: int):
    # Execute a unified multi-domain query
    topic_name_match = "image_raw"
    console.print(
        Panel(
            f"[bold green]TEST {test_num}: Querying catalog for Topics which name matches '{topic_name_match}'[/bold green]"
        )
    )
    results = client.query(
        # Filter 1: Topic Layer (Specific Channel Name)
        QueryTopic().with_name_match(topic_name_match),  # Partial name match
    )

    # Process the Structured Response
    if results:
        # Create table
        table = Table(title=f"Query returned {len(results)} items")
        # Add columns
        table.add_column("Sequence", style="magenta")
        table.add_column("Topic", style="green")
        for item in results:
            # item.topics contains only the topics and time-segments
            # that satisfied ALL criteria simultaneously
            for topic in item.topics:
                table.add_row(item.sequence.name, topic.name)

        console.print(table)


def query_by_ontology(client: MosaicoClient, test_num: int):
    # Execute a unified multi-domain query
    console.print(
        Panel(
            f"[bold green]TEST {test_num}: Querying catalog for Topics related to the '{IMU.__name__}' data type[/bold green]"
        )
    )
    results = client.query(
        # Filter 1: Topic Layer (Specific Channel Name)
        QueryTopic().with_ontology_tag(IMU.ontology_tag()),  # Partial name match
    )

    # Process the Structured Response
    if results:
        # Create table
        table = Table(title=f"Query returned {len(results)} items")
        # Add columns
        table.add_column("Sequence", style="magenta")
        table.add_column("Topic", style="green")
        for item in results:
            # item.topics contains only the topics and time-segments
            # that satisfied ALL criteria simultaneously
            for topic in item.topics:
                table.add_row(item.sequence.name, topic.name)

        console.print(table)


def query_acceleration_camera_imu(client: MosaicoClient, test_num: int):
    # Execute a unified multi-domain query
    console.print(
        Panel(
            f"[bold green]TEST {test_num}: Querying catalog for IMU accelerations (ay >= 1) AND topic name '/front_stereo_imu/imu'[/bold green]"
        )
    )
    results = client.query(
        # Filter 1: Topic Layer (Specific Channel Name)
        QueryOntologyCatalog(
            IMU.Q.acceleration.y.geq(1),
            include_timestamp_range=True,
        ),
        QueryTopic().with_name("/front_stereo_imu/imu"),
    )

    # Process the Structured Response
    if results:
        # Create table
        table = Table(title=f"Query returned {len(results)} items")
        # Add columns
        table.add_column("Sequence", style="magenta")
        table.add_column("Topic", style="green")
        table.add_column("Timestamp Range (ns)", style="green")
        for item in results:
            # item.topics contains only the topics and time-segments
            # that satisfied ALL criteria simultaneously
            for topic in item.topics:
                start = topic.timestamp_range.start if topic.timestamp_range else "N/A"
                end = topic.timestamp_range.end if topic.timestamp_range else "N/A"
                table.add_row(
                    item.sequence.name,
                    topic.name,
                    f"{start}-{end}",
                )

        console.print(table)


def main():
    # Establish a connection
    with MosaicoClient.connect(MOSAICO_HOST, MOSAICO_PORT) as client:
        # Example 1
        query_by_topic_name_match(client=client, test_num=1)
        # Example 2
        query_by_ontology(client=client, test_num=2)
        # Example 3
        query_acceleration_camera_imu(client=client, test_num=3)


if __name__ == "__main__":
    main()
