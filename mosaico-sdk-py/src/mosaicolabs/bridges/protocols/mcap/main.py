from pathlib import Path

from rich.console import Console
from rich.table import Table

from mcap.reader import make_reader

# from mcap_protobuf.decoder import DecoderFactory
from mosaicolabs.bridges.protocols.mcap.registry import McapSchemaRegistry

console = Console()


def main():

    PATH_TO_MCAPS = "/mnt/datasets/mcaps"

    all_mcap_files = Path(PATH_TO_MCAPS).rglob("*.mcap")

    for mcap_file in all_mcap_files:
        table = Table(title=str(mcap_file))
        table.add_column("Topic")
        table.add_column("Encoding")
        table.add_column("Status", justify="center")

        with open(mcap_file, "rb") as f:
            # reader = make_reader(f, decoder_factories=[DecoderFactory()])
            reader = make_reader(f)

            mcap_summary = reader.get_summary()

            assert mcap_summary is not None

            for id, channel in mcap_summary.channels.items():
                schema = mcap_summary.schemas.get(channel.schema_id)

                if not schema:
                    table.add_row(
                        channel.topic,
                        "no schema",
                        "X",
                        style="red",
                    )
                    continue

                converter = McapSchemaRegistry.get_converter(schema.encoding)

                if converter is None:
                    table.add_row(
                        channel.topic,
                        schema.encoding,
                        "X",
                        style="red",
                    )
                    continue

                try:
                    converter.to_pyarrow(schema)
                except Exception as e:
                    table.add_row(
                        channel.topic,
                        schema.encoding,
                        "X",
                        style="red",
                    )
                    print(e)
                    continue

                table.add_row(channel.topic, schema.encoding, "✓")

        console.print(table)


if __name__ == "__main__":
    main()
