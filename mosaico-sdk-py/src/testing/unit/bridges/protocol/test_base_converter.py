from google.protobuf.descriptor_pb2 import FileDescriptorSet
from mcap.reader import make_reader

from mosaicolabs.bridges.mcap.decoders import (
    DecoderRegistry,
)
from mosaicolabs.bridges.protocols.mcap import McapSchemaRegistry


def test_get_schema_class(mcap_mixed_file, channelname_to_protobuf):
    """Checks that reconstructed Python Objects deduced from schema definition are the same
    as the one used to create the mcap file itself. Relies to channel encoding to get the
    correct Decoder to create the Python Class"""

    mcap_file = open(mcap_mixed_file, "rb")

    reader = make_reader(
        mcap_file,
        decoder_factories=[
            decoder_cls.decoder_factory()
            for decoder_cls in DecoderRegistry.list_decoders()
        ],
    )

    for schema, channel, _, _ in reader.iter_decoded_messages():
        assert schema is not None

        schema_converter = McapSchemaRegistry().get_converter(schema.encoding)
        assert schema_converter is not None, print(f"Encoding is {schema.encoding}")

        if schema.encoding == "protobuf":

            def field_signature(descr):
                return {f.name: (f.number, f.type, f.is_repeated) for f in descr.fields}

            deduced_protobuf_cls = schema_converter.get_schema_class(
                schema.name, schema.data
            )
            original_protobuf_cls = channelname_to_protobuf[channel.topic]

            assert (
                deduced_protobuf_cls.DESCRIPTOR.full_name
                == original_protobuf_cls.DESCRIPTOR.full_name
            )
            assert field_signature(deduced_protobuf_cls.DESCRIPTOR) == field_signature(
                original_protobuf_cls.DESCRIPTOR
            )

        elif schema.encoding == "jsonschema":
            assert schema_converter.get_schema_class(schema.name, schema.data) is dict


def test_stringify_schema_def_round_trips_to_original_bytes(mcap_mixed_file):
    """For every registered decoder, `stringify_schema_def`/`destringify_schema_def` must be
    exact inverses of each other, regardless of whether `schema.data` is binary (protobuf's
    serialized `FileDescriptorSet`) or text (jsonschema's JSON schema)."""

    mcap_file = open(mcap_mixed_file, "rb")

    reader = make_reader(
        mcap_file,
        decoder_factories=[
            decoder_cls.decoder_factory()
            for decoder_cls in DecoderRegistry.list_decoders()
        ],
    )

    for schema, _, _, _ in reader.iter_decoded_messages():
        assert schema is not None

        schema_converter = McapSchemaRegistry().get_converter(schema.encoding)
        assert schema_converter is not None

        schema_def_str = schema_converter.stringify_schema_def(schema.data)
        assert isinstance(schema_def_str, str)
        reconstructed_schema_def = schema_converter.destringify_schema_def(
            schema_def_str
        )
        assert reconstructed_schema_def == schema.data

        if schema.encoding == "protobuf":
            assert FileDescriptorSet.FromString(
                schema.data
            ) == FileDescriptorSet.FromString(reconstructed_schema_def)
