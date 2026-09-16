from typing import Dict

from google.protobuf.descriptor_pb2 import FileDescriptorSet
from mcap.reader import make_reader

from mosaicolabs.bridges.mcap.decoders import (
    DecoderRegistry,
)


def test_get_schema_class(mcap_mixed_file, channelname_to_protobuf):
    """Checks that reconstructed Python Objects deduced from schema definition are the same
    as the one used to create the mcap file itself. Relies to channel encoding to get the
    correct Decoder to create the Python Class"""

    mcap_file = open(mcap_mixed_file, "rb")

    reader = make_reader(
        mcap_file,
        decoder_factories=[
            decoder_cls.decoder_factory()
            for decoder_cls in DecoderRegistry.all_decoders()
        ],
    )

    for schema, channel, _, _ in reader.iter_decoded_messages():
        assert schema is not None

        decoder_cls = DecoderRegistry().get_decoder(channel.message_encoding)
        assert decoder_cls is not None, print(f"Encoding is {schema.encoding}")
        decoder = decoder_cls()
        decoder.register_schema(schema)

        if schema.encoding == "protobuf":

            def field_signature(descr):
                return {f.name: (f.number, f.type, f.is_repeated) for f in descr.fields}

            deduced_protobuf_cls = decoder.get_schema_class(schema.name, schema.data)
            original_protobuf_cls = channelname_to_protobuf[channel.topic]

            assert (
                deduced_protobuf_cls.DESCRIPTOR.full_name
                == original_protobuf_cls.DESCRIPTOR.full_name
            )
            assert field_signature(deduced_protobuf_cls.DESCRIPTOR) == field_signature(
                original_protobuf_cls.DESCRIPTOR
            )

        elif schema.encoding == "jsonschema":
            assert decoder.get_schema_class(schema.name, schema.data) is Dict


def test_stringify_schema_def_round_trips_to_original_bytes(mcap_mixed_file):
    """For every registered decoder, `stringify_schema_def`/`destringify_schema_def` must be
    exact inverses of each other, regardless of whether `schema.data` is binary (protobuf's
    serialized `FileDescriptorSet`) or text (jsonschema's JSON schema)."""

    mcap_file = open(mcap_mixed_file, "rb")

    reader = make_reader(
        mcap_file,
        decoder_factories=[
            decoder_cls.decoder_factory()
            for decoder_cls in DecoderRegistry.all_decoders()
        ],
    )

    for schema, channel, _, _ in reader.iter_decoded_messages():
        assert schema is not None

        decoder_cls = DecoderRegistry().get_decoder(channel.message_encoding)
        assert decoder_cls is not None

        schema_def_str = decoder_cls.stringify_schema_def(schema.data)
        assert isinstance(schema_def_str, str)
        reconstructed_schema_def = decoder_cls.destringify_schema_def(schema_def_str)
        assert reconstructed_schema_def == schema.data

        if schema.encoding == "protobuf":
            assert FileDescriptorSet.FromString(
                schema.data
            ) == FileDescriptorSet.FromString(reconstructed_schema_def)
