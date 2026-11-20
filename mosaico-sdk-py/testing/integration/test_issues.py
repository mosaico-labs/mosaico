from mosaicolabs import (
    Message,
    MosaicoClient,
    MosaicoField,
    MosaicoType,
    Serializable,
    SessionLevelErrorPolicy,
)


class CustomOntologyWithListBinary(Serializable):
    blobs: MosaicoType.list_(MosaicoType.binary) = MosaicoField(
        description="Variable-length list of binary blobs."
    )


def test_issue_660(mosaico_client: MosaicoClient):
    "Link to the specific issue: https://github.com/mosaico-labs/mosaico/issues/660"

    # Writing a sequence with one topic containing the custom ontology with the list of binary
    sequence_name = "custom_ontology_with_list_binaries"
    topic_name = "/custom_name/topic_w_binary_list"

    with mosaico_client.sequence_create(
        sequence_name, {}, on_error=SessionLevelErrorPolicy.Delete
    ) as seqw:
        tw = seqw.topic_create(topic_name, {}, CustomOntologyWithListBinary)
        assert tw is not None
        tw.push(
            Message(
                timestamp_ns=1, data=CustomOntologyWithListBinary(blobs=[b"x", b"y"])
            )
        )

    # Reading jst loaded topic to check that is correctly decoded
    th = mosaico_client.topic_handler(sequence_name, topic_name)
    assert th is not None
    streamer = th.get_data_streamer()

    # Try to read messages
    for msg in streamer:
        assert issubclass(msg.data.__class_type__, CustomOntologyWithListBinary)

    mosaico_client.sequence_delete(sequence_name)
