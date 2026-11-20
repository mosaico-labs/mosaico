import pyarrow as pa
import pytest

from mosaicolabs import Message, SerializationFormat
from mosaicolabs.models.core.unmodeled import make_unmodeled_ontology_class


def test_message_not_serializable():
    """Test the correct exception raise if the data is not serializable"""
    data = int(0)
    with pytest.raises(
        ValueError,
        match="Input should be a valid dictionary or instance of Serializable",
    ):
        Message(timestamp_ns=0, data=data)


def test_model_with_colliding_fields():
    """
    Test the correct exception for models with schema fields colliding with
    Message fields
    """

    # Raises with the same definition of 'timestamp_ns' field (same name, same schema of Message)
    DataModel = make_unmodeled_ontology_class(
        "CollidingFieldsModel",
        None,
        SerializationFormat.Default,
        pa.struct(
            [
                pa.field(
                    "timestamp_ns",
                    pa.int64(),
                    nullable=False,
                    metadata={
                        "description": "Ingestion timestamp in nanoseconds (record time)."
                    },
                ),
            ]
        ),
    )

    # Test when constructing a Message
    with pytest.raises(ValueError, match="Fields name collision detected"):
        Message(timestamp_ns=123456, data=DataModel(raw_data={"timestamp_ns": 0}))

    # Test when returning the full schema
    with pytest.raises(ValueError, match="Fields name collision detected"):
        Message._get_schema(DataModel.__msco_pyarrow_struct__)

    # Raises with the a different definition of 'timestamp_ns' field (same name, different schema)
    DataModel = make_unmodeled_ontology_class(
        "CollidingFieldsModelOther",
        None,
        SerializationFormat.Default,
        pa.struct(
            [
                pa.field(
                    "timestamp_ns",
                    pa.struct(
                        [
                            # Left nullable (default) on purpose: exercises the "optional
                            # nested field can be omitted" case in the schema-mismatch tests.
                            pa.field("sec", pa.int32()),
                            pa.field("nanosec", pa.uint32()),
                        ]
                    ),
                    nullable=False,
                    metadata={
                        "description": "Ingestion timestamp in nanoseconds (record time)."
                    },
                ),
            ]
        ),
    )

    # Test when constructing a Message
    with pytest.raises(ValueError, match="Fields name collision detected"):
        Message(
            timestamp_ns=123456,
            data=DataModel(raw_data={"timestamp_ns": {"sec": 0, "nanosec": 0}}),
        )

    # Test when returning the full schema
    with pytest.raises(ValueError, match="Fields name collision detected"):
        Message._get_schema(DataModel.__msco_pyarrow_struct__)
