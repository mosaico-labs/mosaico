# from mosaicolabs import Time
from mosaicolabs.comm import MosaicoClient
from mosaicolabs.models.data import RobotPath
from mosaicolabs.models.sensors import RobotJoint, Temperature
from mosaicolabs.query import QueryOntologyCatalog, QuerySequence, QueryTopic
from testing.integration.config import (
    UPLOADED_ROBOT_JOINTS_TOPIC,
    UPLOADED_ROBOT_PATH_TOPIC,
    UPLOADED_TEMPERATURE_TOPIC,
)

from .helpers import _validate_returned_topic_name


def test_query_basic_list(
    mosaico_client: MosaicoClient,
    inject_synthetic_sequence_w_lists,  # Ensure the data (with lists) are available on the data platform
):
    # Query using all()
    query_resp = mosaico_client.query(
        QueryOntologyCatalog().with_expression(
            RobotJoint.Q.positions.all().geq(0.01)
        )  # set a very small value (data are random, so a small value is likely to be found)
    )
    # We do expect a successful query
    assert query_resp is not None and not query_resp.is_empty()
    # One (1) sequence corresponds to this query
    assert len(query_resp) == 1
    # The target topic is 'UPLOADED_ROBOT_JOINTS_TOPIC'
    expected_topic_names = [
        UPLOADED_ROBOT_JOINTS_TOPIC,
    ]
    assert len(query_resp[0].topics) == len(expected_topic_names)

    # all the expected topics, and only them
    [_validate_returned_topic_name(topic.name) for topic in query_resp[0].topics]
    assert all([t.name in expected_topic_names for t in query_resp[0].topics])

    # Query using any()
    query_resp = mosaico_client.query(
        QueryOntologyCatalog().with_expression(
            RobotJoint.Q.positions.any().geq(0.01)
        )  # set a very small value (data are random, so a small value is likely to be found)
    )
    # We do expect a successful query
    assert query_resp is not None and not query_resp.is_empty()
    # One (1) sequence corresponds to this query
    assert len(query_resp) == 1
    # The target topic is 'UPLOADED_ROBOT_JOINTS_TOPIC'
    expected_topic_names = [
        UPLOADED_ROBOT_JOINTS_TOPIC,
    ]
    assert len(query_resp[0].topics) == len(expected_topic_names)

    # all the expected topics, and only them
    [_validate_returned_topic_name(topic.name) for topic in query_resp[0].topics]
    assert all([t.name in expected_topic_names for t in query_resp[0].topics])

    # Query using index access
    query_resp = mosaico_client.query(
        QueryOntologyCatalog().with_expression(
            RobotJoint.Q.positions[1].geq(0.01)
        )  # set a very small value (data are random, so a small value is likely to be found)
    )
    # We do expect a successful query
    assert query_resp is not None and not query_resp.is_empty()
    # One (1) sequence corresponds to this query
    assert len(query_resp) == 1
    # The target topic is 'UPLOADED_ROBOT_JOINTS_TOPIC'
    expected_topic_names = [
        UPLOADED_ROBOT_JOINTS_TOPIC,
    ]
    assert len(query_resp[0].topics) == len(expected_topic_names)

    # all the expected topics, and only them
    [_validate_returned_topic_name(topic.name) for topic in query_resp[0].topics]
    assert all([t.name in expected_topic_names for t in query_resp[0].topics])

    # Query using index access + string equality
    query_resp = mosaico_client.query(
        QueryOntologyCatalog()
        .with_expression(
            RobotJoint.Q.positions.any().geq(0.01)
        )  # set a very small value (data are random, so a small value is likely to be found)
        .with_expression(
            RobotJoint.Q.names[0].eq("joint1")
        )  # set a very small value (data are random, so a small value is likely to be found)
    )
    # We do expect a successful query
    assert query_resp is not None and not query_resp.is_empty()
    # One (1) sequence corresponds to this query
    assert len(query_resp) == 1
    # The target topic is 'UPLOADED_ROBOT_JOINTS_TOPIC'
    expected_topic_names = [
        UPLOADED_ROBOT_JOINTS_TOPIC,
    ]
    assert len(query_resp[0].topics) == len(expected_topic_names)

    # all the expected topics, and only them
    [_validate_returned_topic_name(topic.name) for topic in query_resp[0].topics]
    assert all([t.name in expected_topic_names for t in query_resp[0].topics])

    # Query using index access + string match
    query_resp = mosaico_client.query(
        QueryOntologyCatalog()
        .with_expression(
            RobotJoint.Q.positions.any().geq(0.01)
        )  # set a very small value (data are random, so a small value is likely to be found)
        .with_expression(
            RobotJoint.Q.names[0].match("j*")
        )  # set a very small value (data are random, so a small value is likely to be found)
    )
    # We do expect a successful query
    assert query_resp is not None and not query_resp.is_empty()
    # One (1) sequence corresponds to this query
    assert len(query_resp) == 1
    # The target topic is 'UPLOADED_ROBOT_JOINTS_TOPIC'
    expected_topic_names = [
        UPLOADED_ROBOT_JOINTS_TOPIC,
    ]
    assert len(query_resp[0].topics) == len(expected_topic_names)

    # all the expected topics, and only them
    [_validate_returned_topic_name(topic.name) for topic in query_resp[0].topics]
    assert all([t.name in expected_topic_names for t in query_resp[0].topics])

    # free resources
    mosaico_client.close()


def test_mixed_query_ontology_w_basic_list(
    mosaico_client: MosaicoClient,
    inject_synthetic_sequence_w_lists,  # Ensure the data (with lists) are available on the data platform
):

    # Query list + another (non-list) data
    query_resp = mosaico_client.query(
        QueryOntologyCatalog()
        .with_expression(
            RobotJoint.Q.positions.any().geq(0.01)
        )  # set a very small value (data are random, so a small value is likely to be found)
        .with_expression(
            Temperature.Q.value.eq(330)
        ),  # set a very small value (data are random, so a small value is likely to be found)
    )
    # We do expect a successful query
    assert query_resp is not None and not query_resp.is_empty()
    # One (1) sequence corresponds to this query
    assert len(query_resp) == 1
    # The target topic is 'UPLOADED_TEMPERATURE_TOPIC' and 'UPLOADED_ROBOT_JOINTS_TOPIC'
    expected_topic_names = [
        UPLOADED_TEMPERATURE_TOPIC,
        UPLOADED_ROBOT_JOINTS_TOPIC,
    ]
    assert len(query_resp[0].topics) == len(expected_topic_names)

    # all the expected topics, and only them
    [_validate_returned_topic_name(topic.name) for topic in query_resp[0].topics]
    assert all([t.name in expected_topic_names for t in query_resp[0].topics])

    # Query list + topic name
    query_resp = mosaico_client.query(
        QueryOntologyCatalog().with_expression(
            RobotJoint.Q.positions.all().geq(0.01)
        ),  # set a very small value (data are random, so a small value is likely to be found)
        QueryTopic().with_name("/robot/joint_states"),
    )
    # We do expect a successful query
    assert query_resp is not None and not query_resp.is_empty()
    # One (1) sequence corresponds to this query
    assert len(query_resp) == 1
    # The target topic is and 'UPLOADED_ROBOT_JOINTS_TOPIC'
    expected_topic_names = [
        UPLOADED_ROBOT_JOINTS_TOPIC,
    ]
    assert len(query_resp[0].topics) == len(expected_topic_names)

    # all the expected topics, and only them
    [_validate_returned_topic_name(topic.name) for topic in query_resp[0].topics]
    assert all([t.name in expected_topic_names for t in query_resp[0].topics])

    # Query list + topic metadata
    query_resp = mosaico_client.query(
        QueryOntologyCatalog().with_expression(
            RobotJoint.Q.positions.all().geq(0.01)
        ),  # set a very small value (data are random, so a small value is likely to be found)
        QueryTopic().with_user_metadata("model", eq="GoFa"),
    )
    # We do expect a successful query
    assert query_resp is not None and not query_resp.is_empty()
    # One (1) sequence corresponds to this query
    assert len(query_resp) == 1
    # The target topic is and 'UPLOADED_ROBOT_JOINTS_TOPIC'
    expected_topic_names = [
        UPLOADED_ROBOT_JOINTS_TOPIC,
    ]
    assert len(query_resp[0].topics) == len(expected_topic_names)

    # all the expected topics, and only them
    [_validate_returned_topic_name(topic.name) for topic in query_resp[0].topics]
    assert all([t.name in expected_topic_names for t in query_resp[0].topics])

    # Query list + sequence name
    query_resp = mosaico_client.query(
        QueryOntologyCatalog().with_expression(
            RobotJoint.Q.positions.all().geq(0.01)
        ),  # set a very small value (data are random, so a small value is likely to be found)
        QuerySequence().with_name("list-query-sequence"),
    )
    # We do expect a successful query
    assert query_resp is not None and not query_resp.is_empty()
    # One (1) sequence corresponds to this query
    assert len(query_resp) == 1
    # The target topic is and 'UPLOADED_ROBOT_JOINTS_TOPIC'
    expected_topic_names = [
        UPLOADED_ROBOT_JOINTS_TOPIC,
    ]
    assert len(query_resp[0].topics) == len(expected_topic_names)

    # all the expected topics, and only them
    [_validate_returned_topic_name(topic.name) for topic in query_resp[0].topics]
    assert all([t.name in expected_topic_names for t in query_resp[0].topics])

    mosaico_client.close()


def test_query_list_of_struct(
    mosaico_client: MosaicoClient,
    inject_synthetic_sequence_w_lists,  # Ensure the data (with lists) are available on the data platform
):
    # Query using all()
    query_resp = mosaico_client.query(
        QueryOntologyCatalog().with_expression(
            RobotPath.Q.poses.all().position.z.geq(-1.01)
        )  # set a -1.01 value (data are random from [-1, 1])
    )
    # We do expect a successful query
    assert query_resp is not None and not query_resp.is_empty()
    # One (1) sequence corresponds to this query
    assert len(query_resp) == 1
    # The target topic is 'UPLOADED_ROBOT_PATH_TOPIC'
    expected_topic_names = [
        UPLOADED_ROBOT_PATH_TOPIC,
    ]
    assert len(query_resp[0].topics) == len(expected_topic_names)

    # all the expected topics, and only them
    [_validate_returned_topic_name(topic.name) for topic in query_resp[0].topics]
    assert all([t.name in expected_topic_names for t in query_resp[0].topics])

    # Query using any()
    query_resp = mosaico_client.query(
        QueryOntologyCatalog().with_expression(
            RobotPath.Q.poses.any().position.z.geq(0.01)
        )  # set a very small value (at least one of the z is between [0, 1])
    )
    # We do expect a successful query
    assert query_resp is not None and not query_resp.is_empty()
    # One (1) sequence corresponds to this query
    assert len(query_resp) == 1
    # The target topic is 'UPLOADED_ROBOT_PATH_TOPIC'
    expected_topic_names = [
        UPLOADED_ROBOT_PATH_TOPIC,
    ]
    assert len(query_resp[0].topics) == len(expected_topic_names)

    # all the expected topics, and only them
    [_validate_returned_topic_name(topic.name) for topic in query_resp[0].topics]
    assert all([t.name in expected_topic_names for t in query_resp[0].topics])

    # Query using index access
    query_resp = mosaico_client.query(
        QueryOntologyCatalog().with_expression(
            RobotPath.Q.poses[2].position.z.geq(-1.01)
        )  # the third pose in Z varies between [-1, 1]
    )
    # We do expect a successful query
    assert query_resp is not None and not query_resp.is_empty()
    # One (1) sequence corresponds to this query
    assert len(query_resp) == 1
    # The target topic is 'UPLOADED_ROBOT_PATH_TOPIC'
    expected_topic_names = [
        UPLOADED_ROBOT_PATH_TOPIC,
    ]
    assert len(query_resp[0].topics) == len(expected_topic_names)

    # all the expected topics, and only them
    [_validate_returned_topic_name(topic.name) for topic in query_resp[0].topics]
    assert all([t.name in expected_topic_names for t in query_resp[0].topics])

    # free resources
    mosaico_client.close()


def test_mixed_query_ontology_w_struct_list(
    mosaico_client: MosaicoClient,
    inject_synthetic_sequence_w_lists,  # Ensure the data (with lists) are available on the data platform
):
    # Query list + another (non-list) data
    query_resp = mosaico_client.query(
        QueryOntologyCatalog()
        .with_expression(
            RobotPath.Q.poses.any().position.z.geq(0.0)
        )  # set a very small value (data are random, so a small value is likely to be found)
        .with_expression(
            Temperature.Q.value.eq(330)
        ),  # set a very small value (data are random, so a small value is likely to be found)
    )
    # We do expect a successful query
    assert query_resp is not None and not query_resp.is_empty()
    # One (1) sequence corresponds to this query
    assert len(query_resp) == 1
    # The target topic is 'UPLOADED_TEMPERATURE_TOPIC' and 'UPLOADED_ROBOT_PATH_TOPIC'
    expected_topic_names = [
        UPLOADED_TEMPERATURE_TOPIC,
        UPLOADED_ROBOT_PATH_TOPIC,
    ]
    assert len(query_resp[0].topics) == len(expected_topic_names)

    # all the expected topics, and only them
    [_validate_returned_topic_name(topic.name) for topic in query_resp[0].topics]
    assert all([t.name in expected_topic_names for t in query_resp[0].topics])

    # Query list + topic name
    query_resp = mosaico_client.query(
        QueryOntologyCatalog().with_expression(
            RobotPath.Q.poses.all().position.z.geq(-1.01)
        ),  # set a -1.01 value (data are random from [-1, 1])
        QueryTopic().with_name("/robot/path"),
    )
    # We do expect a successful query
    assert query_resp is not None and not query_resp.is_empty()
    # One (1) sequence corresponds to this query
    assert len(query_resp) == 1
    # The target topic is and 'UPLOADED_ROBOT_PATH_TOPIC'
    expected_topic_names = [
        UPLOADED_ROBOT_PATH_TOPIC,
    ]
    assert len(query_resp[0].topics) == len(expected_topic_names)

    # all the expected topics, and only them
    [_validate_returned_topic_name(topic.name) for topic in query_resp[0].topics]
    assert all([t.name in expected_topic_names for t in query_resp[0].topics])

    # Query list + topic metadata
    query_resp = mosaico_client.query(
        QueryOntologyCatalog().with_expression(
            RobotPath.Q.poses.all().position.z.between([-1.01, 1.01])
        ),  # set a consistent interval (data are random within [-1.01, 1.01])
        QueryTopic().with_user_metadata("validation_state", eq="verified"),
    )

    # We do expect a successful query
    assert query_resp is not None and not query_resp.is_empty()
    # One (1) sequence corresponds to this query
    assert len(query_resp) == 1
    # The target topic is and 'UPLOADED_ROBOT_PATH_TOPIC'
    expected_topic_names = [
        UPLOADED_ROBOT_PATH_TOPIC,
    ]
    assert len(query_resp[0].topics) == len(expected_topic_names)

    # all the expected topics, and only them
    [_validate_returned_topic_name(topic.name) for topic in query_resp[0].topics]
    assert all([t.name in expected_topic_names for t in query_resp[0].topics])

    # Query list + sequence name
    query_resp = mosaico_client.query(
        QueryOntologyCatalog().with_expression(
            RobotPath.Q.poses[0].position.z.leq(1.01)
        ),  # set a big value (data are random between [0, 1])
        QuerySequence().with_name("list-query-sequence"),
    )
    # We do expect a successful query
    assert query_resp is not None and not query_resp.is_empty()
    # One (1) sequence corresponds to this query
    assert len(query_resp) == 1
    # The target topic is and 'UPLOADED_ROBOT_PATH_TOPIC'
    expected_topic_names = [
        UPLOADED_ROBOT_PATH_TOPIC,
    ]
    assert len(query_resp[0].topics) == len(expected_topic_names)

    # all the expected topics, and only them
    [_validate_returned_topic_name(topic.name) for topic in query_resp[0].topics]
    assert all([t.name in expected_topic_names for t in query_resp[0].topics])

    mosaico_client.close()


def test_mixed_query_no_return(
    mosaico_client: MosaicoClient,
    inject_synthetic_sequence_w_lists,  # Ensure the data (with lists) are available on the data platform
):
    # Query by multiple condition: value and topic metadata
    query_resp = mosaico_client.query(
        QueryOntologyCatalog().with_expression(RobotJoint.Q.positions.all().gt(1.1)),
    )
    # We do expect a successful query
    assert query_resp is not None
    # One (1) sequence corresponds to this query
    assert len(query_resp) == 0

    # free resources
    mosaico_client.close()
