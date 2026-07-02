from mosaicolabs.comm.mosaico_client import MosaicoClient
from mosaicolabs.models.core.unmodeled import Unmodeled

with MosaicoClient.connect("localhost", 6276) as client:
    print("--- Test topic handler stream for 'UnmodeledAcceleration'")
    th = client.topic_handler("unmodeled_seq", "/sensors/acc/no_schema")
    if th is None:
        print("UNABLE TO GET TOPIC HANDLER")
        exit(-1)

    for msg in th.get_data_streamer():
        print(f"from topic handler: {msg}")

    print("--- Test topic handler stream for 'UnmodeledGyro'")
    th = client.topic_handler("unmodeled_seq", "/sensors/gyro/no_schema")
    if th is None:
        print("UNABLE TO GET TOPIC HANDLER")
        exit(-1)

    for msg in th.get_data_streamer():
        print(f"from topic handler: {msg}")
        data = msg.get_data(Unmodeled)
        assert data is not None
        print(f"gyro.x: {data.raw_data['gyro']['x']}")

    print("--- Test sequence handler stream")
    sh = client.sequence_handler("unmodeled_seq")
    assert sh is not None
    for topic, msg in sh.get_data_streamer():
        data = msg.get_data(Unmodeled)
        assert data is not None
        print(f" from sequence handler {topic}: {msg}")
        if topic == "/sensors/gyro/no_schema":
            print(f"gyro.x = {data.raw_data['gyro']['x']}")
        if topic == "/sensors/acc/no_schema":
            print(f"acceleration.x = {data.raw_data['acceleration']['x']}")
